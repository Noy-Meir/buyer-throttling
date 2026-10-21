# Design decisions log

Raw log, in the order decisions were made or settled. Source material for
DESIGN.md. Each entry: decision, reasoning, rejected alternative. Where no
alternative was weighed at the time, the entry says so.

## Store and data flow

- **Redis as the shared store.**
  - Reasoning: pods must converge on one view of buyer health. Redis gives shared counters with atomic HINCRBY, and atomic Lua for compare-and-set. It also has TTLs for stats keys. CLAUDE.md rules out Kafka, Kubernetes and ORMs.
  - Rejected: a relational database (no atomic compare-and-set on a window index without extra locking; heavier for per-second writes). Not weighed in depth.

- **Critical path is local: no network in ShouldCall or Record.**
  - Reasoning: both run on every auction, and the assignment requires minimal latency. ShouldCall reads an immutable snapshot through an atomic pointer. Record increments per-buyer atomic counters.
  - Rejected: calling Redis per request. It adds a round trip to every buyer call and makes Redis availability a request-path dependency.

- **Snapshot refresh every 2s; counter flush every 1s.**
  - Reasoning: 2s bounds how stale a rate can be (a block takes effect within 2s on each pod). 1s flushes keep batches small and the evaluator's view close to real time.
  - Rejected: per-request writes, and longer intervals (slower visibility; bigger batches).

- **Flush uses HINCRBY, not overwrite.**
  - Reasoning: every pod writes the same stats key for a window. Overwriting would make the last pod's value replace the others'. Increments add up across pods with no coordination.
  - Rejected: SET of each pod's local total. Loses the other pods' counts.

- **Three separate counters (ok, failed, timeout); total derived as their sum.**
  - Reasoning: the three outcomes are mutually exclusive, so storing total separately would just create a field that can disagree. Deriving it makes failed+timeout > total impossible by construction.
  - Rejected: storing total as a fourth counter. A second write path could drift from the parts.

- **Window = unix minute, computed by one shared function (`store.WindowIndex`).**
  - Reasoning: pods and the evaluator need to agree which window a call belongs to, with no coordination. `UnixNano / window` gives the same index everywhere, for any positive window size.
  - Rejected: per-process window starts (drift between pods), and relative window numbers (need a shared epoch).

- **Grace period (5s), evaluate the last closed window only, never backfill.**
  - Reasoning: a flush from a slow pod may land a few seconds after the window closes. The grace period lets most of it arrive. Evaluating only the newest closed window means a paused process does not replay several decisions in one burst.
  - Rejected: backfilling every missed window. It would apply stale decisions and step a rate several times at once.

- **Idempotent compare-and-set in a Lua script.**
  - Reasoning: the write succeeds only if the stored last-evaluated window is less than the window being written. Evaluating a window twice is then a no-op, and concurrent evaluators cannot double-apply a step. Redis runs scripts atomically, so the check and the write cannot interleave.
  - Rejected: a SET NX lock as the correctness mechanism. It only reduces duplicate work. A lock holder that dies or stalls would leave the window in an unknown state.
  - Verified: miniredis runs the script (via gopher-lua), and a concurrency test shows exactly one winner.

- **Pure evaluator: `Evaluate(window, prev, params) -> Decision`.**
  - Reasoning: the rules are the part most worth testing exhaustively. A pure function needs no Redis, clock or I/O, so table tests cover them directly.
  - `Decision` holds the state and both violation flags. The flags let the log record both violations, even though only failure rate sets the reason. `Effective` holds the carry-inclusive stats for logging.
  - Rejected: returning a bare `State`. The caller could not log both violations without recomputing them.

- **`State` has no last-evaluated window.**
  - Reasoning: idempotency is a storage concern. Keeping the window in the store's `Snapshot` keeps the pure core free of it.
  - Rejected: storing it in `State`. Mixes persistence into the rules.

- **Evaluator params separate from config.**
  - Reasoning: the evaluator must not import configuration. `Config.EvaluatorParams()` maps between them.

- **Runner lives in `internal/evaluator/runner`, not `internal/evaluator`.**
  - Reasoning: the store refers to `evaluator.State`, and the runner uses the store. In one package that is an import cycle.
  - Rejected: putting the runner in `internal/evaluator` with a duplicated state type in the store. Duplicates the type and needs conversions.

## Rates and numbers

- **Basis points (integers) instead of floats.**
  - Reasoning: threshold checks become `count*10000 >= threshold*total` with no division and no float drift. Config still takes decimals.
  - Rejected: float rates. Boundary comparisons such as exactly 0.7 are unreliable with floats.

- **Config rejects ratios with more than 4 decimal places instead of rounding.**
  - Reasoning: a threshold of 0.12345 must not quietly become 0.1235.
  - Rejected: silent rounding.

- **Config validation.**
  - Recovery step must not exceed the block level. A larger step would drop a buyer from block to 0 in one window.
  - Stats TTL must exceed window plus grace, or a window could expire before it is read.
  - Empty env vars keep their defaults, so compose can pass blanks through.
  - All invalid variables are reported at once.
  - Grace may be 0. Recovery step has no upper bound beyond > 0 (the spec is silent).

## Carry-over (small windows)

- **Carry-over below the minimum sample.**
  - Reasoning: a window below 100 calls is not meaningful and must not trigger a block (assignment). Carrying stats forward until the sample is big enough lets the rule judge real samples.
  - Rejected: treating small windows as clean (lets a buyer recover on no evidence), and ignoring them (a low-volume buyer could never be judged).

- **Carry applies at rate 0 as well (changed 2026-10-21).**
  - Previously, rate 0 with an insufficient sample cleared the carry and never blocked. That meant a low-volume buyer below 100 calls per window could never be blocked, even at 80% failure. The demo confirmed this: `buyer_quiet` ran at 80% failure for three minutes with no block.
  - Now a small window carries at rate 0 until the effective sample reaches the minimum.
  - Rejected: keeping the old rule, because it contradicts "a failing buyer must be blocked".

- **Cap of 5 carried windows.**
  - Reasoning: bounds how long a sample can be held, and how stale that sample is.

- **Cap reached is treated as clean (kept, after discussion).**
  - Rule: when the cap is reached and the sample is still short, the carry is dropped without judging it, and a throttled buyer steps down by one recovery step. At rate 0 the rate stays 0.
  - Reasoning: a low-volume buyer's impact is proportional to its traffic. Holding the rate at the cap instead would leave a low-volume buyer blocked forever when it never produces enough calls to be judged.
  - Rejected: holding the rate at the cap. Brings back the stuck-forever problem.
  - Consequence (see the oscillation finding below): this is a deliberate trade-off, to be documented in DESIGN.md.

- **Violations are never judged on fewer than the minimum sample.**
  - Enforced by the rule and by a test that checks every rate with a failing short sample.

- **Carry is cleared once a window is evaluated.**
  - Reasoning: stats must not leak into a later decision.

- **Finding from the demo: low-volume oscillation.**
  - Observed: a blocked buyer with about 2.5 calls per window reaches the cap quickly and steps down by one recovery step per roughly six windows (about 60s), even at 80% failure. Its traffic rises as the rate falls. At about 0.55 the sample reaches 100 and it is judged, then snaps back to 0.95.
  - This is an estimate from the rules, not a measured run.
  - Reasoning for accepting it: see the cap decision above.

- **Carry logging level.**
  - Carries at rate 0 log at debug. Carries while throttled log at info, since that is the carry-over the demo shows.
  - Reasoning: low-volume buyers carry most windows at rate 0, which would flood info.

## Request path failure behaviour

- **Flush failure drops the batch.**
  - Reasoning: re-adding counts to the next flush would move calls into a later window than they were made in, distorting that window.
  - Rejected: re-accumulating the batch. Counts land in the wrong window.

- **Fail-open for unknown buyers, no snapshot, and stale snapshots.**
  - Unknown buyer: true, no random draw.
  - No snapshot yet: true.
  - Snapshot older than max staleness (5 min): true.
  - Reasoning: a broken Redis must not block auctions. A buyer that is not known as throttled is treated as healthy.
  - Rejected: fail-closed, which would stop all calls to every buyer on a Redis outage.

- **Staleness measured from the last successful refresh, using the injected clock.**
  - Rejected: staleness from the state's window. The window does not say when the pod last heard from Redis.

- **Staleness check kept inside ShouldCall (rejected: moving it to the refresher).**
  - Alternative: the refresher sets a stale flag, so ShouldCall reads one atomic flag and never calls the clock.
  - Reasoning for keeping it: the flag would only be updated every refresh interval (2s), so failing open would lag by up to 2s. The check is cheap and exact.
  - Cost measured: about 111 ns per ShouldCall on one core. Noted as a possible optimization for DESIGN.md.

- **Snapshot holds only buyers with a non-zero rate.**
  - Reasoning: rate 0 and unknown behave the same (true), so storing zeros wastes space on the request path.

- **Record beyond MaxBuyers is dropped and counted as Untracked.**
  - Reasoning: caps memory from unknown or attacker-supplied IDs. An untracked buyer's stats never reach the evaluator, so it stays fail-open. The cap is documented as a limit.
  - Rejected: evicting old buyers. The counter map is lock-free; evicting a `*counters` that a Record still holds would lose its counts.
  - Entries are never removed, so the tracked set is bounded for the process lifetime. A restart resets it.

- **Snapshot cap keeps the highest rates.**
  - Reasoning: the most-throttled buyers matter most. The lowest-rate buyers fail open first.

- **Flush window attribution skew (up to one flush interval, 1s).**
  - A batch is attributed to the window current at flush time, not at the moment of each call. Calls made just before a boundary can land in the next window.
  - Rejected: per-call attribution. That needs the window read on every Record, and a timestamp per call. The skew is bounded and small compared with a 10s or 60s window.

- **Stop does a final flush on a fresh context bounded by the flush interval.**
  - Reasoning: the loops' context is already cancelled. A bounded context means shutdown cannot hang on Redis.
  - Known gap: a batch in flight when the loop context is cancelled can be dropped, and that drop is counted.

- **Start loads the first snapshot synchronously.**
  - Reasoning: a restarted pod should not start open when blocks already exist.

- **Evaluator shutdown lets the current tick finish.**
  - Reasoning: cancelling mid-tick would leave a window half-applied. Store calls remain bounded by Redis client timeouts.

## Clock, concurrency and testing

- **Clock is an interface, with real and fake implementations.**
  - Reasoning: window alignment, grace, staleness and loop timing all depend on time. Tests must not sleep.
  - The `After` timer was added to the interface, because the runner and throttler loops wait on it.

- **Fake clock uses a mutex.**
  - Reasoning: tests advance time from one goroutine while loops read `Now` and register timers from others. Without the lock, `-race` flags it.
  - `BlockUntil` waits until a timer is registered, so a test can advance time without sleeping.

- **`*rand.Rand` is not safe for concurrent use; production uses `SystemRandom`.**
  - Found by: `-race` in the concurrent ShouldCall test. A seeded `*rand.Rand` shared across request goroutines is a data race.
  - Decision: `Random` must be safe for concurrent use. `SystemRandom` wraps the package-level generator, which is lock-free for callers.
  - Rejected: a mutex-wrapped Rand. Puts a global lock on ShouldCall, which the rules forbid.
  - Seeded generators are used only in single-goroutine statistical tests.

- **Random draws in integer basis points.**
  - `IntN(10000) < rate` skips with exactly rate/10000 probability.
  - Statistical test: 100,000 calls at rate 0.95 with a fixed seed, within ±500 of 5,000 (about 7 sigma).

- **Benchmark numbers** (i5-1038NG7, 2.0 GHz, darwin/amd64, 1000 buyers, a third throttled):
  - ShouldCall: 23.2 ns/op parallel on 8 cores; 111.5 ns/op single core; 0 allocs.
  - Record: 8.8 ns/op parallel; 37.2 ns/op single core; 0 allocs.
  - Parallel figures are aggregate throughput, not per-call latency.

- **Store tests run on miniredis.**
  - Reasoning: the Lua script, TTLs and concurrency can be tested against a real Redis protocol implementation without Docker.

## Participation, simulation and demo

- **Participation is owned by the buyers service.**
  - The simulator reads it from the admin API and sends each auction's invited buyers. Pods call only invited buyers.
  - Reasoning: participation is buyer behaviour, so one admin API controls it at runtime.
  - Rejected: pods choosing invitations. Participation would then have to be shared between pods.

- **Buyer timeout outcome sleeps 300ms, longer than the 150ms call timeout.**
  - Reasoning: a timeout must actually exceed the pod's deadline. The handler stops early if the client gives up.

- **Demo window is 10s; grace 5s; min sample 100; carry cap 5.**
  - Reasoning: blocking and recovery show up within minutes. Recovery from 0.95 takes 10 clean steps, about 100s after the first clean window.
  - Traffic: 100 auctions/s across two pods. Full-participation buyers get about 1000 calls per window. A 5%-participation buyer gets about 50, below the minimum.

- **Scenario timing:** `buyer_bad` fails 80% at 30s and heals at 150s. Observed in a local run: block within about one window, then ten steps from 0.95 to 0 once healed.

- **Logging.**
  - Text format on stdout. Level from `LOG_LEVEL`, default info.
  - Violations warn. Rate and reason changes are info. Carries are info while throttled and debug at rate 0. Steady state is debug.
  - Every decision logs the window totals and the effective (carry-inclusive) totals.

## Build and deployment

- **One Dockerfile, selected by a `CMD` build argument.**
  - Reasoning: four binaries share the same build steps.

- **Go base image pinned to `golang:1.27.3-alpine`, matching the `go 1.27.3` directive.**
  - Not verified: the tag was not confirmed against the registry (the check hung).

- **Runtime image is `alpine:3.20`, not scratch.**
  - Reasoning: the compose healthchecks use busybox `wget`, and alpine allows debugging. Non-root user.

- **Shared helpers in `internal/app`.**
  - Reasoning: logger, env readers, the Redis client and graceful HTTP shutdown are the same in each binary. One copy keeps them consistent.

- **Compose uses `service_healthy` for dependencies.**
  - Reasoning: pods and the simulator must not start before Redis and the buyers are answering.

- **`make up` runs compose in the foreground.**
  - Reasoning: the demo is watched in logs. `make logs` tails the services.

## Observability: /state and /metrics

- **/state is served in two places.**
  - The evaluator reads the store (AllStates). That is the source of truth.
  - Each pod reads its local snapshot plus the buyers it has seen. That is what it actually enforces.
  - Reasoning: the two answers can differ for up to one refresh interval (2s) plus the time to the next evaluation. Showing both makes that visible: a pod that disagrees with the store is either behind on refresh or has a stale snapshot.
  - Rejected: serving only the evaluator's view. Then nothing shows what a pod is enforcing right now.
  - Rejected: making the pod's /state call Redis. The handler would depend on the store, so it would fail during an outage. It would also be slower, which matters for an endpoint scraped while auctions run.
  - Verified: a test checks that the pod handler makes no store call, even with the store failing every request.

- **Pods expose the buyers they have seen, not all buyers.**
  - Reasoning: a pod's view covers what it enforces and what it has observed. Including every buyer in Redis would mean the pod has to read the store.
  - Rule: a buyer with no snapshot entry shows rate 0 and an empty reason.

- **/state format and sorting.**
  - Format is exactly `{"buyers":[{"buyer_id","throttle_rate","reason"}]}`. throttle_rate is basis points / 10000. An empty list is `[]`, not `null`. Buyers are sorted by ID.
  - A store error on the evaluator returns 503 with a generic message. The detail is logged, not returned.

- **Custom Prometheus collectors instead of labelled Inc on the request path.**
  - Reasoning: ShouldCall and Record are the hot path. A labelled Counter.Inc costs a map lookup per call, and allocates unless the label set is pre-resolved. The pods already keep atomic counters, so a collector reads them at scrape time and the request path does not change.
  - Rejected: labelled counters updated in Record or ShouldCall. Measurably slower and allocating on the path the assignment says must add as little latency as possible.
  - Consequence: the per-buyer call counts come from the auction service, which keeps cumulative totals. The log summary now reports deltas between its calls, so its lines are unchanged.

- **Metric label cardinality is bounded by MaxBuyers.**
  - The pod's throttler, the pod's auction service and the evaluator's observations each cap per-buyer series at MaxBuyers. Beyond the cap, the calls still happen. Only the per-buyer breakdown is dropped, and the drop is counted in an unlabelled counter.
  - Reasoning: an unknown buyer ID must not create a series. Each cap is a separate map with its own count.
  - Rejected: unbounded per-buyer labels. A misbehaving caller could exhaust memory in Prometheus and in the process.

- **Evaluator metrics describe the last applied decision.**
  - Gauges for rate and last window's effective failure and timeout rates. Counters for evaluations, violations and carried windows.
  - Violations count any rate reaching its threshold.
  - Reasoning: these are what an operator needs to see why a buyer is blocked. Gauges report the last window's state, and counters report how often each event happened.

- **Pod snapshot age is omitted before the first refresh.**
  - Reasoning: no snapshot means no age. Reporting zero would look like a fresh snapshot.

- **Server timeouts.**
  - Read header 5s, read 10s, write 10s, idle 60s, applied to the shared server in `internal/app`.
  - Reasoning: a stalled client should not hold a connection indefinitely.

- **Benchmark before and after (same machine, back to back, count 3).**
  - ShouldCall, one core: 117.4 ns/op before, 116.8 ns/op after.
  - Record, one core: 40.8 ns/op before, 36.6 ns/op after.
  - ShouldCall, eight cores: 29.1 ns/op before, 21.1 ns/op after.
  - Record, eight cores: 11.4 ns/op before, 8.6 ns/op after.
  - Allocations stayed at 0 B/op and 0 allocs/op throughout.
  - An earlier single run appeared to show about 16% slower Record. Back-to-back runs on the same laptop vary by about ±10%, so that was noise.
