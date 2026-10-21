# Design

## Overview and components

Pods decide per request whether to call each buyer, from a local snapshot of rates. The
evaluator turns each buyer's stats into a rate once per window. Redis connects them. The
rules are a pure function, `Evaluate(stats, previous, params)`, with no I/O.

## How stats and state flow

Pods count calls in local atomic counters and every second write them with `HINCRBY` into
`stats:{buyer}:{window}`, so pods add to one window without coordinating. After each window
closes plus a 5-second grace period, the evaluator evaluates the latest closed window for
each buyer and writes `state:{buyer}` with a compare-and-set. Pods reload throttled buyers
every two seconds.

## Keeping the per-request check fast

`ShouldCall` reads an immutable snapshot through an atomic pointer. At rate 0 it returns
true without a random draw; otherwise it skips when a draw in `[0, 10000)` falls below the
rate in basis points, and it checks snapshot age against the clock. `Record` adds to an
atomic counter found in a `sync.Map`. Neither call takes a lock or touches the network.
Prometheus reads counters at scrape time, so no labelled metric is updated per call.

Benchmarks (i5-1038NG7, `go test -bench`, mean of three to six runs, 0 allocations):

| Call | 1 core | 8 cores (parallel) |
|---|---|---|
| `ShouldCall` | ~116 ns/op | ~26 ns/op |
| `Record` | ~40 ns/op | ~11 ns/op |

We did not run a 100k requests/s load test. Our evidence is the per-call benchmarks above
and the absence of network calls and locks on the request path.

The staleness check stays in `ShouldCall`. In the refresher, a hung or dead goroutine would
never flip the flag, and a pod could enforce an old block forever. The check at call time
avoids that; its cost is small against the 150 ms budget, and we did not measure the clock
read alone.

## How multiple pods stay consistent

Pods agree on the window because its index is derived from the clock alone. Two delays add
up, reported separately.

- **Detection latency:** from a buyer starting to fail to the block. The worst case is almost
  two windows plus grace, about 25 seconds with a 10-second window and 5-second grace. A failure
  near a window's end leaves that window mostly clean, so the next full window decides. The best
  case is about 15 seconds. The demo observed about 19 seconds (break at 09:02:46, block logged
  at 09:03:05).
- **Propagation:** from the evaluator's write to every pod holding the state, within one
  refresh interval (2 seconds). This is the convergence time, which is bounded.

The guarantee we would not give up is that the evaluator never applies the same window twice.
The compare-and-set writes only if the stored last-evaluated window is older, so two evaluators
racing on one window cannot both apply a step. The demo runs one evaluator; concurrent
evaluators are covered by the store and runner tests.

## Decisions on the ambiguous cases

| Case | Decision | Why |
|---|---|---|
| Window below the minimum sample | Carry its stats forward and hold the rate, at any rate. | We read "fewer than the minimum sample must not trigger a block" as applying to a single window. A sample accumulated across windows that reaches the minimum can block. Otherwise a low-volume buyer that always fails would never be protected. |
| Carry cap (5 windows) reached | Drop the carry without judging it. A throttled buyer steps down one recovery step; a buyer at rate 0 stays at 0. | Holding the rate at the cap would leave a low-volume buyer blocked forever. |
| Failed and timeout are separate outcomes | Failure rate is failed / total and excludes timeouts. Timeout rate is timeout / total. | Each outcome has its own threshold, and the two never overlap. |
| Both thresholds crossed | The reason is `failure_rate`. Both flags are logged. | A fixed precedence keeps the reason deterministic. |
| Flush fails | Drop the batch; do not re-add it to the next flush. | Re-adding would move calls into a later window than the one they were made in. |
| Redis unreachable from a pod | Keep the snapshot for up to 5 minutes, then fail open. ShouldCall returns true. | An outage in the store must not stop auctions. Redis is never on the request path. |
| Unknown buyer | Call it. | A buyer with no state is not throttled. |
| More buyers than `MaxBuyers` | Calls still happen, but per-buyer counters and metrics are dropped and counted. | Bounds memory and metric cardinality against unknown IDs. |

## Trade-offs and known limitations

- **Low-volume buyers oscillate.** A throttled buyer with too little traffic hits the carry
  cap before it can be judged, then steps down on a timer. If still failing, it snaps back to
  block once its carried sample reaches the minimum. Its damage is proportional to its
  participation, and holding at the cap would bring back the stuck-forever problem. Before
  changing the rule, we would measure how many real buyers fall below the minimum while blocked
  and what they cost, then consider a lower minimum or a longer cap for them.
- **Flush skew.** A batch takes the window current at flush time, so calls just before a boundary
  can land one window late, by at most one second.
- **Data loss on crash.** Unflushed counts, up to about one second per pod, are lost.
- **Per-buyer store calls.** Three store round trips per buyer per window: fine for tens of buyers,
  and the reads should be pipelined at thousands.
- **Window alignment and no backfill.** Window indexes come from each pod's clock, so pods must
  agree within the grace period (NTP), or calls move into a neighbouring window. A paused evaluator
  evaluates only the latest closed window, since replaying old windows would step rates in bursts.
