# Buyer Throttling Service (Rise home assignment)

Go service that monitors external buyer health in a real-time auction platform and controls per-buyer traffic via a throttle rate. Quality bar: depth and clear reasoning over breadth. Every non-obvious decision must be documented in DESIGN.md.

## Source of truth
The original assignment is in docs/assignment.pdf. It defines the requirements and deliverables. This file records our design decisions for the areas the assignment leaves ambiguous (e.g. the small-window carry-over, staleness, flush failures). Where they conflict on requirements, the assignment wins; where the assignment is silent, the decisions here win. Flag any conflict to me instead of resolving it silently.

Resolved conflict (decided by the author, 2026-10-21, do not reopen): the assignment says a window with fewer requests than the minimum sample "must not trigger a block". We read this rule as applying to a single window. A sample accumulated across carried windows that reaches the minimum can block. Reason: otherwise a low-volume buyer that always fails would never be protected. Recorded in DESIGN.md and docs/decisions.md.

## Stack
Go 1.22+, Redis 7 + go-redis/v9, Docker multi-stage + docker compose (with healthchecks), net/http (no web framework), log/slog, prometheus/client_golang, testing + testify + miniredis. No Kubernetes, Kafka, gRPC, ORMs or DI frameworks.

## Architecture
- Redis: shared store for per-window counters and throttle state.
- Pod (auction side): exposes ShouldCall(buyerID) bool and Record(buyerID, result). Each pod is independent.
- Evaluator: separate process, runs once per minute aligned to the minute boundary.
- Simulator + fake buyers: traffic generator for the docker compose demo (configurable failure/timeout rates, changeable at runtime).

### Data flow
- Record increments LOCAL atomic counters (ok/failed/timeout). No network, no global lock.
- Background goroutine flushes every 1s with HINCRBY to key stats:{buyer}:{unixMinute}, with TTL of a few minutes.
- Evaluator evaluates the LAST CLOSED window only, after a 5s grace period. It never backfills missed windows.
- Evaluator writes state atomically via a Lua compare-and-set in the store: the write succeeds only if the stored last_evaluated_window < the window being written (idempotent: re-evaluating the same window is a no-op). The store keeps last_evaluated_window next to the state; the pure core does not. A SET NX lock only reduces duplicate work; correctness comes from idempotency.
- Stats per window are three counters (ok/failed/timeout) in one hash. Total is derived (ok+failed+timeout), never stored, so failed+timeout > total cannot happen.
- ShouldCall reads a LOCAL map (atomic). A background goroutine refreshes it from Redis every 2s. The critical path never touches the network.

## Evaluator rules
- Core is a PURE function: Evaluate(windowStats, prevState, params) -> Decision. Decision = {State, FailureRateViolated, TimeoutRateViolated}; the flags let the caller log both violations even though only failure_rate sets the reason. No Redis, no clock, no I/O.
- State holds rate (bp), reason, carried stats and carried window count. It has NO last_evaluated_window: idempotency is the store's job (see Data flow).
- ok / failed / timeout are mutually exclusive. failure_rate = failed/total, timeout_rate = timeout/total.
- Violation if failure_rate >= 0.7 OR timeout_rate >= 0.3 (use >=, "reaches"). Both violated: reason = failure_rate (fixed precedence), log both.
- Violation at any point (including mid-recovery) -> rate = block level (0.95) immediately.
- Clean window while rate > 0: rate -= step (0.1), floor at 0. From 0.95 it takes 10 clean windows. reason = "recovering" until rate hits 0, then reason = "".
- Clean window at rate 0: stays 0.
- total == 100 is a valid sample (fewer than 100 is invalid). total == 0 is insufficient (guard divide by zero).
- Rates are integers in basis points (block 9500, step 1000). Never compare floats. Config takes floats and converts at load.
- Small-window handling (carry-over): effective stats = window stats + carried stats.
    - If effective total < min sample (at any rate): carry stats forward, hold the rate, count carried windows. Carry at rate 0 too, so a low-volume buyer can accumulate to min sample and be judged (decided 2026-10-21, replacing "rate 0: no change, clear carry").
    - Cap: after 5 carried windows (configurable) with still < min: clear the carry without judging it. A throttled buyer steps down as a clean window; a buyer at rate 0 has nothing to step and stays at 0.
    - A violation is never judged on fewer than min sample calls.
    - If effective total >= min sample: clear carry, then apply violation / clean logic on the effective stats. Carry never leaks across an evaluated window.
    - Logs show the window total and the effective (carry-inclusive) total, so held windows are understandable.
- Reason values: "failure_rate", "timeout_rate", "recovering", "".

## Failure behavior (fail-open everywhere)
- Redis unavailable for reads: pod keeps last known state.
- State older than 5 minutes (configurable): fall back to fail-open (rate 0).
- Stats flush fails: DROP the batch (do not accumulate, it would distort a window), increment a metric.
- Unknown buyer: ShouldCall returns true. New buyer state: rate 0, reason "".
- Skipped (throttled) requests do NOT call Record. Stats describe calls actually made.
- Invalid Record result: ignore, count in a metric, never panic.
- Cap on number of tracked buyers (cardinality limit) to avoid unbounded memory.

## Config (all via env vars, validated at startup, fail fast with clear errors)
Defaults: failure threshold 0.7, timeout threshold 0.3, min sample 100, block level 0.95, step 0.1, window 1m, flush 1s, state refresh 2s, grace 5s, carry cap 5 windows, max staleness 5m.
Validation: 0 < block < 1, step > 0, recovery step <= block level, thresholds in (0,1], min sample > 0. Ratios with more than 4 decimal places are rejected, not rounded.

## Engineering rules
- Clock and Random are interfaces, injected. Tests never use time.Sleep.
- Store is an interface; the evaluator logic must not depend on Redis types.
- evaluator.Params is separate from config.Config; config.Config.EvaluatorParams() maps between them. The pure evaluator does not import config.
- The evaluator runner lives in internal/evaluator/runner, not internal/evaluator: the store refers to evaluator.State, so the runner (which uses the store) cannot sit in the same package without an import cycle.
- Critical path: map read + atomics only. No global mutex on ShouldCall/Record.
- Run go test -race ./... always. Table-driven tests for all evaluator rules and edge cases.
- Small, meaningful commits (conventional commits style). No giant final commit.
- Layout: cmd/{evaluator,pod,simulator}/, internal/{config,evaluator,throttler,store,metrics,clock}/, plus DESIGN.md, README.md, docker-compose.yml, Makefile.
- Makefile targets: test, lint, up, down.
- Bonuses, only after the core works: GET /state (format: {"buyers":[{"buyer_id","throttle_rate","reason"}]}), Prometheus metrics per buyer (throttle rate, error rates, request counts).

## Deliverables checklist
DESIGN.md (~1 page: components, data flow, fast critical path, multi-pod consistency, trade-offs, documented ambiguity decisions), code, evaluator tests, Redis store, docker compose with Redis + evaluator + at least 2 pods + simulator (watch a bad buyer get blocked then recover in logs), README (run, configure, observe).
## Demo expectations (docker compose, watch the evaluator logs)
- Scenario: at 30s buyer_bad and buyer_quiet both go to failure_rate 0.8; at 150s both heal to 0.
- buyer_bad: ~1000 calls per window at full participation, so it is judged every window. Expect a WARN with throttle_rate_bp=9500, reason=failure_rate, within about one window of the break.
- buyer_quiet (5% participation): ~50 calls per 10s window, below min sample 100. At rate 0 the window is carried at debug level, so no line appears for the first window. Expect the first WARN with throttle_rate_bp=9500, reason=failure_rate, effective_total around 100 (carry-inclusive), roughly two windows (about 20-30s) after the break.
- After that buyer_quiet's traffic drops to roughly 2-3 calls per window, so it reaches the carry cap and steps down by one recovery step per about six windows, even at 0.8 failure. This is the accepted low-volume oscillation (see docs/decisions.md). It will eventually snap back to 9500 when the carried sample reaches 100 again.
- Carries while throttled are INFO (carried_windows=N); carries at rate 0 are DEBUG.
