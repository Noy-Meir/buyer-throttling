# Design

## Overview and components

The service decides how much traffic each external buyer receives, based on how
healthy that buyer has been recently. Three kinds of process take part:

- **Pod** (one per auction server). It runs the auction and a throttler library.
  `ShouldCall` decides whether to call a buyer, and `Record` counts the outcome.
  Pods never block on the network to make those decisions.
- **Evaluator** (one or more). Once per window it reads each buyer's stats from
  Redis, applies the throttling rules, and writes the new state back.
- **Redis** holds the stats (`stats:{buyer}:{window}`), the state (`state:{buyer}`)
  and the set of known buyers.

The rules themselves live in a pure function, `Evaluate(stats, previous, params)`,
which has no clock, I/O or Redis types. Everything else is plumbing around it.

## How stats and state flow

A pod counts each call into local atomic counters. Once a second, a flusher swaps
those counters to zero and writes them to Redis as one batch. Each count is added
with `HINCRBY` to the hash for the buyer and window, so several pods can write the
same window without coordinating.

At each window boundary plus a 5-second grace period, the evaluator takes the most
recently closed window, reads the stats, and evaluates. It then writes the new state
with a compare-and-set. Each pod reloads all throttled states from Redis every two
seconds into a local snapshot, and `ShouldCall` reads only that snapshot.

## Keeping the per-request check fast

`ShouldCall` loads an immutable snapshot through an atomic pointer, looks up the
buyer in a map, and returns true at rate 0 without drawing a random number. Otherwise
it draws an integer in `[0, 10000)` and skips when the draw is below the rate in basis
points. Staleness is checked against the clock on each call.

`Record` loads the buyer's counters from a `sync.Map` and adds to an atomic. Neither
call touches the network or takes a lock. Prometheus reads the counters at scrape
time, so no labelled metric is updated on the request path.

Benchmarks (i5-1038NG7, `go test -bench`, mean of three runs, 0 allocations):

| Call | 1 core | 8 cores (parallel) |
|---|---|---|
| `ShouldCall` | ~117 ns/op | ~21 ns/op |
| `Record` | ~37 ns/op | ~9 ns/op |

TODO(noy): rewrite in your own words why the staleness check stays on the request path,
and what moving it to the refresher would cost.

## How multiple pods stay consistent

Pods agree on the window because it is derived from the clock alone: the index is the
number of whole windows since the Unix epoch. Every pod and the evaluator compute the same
index without talking to each other.

Stats are increments, so concurrent flushes add up. The evaluator's compare-and-set
(a Lua script) writes a state only if the stored last-evaluated window is older than
the one being written. Re-evaluating the same window is therefore a no-op, and two
evaluators racing on one window cannot both apply a step. A lock is not needed for
correctness.

The result is eventual consistency. A rate change reaches every pod within the
evaluation delay plus the two-second refresh, which is roughly one window in the
worst case. In the demo, with a 10-second window, that is about 15 to 17 seconds.

TODO(noy): rewrite the consistency argument in your own words, and say which
guarantee you would not give up.

## Decisions on the ambiguous cases

| Case | Decision | Why |
|---|---|---|
| Window below the minimum sample | Carry its stats into the next window and hold the rate, at any rate. | A small sample is not evidence. Carrying lets a low-volume buyer reach a judgeable sample. |
| Carry cap (5 windows) reached | Drop the carry without judging it. A throttled buyer steps down one recovery step; a buyer at rate 0 stays at 0. | Holding the rate at the cap would leave a low-volume buyer blocked forever. |
| Flush fails | Drop the batch; do not re-add it to the next flush. | Re-adding would move calls into a later window than the one they were made in. |
| Redis unreachable on the request path | Fail open. The snapshot is kept for up to 5 minutes, then ShouldCall returns true. | An outage in the store must not stop auctions. |
| Unknown buyer | Call it. | A buyer with no state is not throttled. |
| More buyers than `MaxBuyers` | Calls still happen, but per-buyer counters and metrics are dropped and counted. | Bounds memory and metric cardinality against unknown IDs. |

## Trade-offs and known limitations

- **Low-volume buyers oscillate.** A throttled buyer with too little traffic reaches the
  carry cap before it can be judged, and then steps down on a timer, even if it still
  fails. It snaps back to block once its carried sample reaches the minimum. The
  impact is proportional to the buyer's participation, so we accepted this rather
  than let it stay blocked forever.
  TODO(noy): rewrite the reasoning here, including what you would measure before
  changing it.
- **Flush skew.** A batch is attributed to the window current at flush time, so calls
  made just before a boundary can land one window late. The skew is at most one flush
  interval (1 second).
- **Data loss on crash.** Counts not yet flushed (at most about one second per pod) are
  lost if the pod crashes.
- **Per-buyer store calls.** The evaluator makes three store round trips per buyer per
  window. This is fine for tens of buyers. At thousands, the reads should be pipelined.
- **No backfill.** A paused evaluator skips the windows it missed and evaluates only the
  latest closed one. This is deliberate, because replaying old windows would step rates
  in bursts.
