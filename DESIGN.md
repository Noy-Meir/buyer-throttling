# Design

## Overview and components

Pods decide per request whether to call each buyer, using a local snapshot of throttle
rates. The evaluator turns each buyer's recent stats into a rate once per window. Redis
connects them. The rules are a pure function, `Evaluate(stats, previous, params)`, with
no clock, I/O or Redis types.

## How stats and state flow

Pod to Redis: calls are counted in local atomic counters. Every second they are swapped
to zero and written with `HINCRBY` into `stats:{buyer}:{window}`. Increments let several
pods write one window without coordinating.

Redis to evaluator: after each window closes plus a 5-second grace period, the evaluator
reads and evaluates the latest closed window for every buyer.

Evaluator to Redis to pods: the evaluator writes `state:{buyer}` with a compare-and-set.
Each pod reloads throttled buyers every two seconds into its snapshot.

## Keeping the per-request check fast

`ShouldCall` loads an immutable snapshot through an atomic pointer and looks the buyer
up in a map. At rate 0 it returns true without drawing a random number. Otherwise it
draws an integer in `[0, 10000)` and skips when the draw is below the rate in basis
points. It also checks snapshot age against the clock.

`Record` loads the buyer's counters from a `sync.Map` and adds to an atomic. Neither
call takes a lock or touches the network. Prometheus reads the counters at scrape time,
so no labelled metric is updated on the request path.

Benchmarks (i5-1038NG7, `go test -bench`, mean of three runs, 0 allocations):

| Call | 1 core | 8 cores (parallel) |
|---|---|---|
| `ShouldCall` | ~117 ns/op | ~21 ns/op |
| `Record` | ~37 ns/op | ~9 ns/op |

The staleness check stays inside `ShouldCall`. If it moved into the refresher goroutine,
a hung or dead refresher would leave the flag unchanged, and a pod could enforce an old
block forever. Checking at call time avoids that. The alternative is cheaper, but the
figures above are for the whole call, and we did not measure the clock read on its own.
Fail-open could also be up to one refresh interval late, and the flag adds a failure
mode. We kept the simpler, safer version: the cost is small compared with the
150 ms auction budget.

## How multiple pods stay consistent

Pods agree on the window because it is derived from the clock alone. The index is the
number of whole windows since the Unix epoch, so every pod and the evaluator compute
the same index without talking to each other.

Two delays add up, and we report them separately.

- **Detection latency** is the time from a buyer starting to fail to the evaluator
  blocking it. The worst case is one window plus the grace period, which is about 15
  seconds in the demo (10-second window, 5-second grace).
- **Propagation** is the time from the evaluator's write to every pod holding the new
  state. Each pod picks it up within one refresh interval, 2 seconds by default. This
  propagation delay is the convergence time: once it has passed, all pods enforce the
  same rate.

The assignment requires that pods converge on the same view. Convergence here is
bounded: at most one refresh interval after the evaluator writes.

The guarantee we would not give up is that the evaluator never applies the same window
twice. The compare-and-set (a Lua script) writes only if the stored last-evaluated
window is older than the one being written. Timing varies, but each step is still
correct, so two evaluators racing on one window cannot both apply a step.

## Decisions on the ambiguous cases

| Case | Decision | Why |
|---|---|---|
| Window below the minimum sample | Carry its stats forward and hold the rate, at any rate. | A small sample is not evidence. Carrying lets a low-volume buyer reach a judgeable sample. |
| Carry cap (5 windows) reached | Drop the carry without judging it. A throttled buyer steps down one recovery step; a buyer at rate 0 stays at 0. | Holding the rate at the cap would leave a low-volume buyer blocked forever. |
| Failed and timeout are separate outcomes | Failure rate is failed / total and excludes timeouts. Timeout rate is timeout / total. | Each outcome has its own threshold, and the two never overlap. |
| Both thresholds crossed | The reason is `failure_rate`. Both flags are logged. | A fixed precedence keeps the reason deterministic. |
| Flush fails | Drop the batch; do not re-add it to the next flush. | Re-adding would move calls into a later window than the one they were made in. |
| Redis unreachable from a pod | Keep the snapshot for up to 5 minutes, then fail open. ShouldCall returns true. | An outage in the store must not stop auctions. Redis is never on the request path. |
| Unknown buyer | Call it. | A buyer with no state is not throttled. |
| More buyers than `MaxBuyers` | Calls still happen, but per-buyer counters and metrics are dropped and counted. | Bounds memory and metric cardinality against unknown IDs. |

## Trade-offs and known limitations

- **Low-volume buyers oscillate.** A throttled buyer with too little traffic reaches the
  carry cap before its sample can be judged, and then steps down on a timer. If it is
  still failing, it snaps back to block once its carried sample reaches the minimum.
  The damage it does is proportional to its participation. Holding its rate at the cap
  would bring back the stuck-forever problem. Before changing the rule, we would measure
  how many real buyers fall below the minimum while blocked, and what latency or revenue
  they cost. Then we would consider a lower minimum or a longer cap for them.
- **Flush skew.** A batch is attributed to the window current at flush time, so calls
  made just before a boundary can land one window late. The skew is at most one flush
  interval (1 second).
- **Data loss on crash.** Counts not yet flushed, at most about one second per pod, are
  lost if the pod crashes.
- **Per-buyer store calls.** The evaluator makes three store round trips per buyer per
  window. This is fine for tens of buyers. At thousands, the reads should be pipelined.
- **No backfill.** A paused evaluator skips the windows it missed and evaluates only the
  latest closed one. Replaying old windows would step rates in bursts.
