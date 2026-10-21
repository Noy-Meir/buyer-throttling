# Buyer Throttling

[![ci](https://github.com/Noy-Meir/buyer-throttling/actions/workflows/ci.yml/badge.svg)](https://github.com/Noy-Meir/buyer-throttling/actions/workflows/ci.yml)

A throttling service for external buyers in a real-time auction. Pods decide per request
whether to call each buyer, based on that buyer's recent failure and timeout rates. The
decisions are evaluated once per window and shared through Redis, so every pod converges
on the same view.

## Run

Requires Docker with Compose.

```
docker compose up --build   # build and start the demo stack in the foreground (Ctrl-C to stop)
```

`docker-compose up` works on older installs. The Makefile wraps the same commands:

```
make up      # docker compose up --build
make down    # stop and remove the stack
make logs    # follow the evaluator, pod, simulator and buyer logs
```

The stack is Redis, the evaluator, two pods, three simulated buyers and a traffic
simulator. The simulator breaks two buyers at 30s and heals them at 150s, so nothing
needs to be typed during the demo.

## Why the extras

- **Two `/state` views.** The evaluator shows what it decided, and each pod shows what it
  enforces. Comparing them shows whether the pods have converged.
- **Metrics read at scrape time.** A collector reads the existing counters when Prometheus
  scrapes. The request path never updates a labelled metric, so the metrics cost nothing
  per auction.
- **Carry-over.** Without it, a low-volume buyer's sample never reaches the minimum, so it
  is either never protected or stuck blocked. Carrying small windows lets it be judged.
- **Idempotent compare-and-set.** A window is applied once, so several evaluators can run
  side by side without applying a step twice.

## Configure

The throttling rules are read from `THROTTLE_*` variables, validated at startup, and
any invalid value stops the process with a message naming the variable. An empty value
means the default.

| Variable | Default | What it does |
|---|---|---|
| `THROTTLE_FAILURE_THRESHOLD` | `0.7` | Block when failed / total reaches this. |
| `THROTTLE_TIMEOUT_THRESHOLD` | `0.3` | Block when timeout / total reaches this. |
| `THROTTLE_MIN_SAMPLE` | `100` | Smallest window (after carry-over) that can be judged. |
| `THROTTLE_BLOCK_LEVEL` | `0.95` | Rate set on a violation. Must be below 1. |
| `THROTTLE_RECOVERY_STEP` | `0.1` | Rate reduction per clean window. Must not exceed the block level. |
| `THROTTLE_CARRY_CAP_WINDOWS` | `5` | Windows a small sample may be carried before it is dropped. |
| `THROTTLE_EVALUATION_WINDOW` | `1m` | Length of the window. |
| `THROTTLE_GRACE` | `5s` | Wait after a window closes before evaluating it. |
| `THROTTLE_FLUSH_INTERVAL` | `1s` | How often a pod writes its counters to Redis. |
| `THROTTLE_STATE_REFRESH_INTERVAL` | `2s` | How often a pod reloads throttle state. |
| `THROTTLE_MAX_STALENESS` | `5m` | Pods fail open when the evaluator's heartbeat has not changed for this long, on the pod's clock. Must exceed window plus grace plus refresh interval. |
| `THROTTLE_STATS_TTL` | `5m` | How long a window's stats are kept. Must exceed window plus grace. |
| `THROTTLE_MAX_BUYERS` | `10000` | Cap on distinct buyers tracked per process. |
| `THROTTLE_BUYER_STATE_TTL` | `1h` | How long a buyer's state, and the set of known buyers, persist after their last write. Must exceed window plus grace. |

Ratios are written as decimals (`0.7`) and must have at most four decimal places.

The binaries have their own variables:

| Variable | Used by | Default | What it does |
|---|---|---|---|
| `REDIS_ADDR` | evaluator, pod | `localhost:6379` | Redis address. |
| `LISTEN_ADDR` | all HTTP services | `:8081` buyers, `:8090` evaluator, `:8080` pod | Listen address. |
| `LOG_LEVEL` | all | `info` | `debug`, `info`, `warn` or `error`. |
| `POD_ID` | pod | `pod` | Name shown in pod logs. |
| `BUYERS_URL` | pod, simulator | `http://localhost:8081` | Base URL of the buyers. |
| `CALL_TIMEOUT` | pod | `150ms` | Per-buyer call deadline. |
| `SUMMARY_INTERVAL` | pod | `5s` | How often a pod logs per-buyer activity. |
| `BUYERS` | buyers | required | Initial buyers, for example `buyer_good:failure=0,timeout=0,participation=1`. |
| `TIMEOUT_DELAY` | buyers | `300ms` | How long a simulated timeout takes. Must exceed `CALL_TIMEOUT`. |
| `AUCTION_URLS` | simulator | required | Comma-separated pod URLs. |
| `SIM_RATE` | simulator | `100` | Auctions per second. |
| `STATS_INTERVAL` | simulator | `10s` | How often totals are logged. |
| `SCENARIO` | simulator | none | Timed buyer changes, for example `30s:buyer_bad:failure_rate=0.8; 150s:buyer_bad:failure_rate=0`. |

The demo settings are in `docker-compose.yml`. The demo uses a 10-second window so a
block and a recovery take minutes rather than hours.

## Watch it work

Ports on the host: evaluator `8090`, pod-1 `8080`, pod-2 `8082`, buyers admin `8081`.

```
curl localhost:8090/state                 # evaluator: the store's view (source of truth)
curl localhost:8080/state                 # pod-1: the state it enforces right now
curl localhost:8090/metrics | grep throttle_evaluator
curl localhost:8080/metrics | grep throttle_pod_throttle_rate
```

`/state` returns `{"buyers":[{"buyer_id","throttle_rate","reason"}]}`, sorted by buyer.
`throttle_rate` is 0 to 1. `reason` is `failure_rate`, `timeout_rate`, `recovering`, or empty.

Break and heal a buyer yourself while the demo runs:

```
curl -X POST -H 'Content-Type: application/json' -d '{"failure_rate":0.8}' localhost:8081/admin/buyers/buyer_bad
curl -X POST -H 'Content-Type: application/json' -d '{"failure_rate":0}'   localhost:8081/admin/buyers/buyer_bad
```

Expected progression for `buyer_bad` (full traffic, judged every window):

1. **Healthy.** `/state` shows rate 0 and an empty reason.
2. **Blocked, within about two windows of the break (about 20 to 25 seconds in the demo).** The evaluator logs
   `level=WARN msg="buyer evaluated" buyer=buyer_bad ... throttle_rate_bp=9500 reason=failure_rate`.
   `/state` shows `0.95` with `failure_rate`. Pod summaries show `skipped` rising to about 95% of invitations.
3. **Recovering, after the heal.** The evaluator logs `level=INFO` lines with
   `reason=recovering` and the rate falling one step per 10-second window. `/state`
   reaches `0` with an empty reason after about 100 seconds of clean windows.

For `buyer_quiet` (about 5% participation, so about 50 calls per window):

- The first window below 100 calls is carried. At rate 0 that is a debug line, so
  nothing shows yet.
- About two windows after the break, the first `WARN` appears with `throttle_rate_bp=9500`
  and `effective_total` around 100. The `total` field shows only that window's calls.
- Afterwards it steps down slowly, about one step per six windows, even while it still
  fails. This is the known low-volume trade-off described in `DESIGN.md`.

Other lines to look for:

- `buyer summary`: per-buyer calls, skipped, ok, failed and timeout for each pod, every 5 seconds.
- `throttler stats`: flushed calls and the failure counters.
- `scenario event applied`: when the simulator changed a buyer.

## Tests

CI runs lint and the full test suite with the race detector on every pull request and on pushes to `main`. A second job validates the compose file and builds the images, without starting them.

```
make test    # go test -race ./...
make lint    # go vet and a gofmt check
```

The evaluator rules are covered by table-driven tests. The store runs against miniredis.
The pod and buyer logic are tested with fake stores, buyers and clocks, so no test sleeps.

## What I would improve with more time

- **Batch the evaluator's store calls.** Each buyer costs three round trips per window.
  Pipelining would make the evaluator's cost independent of the number of buyers.
- **A real Prometheus server and dashboards.** The metrics are exposed, but nothing
  scrapes or charts them yet.
- **Leader election for the evaluator.** Several evaluators can run today. The
  compare-and-set keeps them correct, but they duplicate work.
- **Per-buyer thresholds.** Every buyer uses the same thresholds and minimum sample.
  Buyers with very different volumes or normal error rates would need their own.
