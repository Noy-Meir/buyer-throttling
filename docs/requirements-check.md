# Requirements check

Traceability of `docs/assignment.pdf` against the repository, as of commit `b42d95d`.

Status meanings:
- **PASS**: implemented, and a named test or a recorded run shows it.
- **PARTIAL**: implemented, but a stated part is unproven, undocumented, or differs from the text.
- **MISSING**: not implemented.

"Run" means a local run of the real binaries against a miniredis stand-in, on the demo
settings. Docker was not started, so nothing below is verified on compose.

## Product rules

| # | Requirement (assignment) | Implemented in | Proven by | Status | Note |
|---|---|---|---|---|---|
| R1 | `throttle_rate` is between 0.0 and the block level. 0.0 sends every request; 0.95 skips 95%. | `internal/throttler/throttler.go` `ShouldCall`; `internal/config` `BlockLevelBP` | `TestShouldCallRateZeroAlwaysTrueWithoutDrawing`, `TestShouldCallSkipsAtBoundaryOfRandomDraw`, `TestShouldCallRateNinetyFivePercentStatistically` | PASS | |
| R2 | Traffic to a buyer is never cut to zero. | Block level validated to be below 1 (`internal/config`) | `TestLoadValidation` ("block level of 1 is rejected"); `TestShouldCallRateNinetyFivePercentStatistically` (5% still pass) | PASS | |
| R3 | The evaluator runs periodically and, per buyer, reads the last window's stats collected from all pods. | `internal/evaluator/runner/runner.go` `Run`, `Tick`; `internal/store/redis.go` `WindowStats` | `TestRunEvaluatesLastClosedWindowAtBoundaryPlusGrace`; `TestTickEvaluatesEveryKnownBuyerIndependently`; local run | PASS | Stats come from all pods through the shared `stats:{buyer}:{window}` hash. |
| R4 | The evaluator decides the new rate from the stats and the previous state. | `internal/evaluator/evaluator.go` `Evaluate` | `TestEvaluateScenarios` (27 cases) | PASS | |
| R5 | The evaluator writes the new state where the pods can read it. | `internal/store/redis.go` `CompareAndSetState`; pod refresher in `internal/throttler/loop.go` | `TestCompareAndSetRoundTripsStateAndCarry`; `TestStatesKeepsLastSnapshotWhenStoreIsDown`; local run (pod `/state` matched store `/state`) | PASS | |
| R6 | If `failed / total` or `timeouts / total` reaches its threshold, the rate goes straight to the block level. | `evaluator.go` `reaches`, `blocked` | `TestEvaluateScenarios` ("exactly at threshold blocks (>=)", "timeout rate exactly at threshold"); `TestEvaluateViolationFlags` | PASS | Threshold check is integer: `count*10000 >= threshold*total`. |
| R7 | If the window has fewer requests than the minimum sample, the rates "must not trigger a block". | `evaluator.go` `insufficientSample`: carries the window and judges the carried-plus-current sample | `TestEvaluateScenarios` ("carried windows accumulate to 100", "violation found only on accumulated carried stats"); `TestNoViolationJudgedOnInsufficientSample` | **PARTIAL** | DESIGN.md now states this reading explicitly. The literal assignment text still says "the window", so the difference remains. See F1. |
| R8 | After a block, every clean window lowers the rate by the step, until 0.0. | `evaluator.go` `stepDown` | `TestEvaluateScenarios` ("full recovery from block level to 0 in 10 clean windows") | PASS | |
| R9 | A violation at any point, including mid-recovery, sends the rate straight back to the block level. | `evaluator.go` `Evaluate` (violation branch is checked before the clean step) | `TestEvaluateScenarios` ("snap-back to block level mid-recovery") | PASS | |
| R10 | Default configuration, and every value configurable. | `internal/config/config.go` `Defaults`, `Load` | `TestLoadDefaults` (checks 0.7, 0.3, 100, 0.95, 0.1, 1m); `TestLoadOverrides` | PASS | |
| R11 | Each pod makes its own decisions, but all pods must converge on the same view of buyer health. | Shared Redis state; shared window index `store.WindowIndex`; idempotent CAS; 2s refresh | `TestCompareAndSetIsIdempotentForSameWindow`, `TestConcurrentCompareAndSetHasExactlyOneWinner`; design in DESIGN.md | **PARTIAL** | Convergence is argued and bounded (about 2s after the write), but no automated test runs two pods against one evaluator. The only multi-pod evidence is a manual local run. |
| R12 | Constraint: the per-request check adds as little latency as possible. | No network or lock on `ShouldCall`/`Record`; atomic snapshot and counters | Benchmarks: about 117 ns (1 core) and 21 ns (8 cores) for `ShouldCall`; 0 allocations | PASS | Microbenchmarks only. See R13. |
| R13 | Constraint: the system handles 100k+ requests per second across all pods. | Not measured | None | **MISSING** | No load test. The benchmarks measure one process. The evaluator's cost is about 3 store calls per buyer per window, and that has not been measured at scale. |
| R14 | `ShouldCall(buyerID) bool` | `internal/throttler/throttler.go` | `TestShouldCall*` | PASS | |
| R15 | `Record(buyerID, result)` with `ok`, `failed` or `timeout`. | `throttler.go` `Record`, `Result` constants | `TestInvalidRecordIsCountedAndIgnored`; `TestConcurrentRecordAndShouldCallUnderRace` | PASS | |
| R16 | Cases the rules do not specify must be decided and documented in the design doc. | DESIGN.md "Decisions on the ambiguous cases" | Reviewed | PASS | Carry, cap, dropped batch, fail-open and staleness are all in the table. R7 is the exception. |

## Deliverables

| # | Deliverable (assignment) | Implemented in | Proven by | Status | Note |
|---|---|---|---|---|---|
| D1 | Design doc: about one page; components; stats and state flow; fast check; multi-pod consistency; trade-offs. | `DESIGN.md` | Read | **PARTIAL** | All sections are present, and the R7 reading is now stated. The file is about 1,220 words, still more than one page with the tables. See F7. |
| D2 | Code: well structured, clear separation of concerns, meaningful interfaces. | Packages `config`, `evaluator`, `evaluator/runner`, `throttler`, `store`, `auction`, `buyersim`, `simulator`, `metrics`, `api`, `clock`; interfaces `Store`, `Clock`, `Random`, `Caller`, `Throttler` | Layout in CLAUDE.md; `go vet`, `make lint` | PASS | |
| D3 | Tests cover block, recovery steps, snap-back, minimum sample, and the edge cases identified. | `internal/evaluator/evaluator_test.go`, `runner_test.go` | `make test` exits 0 | PASS | The minimum-sample tests check the carry reading (R7), not the literal reading. |
| D4 | Store: any store that fits. | `internal/store/redis.go`, Lua CAS | `redis_test.go` on miniredis (idempotence, TTL expiry and refresh, concurrency) | PASS | |
| D5 | `docker-compose up` starts the store, the evaluator, at least 2 pods, and a simulator with configurable fake buyers. | `docker-compose.yml`: redis, buyers, evaluator, pod-1, pod-2, simulator | `docker compose config -q` exits 0 | **PARTIAL** | The README now gives the literal command. The stack has never been built or started, so the runtime claim is unverified. See F3. |
| D6 | Watch a bad buyer get blocked and then gradually recover, in logs. | Evaluator `buyer evaluated` lines; pod `buyer summary` lines; simulator scenario | Local run with the real binaries: block at 9500 with `failure_rate`, then ten steps to 0 | PASS | Evidence is from the local run, not compose. |
| D7 | Simulator with fake buyers whose failure and timeout rates are configurable. | `internal/buyersim` (`Behavior`, admin API, `ParseSpec`); `cmd/simulator` `SCENARIO` | `TestRegistryUpdateIsPartialAndAllOrNothing`; `TestBidFailureRange`; `TestTimeoutOutcomeHonoursClientCancel`; `TestParseScenario` | **PARTIAL** | The timeout path is configurable and unit-tested. No run exercised it: every logged summary shows `timeout=0`. See F4. |
| D8 | README: how to run, how to configure, how to see the throttling. | `README.md` | Read; every variable name, default and port checked against the code | **PASS** | Run (with the literal `docker compose up --build`), configure and watch sections are present. The 10s demo window is stated. |

## Bonuses

| # | Bonus (assignment) | Implemented in | Proven by | Status | Note |
|---|---|---|---|---|---|
| B1 | Write it in Go. | Whole repository | `go.mod` (`go 1.27.3`) | PASS | |
| B2 | Metrics for throttle rate, error rates and request counts per buyer. | `internal/metrics/pod.go` (calls, skipped, ok, failed, timeout, rate); `internal/metrics/evaluator.go` (rate, failure rate, timeout rate, evaluations, violations, carried windows) | `TestPodCollectorReportsCountersPerBuyer`; `TestEvaluatorCollectorReportsLastDecision`; `cmd/pod/http_test.go` `TestPodMetricsExposeEnforcedRate`; local `curl /metrics` | PASS | Pods expose counters, not an error-rate gauge. The rate is a PromQL division, which is standard but not a series. |
| B3 | `GET /state` in the assignment's format. | `internal/api/state.go`; pod (`cmd/pod/http.go`) and evaluator (`cmd/evaluator/main.go`) | `TestStateFormatMatchesAssignment`; `TestStateSortsBuyersByID`; `TestPodStateServedFromLocalSnapshot` | PASS | Rates are JSON numbers, so `0` is shown where the example shows `0.0`. Both are valid JSON. |
| B4 | "Anything else you think makes the system better: tell us why in the README." | README "Why the extras" section, plus "What I would improve" | Read | **PASS** | The README now says why the two `/state` views, the metrics collector, carry-over and the idempotent compare-and-set help. |

## Handover

| # | Requirement | Status | Note |
|---|---|---|---|
| H1 | Hand over through GitHub. | **PARTIAL** | `origin` is set and `origin/main` is at `55adf9a`. Local commits after that are not pushed, because you asked to push yourself. |

## Findings for the author

- **F1 (R7, conflict).** The assignment says a window "with fewer requests than the minimum sample size must not trigger a block". Our rule judges the carried-plus-current total, so a window alone below 100 calls can produce a block. DESIGN.md now states this reading. CLAUDE.md says to flag conflicts with the assignment rather than resolve them silently, so this still needs your decision.
  - keep the carry, and state the reading explicitly in DESIGN.md as an interpretation of the rule;
  - judge only windows that meet the minimum alone, and accept that low-volume buyers are never blocked.
- **F2 (closed).** The README now shows the literal `docker compose up --build`, and says `docker-compose up` works on older installs.
- **F3 (D5).** The compose stack has been validated (`docker compose config -q`) but never built or started in this environment. The image tags `golang:1.27.3-alpine` and `alpine:3.20` are unverified.
- **F4 (D7).** The timeout path has not run end-to-end. Its tests are at the unit level.
- **F5 (R11).** Multi-pod convergence has no automated test. It was seen only in a manual local run.
- **F6 (R13).** No measurement at 100k requests per second. The benchmarks are per process.
- **F7 (D1).** DESIGN.md is about 1,220 words, more than one page. The length has not been reduced yet.
- **F8 (closed).** The README has a "Why the extras" section.
- **F9 (H1).** Local commits after `55adf9a` are not pushed. Pushing is your decision.
