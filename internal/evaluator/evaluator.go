// Package evaluator holds the throttling rules as a pure function. It performs
// no I/O, reads no clock and knows nothing about Redis: given the last window's
// stats, the buyer's previous state and the parameters, it returns the next
// state. Everything that touches the outside world lives in the caller.
//
// All ratios are integer basis points (10000 == 1.0) so that threshold checks
// are exact integer comparisons.
package evaluator

// basisPoints is the scale of a ratio.
const basisPoints = 10000

// Reason explains why a buyer's throttle rate is what it is.
type Reason string

const (
	ReasonFailureRate Reason = "failure_rate"
	ReasonTimeoutRate Reason = "timeout_rate"
	ReasonRecovering  Reason = "recovering"
	ReasonNone        Reason = ""
)

// WindowStats are the aggregated call outcomes for one buyer in one window.
// ok, failed and timeout are mutually exclusive, so ok = Total - Failed - Timeout.
type WindowStats struct {
	Total   int64
	Failed  int64
	Timeout int64
}

// Add returns the element-wise sum of s and o.
func (s WindowStats) Add(o WindowStats) WindowStats {
	return WindowStats{
		Total:   s.Total + o.Total,
		Failed:  s.Failed + o.Failed,
		Timeout: s.Timeout + o.Timeout,
	}
}

// Params are the evaluator's tunables, all in basis points or counts.
type Params struct {
	FailureThresholdBP int
	TimeoutThresholdBP int
	MinSample          int64
	BlockLevelBP       int
	RecoveryStepBP     int
	CarryCapWindows    int
}

// State is the per-buyer throttle state. It is what the evaluator persists and
// what the auction pods read.
type State struct {
	ThrottleRateBP int
	Reason         Reason

	// CarriedStats holds the stats of insufficient-sample windows that have not
	// yet been evaluated, and CarriedWindows counts how many such windows.
	CarriedStats   WindowStats
	CarriedWindows int
}

// Decision is the result of one evaluation. The violation flags are reported
// separately from State so the caller can log every violated rate, even though
// only the first one (failure rate) sets the reason.
type Decision struct {
	State               State
	FailureRateViolated bool
	TimeoutRateViolated bool
}

// Evaluate computes the next state for a buyer from the stats of the last
// closed window, the previous state and the parameters.
func Evaluate(window WindowStats, prev State, p Params) Decision {
	// Carried windows are folded in before any decision, so a buyer that only
	// sends a trickle of traffic is judged on enough calls to mean something.
	effective := prev.CarriedStats.Add(window)

	// A zero total is always insufficient, even if MinSample were misconfigured
	// to 0, which also keeps the ratio arithmetic below safe.
	if effective.Total == 0 || effective.Total < p.MinSample {
		return insufficientSample(prev, effective, p)
	}

	failureViolated := reaches(effective.Failed, effective.Total, p.FailureThresholdBP)
	timeoutViolated := reaches(effective.Timeout, effective.Total, p.TimeoutThresholdBP)
	d := Decision{
		FailureRateViolated: failureViolated,
		TimeoutRateViolated: timeoutViolated,
	}

	switch {
	case failureViolated:
		d.State = blocked(ReasonFailureRate, p)
	case timeoutViolated:
		d.State = blocked(ReasonTimeoutRate, p)
	default:
		d.State = stepDown(prev.ThrottleRateBP, p)
	}
	return d
}

// insufficientSample handles a window whose effective total is below MinSample.
// The rates are not meaningful yet, so this path never blocks a buyer.
func insufficientSample(prev State, effective WindowStats, p Params) Decision {
	// A buyer that is not throttled has nothing to hold, so nothing is carried.
	if prev.ThrottleRateBP == 0 {
		return Decision{State: State{}}
	}

	// Hold the current rate and keep collecting. A throttled buyer that stays
	// quiet is not treated as healthy until the carry cap is reached.
	if prev.CarriedWindows < p.CarryCapWindows {
		return Decision{State: State{
			ThrottleRateBP: prev.ThrottleRateBP,
			Reason:         prev.Reason,
			CarriedStats:   effective,
			CarriedWindows: prev.CarriedWindows + 1,
		}}
	}

	// Carry cap reached: count the window as clean. The carried sample is
	// discarded, and violations in it are not checked.
	return Decision{State: stepDown(prev.ThrottleRateBP, p)}
}

// reaches reports whether count/total is at or above the threshold. The
// comparison is count*10000 >= threshold*total, so no division or float is
// involved. The caller guarantees total > 0.
func reaches(count, total int64, thresholdBP int) bool {
	return count*basisPoints >= int64(thresholdBP)*total
}

// blocked sends the rate straight to the block level with the given reason.
func blocked(reason Reason, p Params) State {
	return State{ThrottleRateBP: p.BlockLevelBP, Reason: reason}
}

// stepDown takes one clean-window step down from the current rate, floored at
// zero. A rate of zero stays zero.
func stepDown(rateBP int, p Params) State {
	if rateBP == 0 {
		return State{}
	}
	next := rateBP - p.RecoveryStepBP
	if next <= 0 {
		return State{}
	}
	return State{ThrottleRateBP: next, Reason: ReasonRecovering}
}
