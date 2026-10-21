package evaluator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultParams are the assignment defaults in basis points.
var defaultParams = Params{
	FailureThresholdBP: 7000,
	TimeoutThresholdBP: 3000,
	MinSample:          100,
	BlockLevelBP:       9500,
	RecoveryStepBP:     1000,
	CarryCapWindows:    5,
}

// stats builds a WindowStats; ok is implied as total - failed - timeout.
func stats(total, failed, timeout int64) WindowStats {
	return WindowStats{Total: total, Failed: failed, Timeout: timeout}
}

// clean is a healthy window that is large enough to be evaluated.
var clean = stats(100, 0, 0)

// step is one window fed into the evaluator, with the state expected after it.
type step struct {
	window WindowStats
	want   State
}

// scenario is a sequence of windows applied to a starting state.
type scenario struct {
	name   string
	params Params
	start  State
	steps  []step
}

func TestEvaluateScenarios(t *testing.T) {
	blockedFailure := State{ThrottleRateBP: 9500, Reason: ReasonFailureRate}
	blockedTimeout := State{ThrottleRateBP: 9500, Reason: ReasonTimeoutRate}

	tests := []scenario{
		{
			name:  "block on failure rate",
			start: State{},
			steps: []step{{stats(100, 70, 0), blockedFailure}},
		},
		{
			name:  "block on timeout rate",
			start: State{},
			steps: []step{{stats(100, 0, 30), blockedTimeout}},
		},
		{
			name:  "failure rate exactly at threshold blocks (>=)",
			start: State{},
			steps: []step{{stats(200, 140, 0), blockedFailure}},
		},
		{
			name:  "failure rate just below threshold does not block",
			start: State{},
			steps: []step{{stats(200, 139, 0), State{}}},
		},
		{
			name:  "timeout rate exactly at threshold blocks (>=)",
			start: State{},
			steps: []step{{stats(100, 0, 30), blockedTimeout}},
		},
		{
			name:  "timeout rate just below threshold does not block",
			start: State{},
			steps: []step{{stats(100, 0, 29), State{}}},
		},
		{
			name:  "both rates violated: failure rate wins the reason",
			start: State{},
			steps: []step{{stats(100, 70, 30), blockedFailure}},
		},
		{
			name:  "total 0 with no throttle does not block or divide by zero, and carries an empty window",
			start: State{},
			steps: []step{{stats(0, 0, 0), State{CarriedWindows: 1}}},
		},
		{
			name:  "total 0 while throttled holds the rate and carries nothing new",
			start: blockedFailure,
			steps: []step{{stats(0, 0, 0), State{
				ThrottleRateBP: 9500,
				Reason:         ReasonFailureRate,
				CarriedWindows: 1,
			}}},
		},
		{
			name:  "total 99 is insufficient and does not block",
			start: State{},
			steps: []step{{stats(99, 99, 0), State{CarriedStats: stats(99, 99, 0), CarriedWindows: 1}}},
		},
		{
			name:  "total 100 is a valid sample and blocks",
			start: State{},
			steps: []step{{stats(100, 100, 0), blockedFailure}},
		},
		{
			name:  "full recovery from block level to 0 in 10 clean windows",
			start: blockedFailure,
			steps: []step{
				{clean, State{ThrottleRateBP: 8500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 7500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 6500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 5500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 4500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 3500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 2500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 1500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 500, Reason: ReasonRecovering}},
				{clean, State{}},
			},
		},
		{
			name:  "snap-back to block level mid-recovery on failure rate",
			start: blockedFailure,
			steps: []step{
				{clean, State{ThrottleRateBP: 8500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 7500, Reason: ReasonRecovering}},
				{clean, State{ThrottleRateBP: 6500, Reason: ReasonRecovering}},
				{stats(100, 80, 0), blockedFailure},
				{clean, State{ThrottleRateBP: 8500, Reason: ReasonRecovering}},
			},
		},
		{
			name:  "snap-back mid-recovery on timeout rate reports timeout_rate",
			start: State{ThrottleRateBP: 4500, Reason: ReasonRecovering},
			steps: []step{{stats(100, 0, 50), blockedTimeout}},
		},
		{
			name:  "clean window at rate 0 stays at 0",
			start: State{},
			steps: []step{{clean, State{}}, {clean, State{}}},
		},
		{
			name:  "small window at rate 0 carries instead of blocking",
			start: State{},
			steps: []step{{stats(50, 50, 0), State{CarriedStats: stats(50, 50, 0), CarriedWindows: 1}}},
		},
		{
			name:  "low-volume buyer accumulates to 100 at rate 0 and is blocked on the total",
			start: State{},
			steps: []step{
				{stats(40, 40, 0), State{CarriedStats: stats(40, 40, 0), CarriedWindows: 1}},
				{stats(40, 40, 0), State{CarriedStats: stats(80, 80, 0), CarriedWindows: 2}},
				// 80 + 20 = 100 failed of 100: the accumulated sample is judged and blocks.
				{stats(20, 20, 0), blockedFailure},
			},
		},
		{
			name:  "cap reached at rate 0 clears the carry without a block",
			start: State{},
			steps: []step{
				{stats(10, 0, 0), State{CarriedStats: stats(10, 0, 0), CarriedWindows: 1}},
				{stats(10, 0, 0), State{CarriedStats: stats(20, 0, 0), CarriedWindows: 2}},
				{stats(10, 0, 0), State{CarriedStats: stats(30, 0, 0), CarriedWindows: 3}},
				{stats(10, 0, 0), State{CarriedStats: stats(40, 0, 0), CarriedWindows: 4}},
				{stats(10, 0, 0), State{CarriedStats: stats(50, 0, 0), CarriedWindows: 5}},
				// Sixth insufficient window at rate 0: cap reached, carry dropped, no change.
				{stats(10, 0, 0), State{}},
			},
		},
		{
			name:  "cap reached at rate 0 drops a violating sample without judging it",
			start: State{},
			steps: []step{
				{stats(10, 10, 0), State{CarriedStats: stats(10, 10, 0), CarriedWindows: 1}},
				{stats(10, 10, 0), State{CarriedStats: stats(20, 20, 0), CarriedWindows: 2}},
				{stats(10, 10, 0), State{CarriedStats: stats(30, 30, 0), CarriedWindows: 3}},
				{stats(10, 10, 0), State{CarriedStats: stats(40, 40, 0), CarriedWindows: 4}},
				{stats(10, 10, 0), State{CarriedStats: stats(50, 50, 0), CarriedWindows: 5}},
				{stats(10, 10, 0), State{}},
			},
		},
		{
			name:  "carry is cleared after an evaluated window and does not leak into the next",
			start: State{},
			steps: []step{
				{stats(50, 0, 0), State{CarriedStats: stats(50, 0, 0), CarriedWindows: 1}},
				// 50 + 50 = 100: evaluated and clean. Nothing stays carried.
				{stats(50, 0, 0), State{}},
				{stats(30, 0, 0), State{CarriedStats: stats(30, 0, 0), CarriedWindows: 1}},
			},
		},
		{
			name:  "floor at 0: a step larger than the remaining rate lands on 0",
			start: State{ThrottleRateBP: 500, Reason: ReasonRecovering},
			steps: []step{{clean, State{}}},
		},
		{
			name:  "small clean windows while throttled carry and hold the rate",
			start: blockedFailure,
			steps: []step{
				{stats(40, 0, 0), State{
					ThrottleRateBP: 9500,
					Reason:         ReasonFailureRate,
					CarriedStats:   stats(40, 0, 0),
					CarriedWindows: 1,
				}},
				{stats(40, 0, 0), State{
					ThrottleRateBP: 9500,
					Reason:         ReasonFailureRate,
					CarriedStats:   stats(80, 0, 0),
					CarriedWindows: 2,
				}},
				// Accumulated total reaches 100: evaluated as a clean window.
				{stats(20, 0, 0), State{ThrottleRateBP: 8500, Reason: ReasonRecovering}},
			},
		},
		{
			name:  "carried windows accumulate to 100 and then step down on the total",
			start: State{ThrottleRateBP: 5000, Reason: ReasonRecovering},
			steps: []step{
				{stats(50, 0, 0), State{
					ThrottleRateBP: 5000,
					Reason:         ReasonRecovering,
					CarriedStats:   stats(50, 0, 0),
					CarriedWindows: 1,
				}},
				{stats(50, 0, 0), State{ThrottleRateBP: 4000, Reason: ReasonRecovering}},
			},
		},
		{
			name:  "violation found only on accumulated carried stats",
			start: State{ThrottleRateBP: 5000, Reason: ReasonRecovering},
			steps: []step{
				// 30/50 = 60%, below the 70% threshold, so it is only carried.
				{stats(50, 30, 0), State{
					ThrottleRateBP: 5000,
					Reason:         ReasonRecovering,
					CarriedStats:   stats(50, 30, 0),
					CarriedWindows: 1,
				}},
				// Accumulated 70/100 = 70%: blocks, although this window alone is 80%
				// and insufficient on its own.
				{stats(50, 40, 0), blockedFailure},
			},
		},
		{
			name:  "carry cap reached after 5 carried windows is treated as clean",
			start: blockedFailure,
			steps: []step{
				{stats(10, 0, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(10, 0, 0), CarriedWindows: 1}},
				{stats(10, 0, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(20, 0, 0), CarriedWindows: 2}},
				{stats(10, 0, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(30, 0, 0), CarriedWindows: 3}},
				{stats(10, 0, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(40, 0, 0), CarriedWindows: 4}},
				{stats(10, 0, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(50, 0, 0), CarriedWindows: 5}},
				// Sixth insufficient window: cap reached, so it steps down and the carry clears.
				{stats(10, 0, 0), State{ThrottleRateBP: 8500, Reason: ReasonRecovering}},
			},
		},
		{
			name:  "carry cap treats an insufficient violating sample as clean",
			start: blockedFailure,
			steps: []step{
				{stats(10, 10, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(10, 10, 0), CarriedWindows: 1}},
				{stats(10, 10, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(20, 20, 0), CarriedWindows: 2}},
				{stats(10, 10, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(30, 30, 0), CarriedWindows: 3}},
				{stats(10, 10, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(40, 40, 0), CarriedWindows: 4}},
				{stats(10, 10, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(50, 50, 0), CarriedWindows: 5}},
				{stats(10, 10, 0), State{ThrottleRateBP: 8500, Reason: ReasonRecovering}},
			},
		},
		{
			name:  "carried stats clear after a sufficient window",
			start: blockedFailure,
			steps: []step{
				{stats(60, 0, 0), State{ThrottleRateBP: 9500, Reason: ReasonFailureRate, CarriedStats: stats(60, 0, 0), CarriedWindows: 1}},
				{clean, State{ThrottleRateBP: 8500, Reason: ReasonRecovering}},
				// A new small window starts from an empty carry, not the old 60.
				{stats(60, 0, 0), State{ThrottleRateBP: 8500, Reason: ReasonRecovering, CarriedStats: stats(60, 0, 0), CarriedWindows: 1}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := defaultParams
			if tc.params != (Params{}) {
				p = tc.params
			}
			state := tc.start
			for i, s := range tc.steps {
				d := Evaluate(s.window, state, p)
				assert.Equal(t, s.want, d.State, "step %d", i)
				state = d.State
			}
		})
	}
}

// A violation is never judged on fewer than MinSample calls, at any rate.
func TestNoViolationJudgedOnInsufficientSample(t *testing.T) {
	for _, start := range []State{{}, {ThrottleRateBP: 5000, Reason: ReasonRecovering}} {
		state := start
		for i := 0; i < 5; i++ {
			d := Evaluate(stats(10, 10, 0), state, defaultParams) // 100% failed, 10 calls
			require.False(t, d.FailureRateViolated, "window %d, start rate %d", i, start.ThrottleRateBP)
			require.False(t, d.TimeoutRateViolated)
			state = d.State
		}
	}
}

// Effective is the window plus carried stats, so a held window's log line can be read.
func TestDecisionEffectiveIncludesCarry(t *testing.T) {
	first := Evaluate(stats(40, 4, 0), State{}, defaultParams)
	second := Evaluate(stats(40, 4, 0), first.State, defaultParams)
	assert.Equal(t, stats(40, 4, 0), first.Effective)
	assert.Equal(t, stats(80, 8, 0), second.Effective)
}

func TestEvaluateViolationFlags(t *testing.T) {
	tests := []struct {
		name            string
		window          WindowStats
		wantFailureFlag bool
		wantTimeoutFlag bool
	}{
		{"neither violated", stats(100, 0, 0), false, false},
		{"failure only", stats(100, 70, 0), true, false},
		{"timeout only", stats(100, 0, 30), false, true},
		{"both violated are both reported", stats(100, 70, 30), true, true},
		{"insufficient sample reports no violation", stats(99, 99, 0), false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := Evaluate(tc.window, State{}, defaultParams)
			assert.Equal(t, tc.wantFailureFlag, d.FailureRateViolated, "failure flag")
			assert.Equal(t, tc.wantTimeoutFlag, d.TimeoutRateViolated, "timeout flag")
		})
	}
}

func TestEvaluateUsesConfiguredParams(t *testing.T) {
	p := defaultParams
	p.BlockLevelBP = 8000
	p.RecoveryStepBP = 2500
	p.MinSample = 10

	d := Evaluate(stats(10, 10, 0), State{}, p)
	assert.Equal(t, State{ThrottleRateBP: 8000, Reason: ReasonFailureRate}, d.State)

	d = Evaluate(clean, d.State, p)
	assert.Equal(t, State{ThrottleRateBP: 5500, Reason: ReasonRecovering}, d.State)
}
