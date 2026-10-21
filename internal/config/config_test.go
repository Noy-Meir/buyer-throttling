package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// env returns a lookup over a fixed map, so tests never touch the process env.
func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(nil))
	require.NoError(t, err)
	assert.Equal(t, Defaults(), cfg)

	// The defaults match the assignment table, in basis points and durations.
	assert.Equal(t, 7000, cfg.FailureThresholdBP)
	assert.Equal(t, 3000, cfg.TimeoutThresholdBP)
	assert.Equal(t, int64(100), cfg.MinSample)
	assert.Equal(t, 9500, cfg.BlockLevelBP)
	assert.Equal(t, 1000, cfg.RecoveryStepBP)
	assert.Equal(t, 5, cfg.CarryCapWindows)
	assert.Equal(t, time.Minute, cfg.EvaluationWindow)
	assert.Equal(t, 5*time.Second, cfg.Grace)
	assert.Equal(t, time.Second, cfg.FlushInterval)
	assert.Equal(t, 2*time.Second, cfg.StateRefreshInterval)
	assert.Equal(t, 5*time.Minute, cfg.MaxStaleness)
}

func TestLoadOverrides(t *testing.T) {
	tests := []struct {
		name   string
		env    map[string]string
		check  func(t *testing.T, c Config)
		errTxt string
	}{
		{
			name: "decimal ratios become basis points",
			env: map[string]string{
				EnvFailureThreshold: "0.5",
				EnvTimeoutThreshold: "0.25",
				EnvBlockLevel:       "0.9",
				EnvRecoveryStep:     "0.05",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 5000, c.FailureThresholdBP)
				assert.Equal(t, 2500, c.TimeoutThresholdBP)
				assert.Equal(t, 9000, c.BlockLevelBP)
				assert.Equal(t, 500, c.RecoveryStepBP)
			},
		},
		{
			name: "threshold of exactly 1 is allowed",
			env:  map[string]string{EnvFailureThreshold: "1"},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 10000, c.FailureThresholdBP)
			},
		},
		{
			name: "four decimal places are exact",
			env:  map[string]string{EnvBlockLevel: "0.9999"},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 9999, c.BlockLevelBP)
			},
		},
		{
			name: "integers and durations are read",
			env: map[string]string{
				EnvMinSample:            "250",
				EnvCarryCapWindows:      "3",
				EnvEvaluationWindow:     "30s",
				EnvGrace:                "0s",
				EnvFlushInterval:        "500ms",
				EnvStateRefreshInterval: "3s",
				EnvMaxStaleness:         "10m",
				EnvStatsTTL:             "10m",
				EnvMaxBuyers:            "50",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, int64(250), c.MinSample)
				assert.Equal(t, 3, c.CarryCapWindows)
				assert.Equal(t, 30*time.Second, c.EvaluationWindow)
				assert.Equal(t, time.Duration(0), c.Grace, "grace may be zero")
				assert.Equal(t, 500*time.Millisecond, c.FlushInterval)
				assert.Equal(t, 3*time.Second, c.StateRefreshInterval)
				assert.Equal(t, 10*time.Minute, c.MaxStaleness)
				assert.Equal(t, 10*time.Minute, c.StatsTTL)
				assert.Equal(t, 50, c.MaxBuyers)
			},
		},
		{
			name: "empty value keeps the default",
			env:  map[string]string{EnvBlockLevel: "", EnvMinSample: ""},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 9500, c.BlockLevelBP)
				assert.Equal(t, int64(100), c.MinSample)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(env(tc.env))
			require.NoError(t, err)
			tc.check(t, cfg)
		})
	}
}

func TestLoadValidation(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{"block level of 0 is rejected", EnvBlockLevel, "0", "THROTTLE_BLOCK_LEVEL"},
		{"block level of 1 is rejected", EnvBlockLevel, "1", "must be greater than 0 and less than 1"},
		{"block level above 1 is rejected", EnvBlockLevel, "1.5", "less than 1"},
		{"block level that rounds to 1 is rejected", EnvBlockLevel, "0.99999", "at most 4 decimal places"},
		{"negative block level is rejected", EnvBlockLevel, "-0.5", "less than 1"},
		{"recovery step of 0 is rejected", EnvRecoveryStep, "0", "must be greater than 0"},
		{"recovery step above block level is rejected", EnvRecoveryStep, "0.96", "less than or equal to THROTTLE_BLOCK_LEVEL"},
		{"recovery step below one basis point is rejected", EnvRecoveryStep, "0.00001", "at most 4 decimal places"},
		{"failure threshold of 0 is rejected", EnvFailureThreshold, "0", "must be greater than 0 and at most 1"},
		{"failure threshold above 1 is rejected", EnvFailureThreshold, "1.01", "at most 1"},
		{"timeout threshold of 0 is rejected", EnvTimeoutThreshold, "0", "at most 1"},
		{"timeout threshold above 1 is rejected", EnvTimeoutThreshold, "2", "at most 1"},
		{"threshold with five decimals is rejected, not rounded", EnvFailureThreshold, "0.12345", "at most 4 decimal places"},
		{"non-numeric ratio is rejected", EnvBlockLevel, "high", "not a number"},
		{"NaN ratio is rejected", EnvBlockLevel, "NaN", "not a finite number"},
		{"infinite ratio is rejected", EnvBlockLevel, "+Inf", "not a finite number"},
		{"absurdly large ratio is rejected", EnvBlockLevel, "1e300", "out of range"},
		{"min sample of 0 is rejected", EnvMinSample, "0", "greater than 0"},
		{"min sample must be an integer", EnvMinSample, "100.5", "must be an integer greater than 0"},
		{"carry cap of 0 is rejected", EnvCarryCapWindows, "0", "greater than 0"},
		{"window of 0 is rejected", EnvEvaluationWindow, "0s", "positive duration"},
		{"negative flush interval is rejected", EnvFlushInterval, "-1s", "positive duration"},
		{"state refresh must be a duration", EnvStateRefreshInterval, "2", "positive duration"},
		{"negative grace is rejected", EnvGrace, "-1s", "non-negative duration"},
		{"max staleness of 0 is rejected", EnvMaxStaleness, "0", "positive duration"},
		{"stats TTL of 0 is rejected", EnvStatsTTL, "0s", "positive duration"},
		{"stats TTL not longer than window plus grace is rejected", EnvStatsTTL, "1m", "must be greater than THROTTLE_EVALUATION_WINDOW + THROTTLE_GRACE"},
		{"max buyers of 0 is rejected", EnvMaxBuyers, "0", "greater than 0"},
		{"max buyers must be an integer", EnvMaxBuyers, "many", "must be an integer greater than 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(env(map[string]string{tc.key: tc.value}))
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.key)
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestLoadReportsAllProblemsAtOnce(t *testing.T) {
	_, err := Load(env(map[string]string{
		EnvBlockLevel:   "1",
		EnvRecoveryStep: "0",
		EnvMinSample:    "abc",
	}))
	require.Error(t, err)
	assert.ErrorContains(t, err, EnvBlockLevel)
	assert.ErrorContains(t, err, EnvRecoveryStep)
	assert.ErrorContains(t, err, EnvMinSample)
}

func TestLoadAcceptsRecoveryStepEqualToBlockLevel(t *testing.T) {
	cfg, err := Load(env(map[string]string{EnvRecoveryStep: "0.95"}))
	require.NoError(t, err)
	assert.Equal(t, cfg.BlockLevelBP, cfg.RecoveryStepBP)
}

func TestEvaluatorParamsMapsFields(t *testing.T) {
	cfg := Defaults()
	cfg.FailureThresholdBP = 6000

	p := cfg.EvaluatorParams()

	assert.Equal(t, 6000, p.FailureThresholdBP)
	assert.Equal(t, cfg.TimeoutThresholdBP, p.TimeoutThresholdBP)
	assert.Equal(t, cfg.MinSample, p.MinSample)
	assert.Equal(t, cfg.BlockLevelBP, p.BlockLevelBP)
	assert.Equal(t, cfg.RecoveryStepBP, p.RecoveryStepBP)
	assert.Equal(t, cfg.CarryCapWindows, p.CarryCapWindows)
}
