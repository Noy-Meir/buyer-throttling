// Package config loads and validates service configuration from environment
// variables. Ratios are written as decimals (0.7) but stored as integer basis
// points (7000) so that the evaluator never compares floats.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

// basisPoints is the scale of a ratio: 1.0 == 10000 bp.
const basisPoints = 10000

// Environment variable names.
const (
	EnvFailureThreshold     = "THROTTLE_FAILURE_THRESHOLD"
	EnvTimeoutThreshold     = "THROTTLE_TIMEOUT_THRESHOLD"
	EnvMinSample            = "THROTTLE_MIN_SAMPLE"
	EnvBlockLevel           = "THROTTLE_BLOCK_LEVEL"
	EnvRecoveryStep         = "THROTTLE_RECOVERY_STEP"
	EnvCarryCapWindows      = "THROTTLE_CARRY_CAP_WINDOWS"
	EnvEvaluationWindow     = "THROTTLE_EVALUATION_WINDOW"
	EnvGrace                = "THROTTLE_GRACE"
	EnvFlushInterval        = "THROTTLE_FLUSH_INTERVAL"
	EnvStateRefreshInterval = "THROTTLE_STATE_REFRESH_INTERVAL"
	EnvMaxStaleness         = "THROTTLE_MAX_STALENESS"
)

// Config is the validated service configuration. Ratio fields are in basis
// points; durations are time.Duration.
type Config struct {
	FailureThresholdBP int
	TimeoutThresholdBP int
	MinSample          int64
	BlockLevelBP       int
	RecoveryStepBP     int
	CarryCapWindows    int

	EvaluationWindow     time.Duration
	Grace                time.Duration
	FlushInterval        time.Duration
	StateRefreshInterval time.Duration
	MaxStaleness         time.Duration
}

// Defaults returns the default configuration from the assignment and CLAUDE.md.
func Defaults() Config {
	return Config{
		FailureThresholdBP:   7000,
		TimeoutThresholdBP:   3000,
		MinSample:            100,
		BlockLevelBP:         9500,
		RecoveryStepBP:       1000,
		CarryCapWindows:      5,
		EvaluationWindow:     time.Minute,
		Grace:                5 * time.Second,
		FlushInterval:        time.Second,
		StateRefreshInterval: 2 * time.Second,
		MaxStaleness:         5 * time.Minute,
	}
}

// EvaluatorParams projects the evaluator-relevant fields into the evaluator's
// own parameter type, so the evaluator does not depend on this package.
func (c Config) EvaluatorParams() evaluator.Params {
	return evaluator.Params{
		FailureThresholdBP: c.FailureThresholdBP,
		TimeoutThresholdBP: c.TimeoutThresholdBP,
		MinSample:          c.MinSample,
		BlockLevelBP:       c.BlockLevelBP,
		RecoveryStepBP:     c.RecoveryStepBP,
		CarryCapWindows:    c.CarryCapWindows,
	}
}

// FromEnv loads configuration from the process environment.
func FromEnv() (Config, error) {
	return Load(os.LookupEnv)
}

// Load builds a Config from lookup, starting from Defaults. An unset or empty
// variable keeps its default, so docker-compose can pass blank values through.
// Every invalid value is reported in one error, each naming its variable.
func Load(lookup func(key string) (string, bool)) (Config, error) {
	cfg := Defaults()
	l := &loader{lookup: lookup}

	l.ratio(EnvFailureThreshold, &cfg.FailureThresholdBP, inRange(0, basisPoints), "must be greater than 0 and at most 1")
	l.ratio(EnvTimeoutThreshold, &cfg.TimeoutThresholdBP, inRange(0, basisPoints), "must be greater than 0 and at most 1")
	l.ratio(EnvBlockLevel, &cfg.BlockLevelBP, inRange(0, basisPoints-1), "must be greater than 0 and less than 1")
	l.ratio(EnvRecoveryStep, &cfg.RecoveryStepBP, positive[int], "must be greater than 0")

	setInt(l, EnvMinSample, &cfg.MinSample, positive[int64], "must be an integer greater than 0")
	setInt(l, EnvCarryCapWindows, &cfg.CarryCapWindows, positive[int], "must be an integer greater than 0")

	setDuration(l, EnvEvaluationWindow, &cfg.EvaluationWindow, positive[time.Duration], "must be a positive duration, e.g. 1m")
	setDuration(l, EnvGrace, &cfg.Grace, nonNegative, "must be a non-negative duration, e.g. 5s")
	setDuration(l, EnvFlushInterval, &cfg.FlushInterval, positive[time.Duration], "must be a positive duration, e.g. 1s")
	setDuration(l, EnvStateRefreshInterval, &cfg.StateRefreshInterval, positive[time.Duration], "must be a positive duration, e.g. 2s")
	setDuration(l, EnvMaxStaleness, &cfg.MaxStaleness, positive[time.Duration], "must be a positive duration, e.g. 5m")

	if len(l.errs) > 0 {
		return Config{}, errors.Join(l.errs...)
	}
	return cfg, nil
}

// loader reads variables and accumulates errors so that all problems are
// reported at once rather than one per restart.
type loader struct {
	lookup func(key string) (string, bool)
	errs   []error
}

// raw returns the variable's value and whether it is set to something non-empty.
func (l *loader) raw(key string) (string, bool) {
	v, ok := l.lookup(key)
	return v, ok && v != ""
}

func (l *loader) fail(key, raw string, err error) {
	l.errs = append(l.errs, fmt.Errorf("%s=%q: %w", key, raw, err))
}

// ratio parses a decimal ratio into basis points and checks it with valid.
func (l *loader) ratio(key string, dst *int, valid func(int) bool, want string) {
	raw, ok := l.raw(key)
	if !ok {
		return
	}
	bp, err := parseRatioBP(raw)
	if err == nil && !valid(bp) {
		err = errors.New(want)
	}
	if err != nil {
		l.fail(key, raw, err)
		return
	}
	*dst = bp
}

// setInt parses an integer and checks it with valid.
func setInt[T ~int | ~int64](l *loader, key string, dst *T, valid func(T) bool, want string) {
	raw, ok := l.raw(key)
	if !ok {
		return
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		err = fmt.Errorf("%s: %w", want, err)
	} else if !valid(T(v)) {
		err = errors.New(want)
	}
	if err != nil {
		l.fail(key, raw, err)
		return
	}
	*dst = T(v)
}

// setDuration parses a Go duration string and checks it with valid.
func setDuration(l *loader, key string, dst *time.Duration, valid func(time.Duration) bool, want string) {
	raw, ok := l.raw(key)
	if !ok {
		return
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		err = fmt.Errorf("%s: %w", want, err)
	} else if !valid(d) {
		err = errors.New(want)
	}
	if err != nil {
		l.fail(key, raw, err)
		return
	}
	*dst = d
}

// parseRatioBP converts a decimal ratio to basis points. It rejects values
// with more than four decimal places instead of silently rounding them: a
// threshold of 0.12345 must not quietly become 0.1235.
func parseRatioBP(raw string) (int, error) {
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, errors.New("not a number")
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, errors.New("not a finite number")
	}
	scaled := f * basisPoints
	if math.Abs(scaled) > 1e9 {
		return 0, errors.New("out of range")
	}
	bp := math.Round(scaled)
	if math.Abs(scaled-bp) > 1e-6 {
		return 0, errors.New("must have at most 4 decimal places")
	}
	return int(bp), nil
}

// inRange returns a check for lo < bp <= hi.
func inRange(lo, hi int) func(int) bool {
	return func(bp int) bool { return bp > lo && bp <= hi }
}

func positive[T int | int64 | time.Duration](v T) bool { return v > 0 }

func nonNegative(d time.Duration) bool { return d >= 0 }
