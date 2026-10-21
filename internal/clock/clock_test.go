package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFakeNowReturnsStart(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	assert.Equal(t, start, NewFake(start).Now())
}

func TestFakeAdvanceMovesForward(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	f := NewFake(start)

	f.Advance(90 * time.Second)

	assert.Equal(t, start.Add(90*time.Second), f.Now())
}

func TestFakeSetCanMoveBackwards(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	f := NewFake(start)
	f.Advance(time.Hour)

	f.Set(start.Add(-time.Minute))

	assert.Equal(t, start.Add(-time.Minute), f.Now())
}
