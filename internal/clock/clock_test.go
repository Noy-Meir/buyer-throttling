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

func TestFakeAfterFiresOnlyWhenDeadlinePasses(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	f := NewFake(start)
	ch := f.After(10 * time.Second)

	f.Advance(9 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired before its deadline")
	default:
	}

	f.Advance(time.Second)
	select {
	case got := <-ch:
		assert.Equal(t, start.Add(10*time.Second), got)
	default:
		t.Fatal("timer did not fire at its deadline")
	}
}

func TestFakeAfterNonPositiveFiresImmediately(t *testing.T) {
	f := NewFake(time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC))
	select {
	case <-f.After(0):
	default:
		t.Fatal("zero-duration timer should fire immediately")
	}
}

func TestFakeBlockUntilWaitsForRegisteredTimers(t *testing.T) {
	f := NewFake(time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC))
	done := make(chan struct{})
	go func() {
		f.BlockUntil(2)
		close(done)
	}()
	f.After(time.Second)
	f.After(time.Minute)
	<-done
}
