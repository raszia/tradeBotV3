package clock

import (
	"testing"
	"time"
)

func TestSystemNowMovesForward(t *testing.T) {
	c := NewSystem()
	a := c.Now()
	time.Sleep(time.Millisecond)
	b := c.Now()
	if !b.After(a) {
		t.Fatalf("expected system clock to advance: a=%v b=%v", a, b)
	}
}

func TestManualClock(t *testing.T) {
	start := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	m := NewManual(start)

	if got := m.Now(); !got.Equal(start) {
		t.Fatalf("Now() = %v, want %v", got, start)
	}

	got := m.Advance(90 * time.Second)
	want := start.Add(90 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("Advance returned %v, want %v", got, want)
	}
	if !m.Now().Equal(want) {
		t.Fatalf("Now() after Advance = %v, want %v", m.Now(), want)
	}

	reset := start.Add(-time.Hour)
	m.Set(reset)
	if !m.Now().Equal(reset) {
		t.Fatalf("Now() after Set = %v, want %v", m.Now(), reset)
	}
}
