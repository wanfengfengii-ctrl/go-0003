package clock

import (
	"testing"
	"time"
)

func TestManualAdvance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := NewManual(start)
	if !m.Now().Equal(start) {
		t.Fatalf("Now = %v, want %v", m.Now(), start)
	}
	m.Advance(999 * time.Millisecond)
	if got := m.Now(); !got.Equal(start.Add(999 * time.Millisecond)) {
		t.Fatalf("after 999ms: Now = %v, want %v", got, start.Add(999*time.Millisecond))
	}
	m.Advance(time.Millisecond)
	if got := m.Now(); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("after +1ms: Now = %v, want %v", got, start.Add(time.Second))
	}
}

func TestManualSet(t *testing.T) {
	m := NewManual(time.Unix(1, 0))
	later := time.Date(2030, 5, 5, 5, 5, 5, 0, time.UTC)
	m.Set(later)
	if !m.Now().Equal(later) {
		t.Fatalf("Set did not replace time: %v", m.Now())
	}
}

func TestRealIsMonotonicIncreasing(t *testing.T) {
	r := Real{}
	a := r.Now()
	time.Sleep(time.Millisecond)
	b := r.Now()
	if !b.After(a) {
		t.Fatal("Real clock did not advance")
	}
}
