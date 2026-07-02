package redis

import (
	"testing"
	"time"
)

// TestAdaptiveCoalesce pins the RTT-adaptive default-window derivation:
// loopback RTTs clamp to the fixed floors (behavior identical to the old
// constants), WAN RTTs widen gap and window proportionally, and the ceilings
// bound the tax on a lone late command.
func TestAdaptiveCoalesce(t *testing.T) {
	floor := 200 * time.Microsecond
	cases := []struct {
		name   string
		ewma   time.Duration
		gap    time.Duration
		window time.Duration
	}{
		{"no sample yet", 0, 20 * time.Microsecond, floor},
		{"loopback 100µs", 100 * time.Microsecond, 20 * time.Microsecond, floor},
		{"fast lan 1ms", time.Millisecond, 20 * time.Microsecond, 250 * time.Microsecond},
		{"lan 2ms", 2 * time.Millisecond, 31250 * time.Nanosecond, 500 * time.Microsecond},
		{"wan 52ms", 52 * time.Millisecond, 812500 * time.Nanosecond, 13 * time.Millisecond},
		{"slow wan 200ms (ceilings)", 200 * time.Millisecond, 3 * time.Millisecond, 15 * time.Millisecond},
	}
	for _, c := range cases {
		gap, window := adaptiveCoalesce(int64(c.ewma), floor)
		if gap != c.gap || window != c.window {
			t.Errorf("%s: adaptiveCoalesce(%v) = (%v, %v), want (%v, %v)",
				c.name, c.ewma, gap, window, c.gap, c.window)
		}
	}

	// Enlarged floor (as the stops-growing test does): the floor wins over the
	// ceiling and the gap never exceeds the window.
	gap, window := adaptiveCoalesce(int64(52*time.Millisecond), time.Second)
	if window != time.Second {
		t.Errorf("enlarged floor: window = %v, want 1s", window)
	}
	if gap > window {
		t.Errorf("enlarged floor: gap %v exceeds window %v", gap, window)
	}
}

// TestObserveBatchExecEWMA pins the smoothing behavior.
func TestObserveBatchExecEWMA(t *testing.T) {
	ap := &AutoPipeliner{}
	ap.observeBatchExec(0) // ignored
	if got := ap.execEWMA.Load(); got != 0 {
		t.Fatalf("zero sample stored: %d", got)
	}
	ap.observeBatchExec(80 * time.Millisecond) // first sample: stored as-is
	if got := ap.execEWMA.Load(); got != int64(80*time.Millisecond) {
		t.Fatalf("first sample = %d", got)
	}
	ap.observeBatchExec(160 * time.Millisecond) // ewma += (sample-ewma)/8
	want := int64(80*time.Millisecond) + int64(80*time.Millisecond)/8
	if got := ap.execEWMA.Load(); got != want {
		t.Fatalf("ewma after second sample = %d, want %d", got, want)
	}
}
