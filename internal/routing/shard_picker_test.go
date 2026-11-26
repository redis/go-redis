package routing

import (
	"math"
	"testing"
)

// TestRoundRobinPickerHighCounter pins that Next stays in [0,total) once the
// counter passes math.MaxInt32. Where int is 32 bits the counter used to be
// converted to a negative int, and the cluster client indexed a slice with it.
func TestRoundRobinPickerHighCounter(t *testing.T) {
	const total = 3
	p := &RoundRobinPicker{}
	p.cnt.Store(math.MaxInt32)

	prev := -1
	for i := 0; i < 2*total; i++ {
		got := p.Next(total)
		if got < 0 || got >= total {
			t.Fatalf("Next(%d) = %d, want an index in [0,%d)", total, got, total)
		}
		if prev >= 0 && got != (prev+1)%total {
			t.Fatalf("Next(%d) = %d after %d, want %d", total, got, prev, (prev+1)%total)
		}
		prev = got
	}
}
