package redis

import (
	"math/rand"
	"testing"
)

// mkReq returns an fdReq tagged with a unique *apBatch pointer used as identity
// in the model comparison.
func mkReq() fdReq { return fdReq{batch: newAPBatch()} }

// TestFDInflightRingModel compares the ring buffer against a reference []fdReq
// model over a long randomized sequence of push / advance / frontBatch /
// takeRemaining, deliberately driving the head around the array so growth and
// snapshots straddle the wrap seam — the one place a ring breaks and that no
// existing FD test targets.
func TestFDInflightRingModel(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))
	for trial := 0; trial < 200; trial++ {
		f := newFDInflightCap(rng.Intn(8)) // mix of presized and grow-from-zero
		var model []fdReq
		var scratch []fdReq

		for step := 0; step < 400; step++ {
			switch rng.Intn(3) {
			case 0: // push a batch
				k := 1 + rng.Intn(20)
				reqs := make([]fdReq, k)
				for i := range reqs {
					reqs[i] = mkReq()
				}
				f.pushBatch(reqs)
				model = append(model, reqs...)
			case 1: // advance (pop front)
				if len(model) == 0 {
					continue
				}
				n := rng.Intn(len(model) + 1)
				f.advance(n)
				model = model[n:]
			case 2: // frontBatch snapshot must equal model front prefix, in order
				if len(model) == 0 {
					continue // frontBatch blocks by contract on an empty, open ring
				}
				scratch, _ = f.frontBatch(scratch)
				want := len(model)
				if want > fdReadBatch {
					want = fdReadBatch
				}
				if len(scratch) != want {
					t.Fatalf("trial %d step %d: frontBatch len=%d want=%d", trial, step, len(scratch), want)
				}
				for i := 0; i < want; i++ {
					if scratch[i].batch != model[i].batch {
						t.Fatalf("trial %d step %d: frontBatch[%d] mismatch", trial, step, i)
					}
				}
			}
			if f.len() != len(model) {
				t.Fatalf("trial %d step %d: len=%d want=%d", trial, step, f.len(), len(model))
			}
			if f.empty() != (len(model) == 0) {
				t.Fatalf("trial %d step %d: empty=%v want=%v", trial, step, f.empty(), len(model) == 0)
			}
		}

		// takeRemaining must return exactly the model tail, in order.
		rem := f.takeRemaining()
		if len(rem) != len(model) {
			t.Fatalf("trial %d: takeRemaining len=%d want=%d", trial, len(rem), len(model))
		}
		for i := range rem {
			if rem[i].batch != model[i].batch {
				t.Fatalf("trial %d: takeRemaining[%d] mismatch", trial, i)
			}
		}
	}
}

// TestFDInflightRingGrowWhileWrapped forces the specific hazard: grow the ring
// while the live window straddles the end of the backing array (head > 0 and
// the tail has wrapped to the front), then verify order is preserved end to end.
func TestFDInflightRingGrowWhileWrapped(t *testing.T) {
	f := newFDInflightCap(8)
	var model []fdReq

	push := func(k int) {
		reqs := make([]fdReq, k)
		for i := range reqs {
			reqs[i] = mkReq()
		}
		f.pushBatch(reqs)
		model = append(model, reqs...)
	}
	adv := func(n int) { f.advance(n); model = model[n:] }

	push(8)  // fill: head=0 count=8 cap=8
	adv(5)   // head=5 count=3
	push(4)  // tail wraps past end: entries at 5,6,7,0,... head=5 count=7 cap=8 (still fits)
	adv(2)   // head=7 count=5
	push(10) // count 15 > cap 8 -> grow WHILE wrapped (head=7)
	// Advance across what was the old wrap seam and verify order throughout.
	for f.len() > 0 {
		var snap []fdReq
		snap, _ = f.frontBatch(snap)
		if snap[0].batch != model[0].batch {
			t.Fatalf("front mismatch after wrapped grow")
		}
		adv(1)
	}
	if len(model) != 0 {
		t.Fatalf("model not drained: %d left", len(model))
	}
	if rem := f.takeRemaining(); rem != nil {
		t.Fatalf("takeRemaining after drain should be nil, got %d", len(rem))
	}
}

// TestFDInflightRingAdvanceEmpty guards against a divide-by-zero in advance on a
// never-grown ring (nil backing buffer): n>0 clamps to 0, and the modulo update
// must not run. Regression for the copilot review on #3970.
func TestFDInflightRingAdvanceEmpty(t *testing.T) {
	f := newFDInflight() // zero-cap: buf is nil until first push
	f.advance(5)         // must be a no-op, not a panic
	if f.len() != 0 {
		t.Fatalf("len=%d after advance on empty ring, want 0", f.len())
	}
	// Also after draining a grown ring back to empty.
	f.pushBatch([]fdReq{mkReq(), mkReq()})
	f.advance(2)
	f.advance(3) // over-advance on an empty (but grown) ring: no-op, no panic
	if f.len() != 0 {
		t.Fatalf("len=%d after over-advance, want 0", f.len())
	}
}

// TestFDInflightRingZeroesConsumed guards the load-bearing zeroing in advance:
// popped slots must be cleared so drained entries don't pin cmd/ctx/batch for
// the life of the session.
func TestFDInflightRingZeroesConsumed(t *testing.T) {
	f := newFDInflightCap(4)
	reqs := []fdReq{mkReq(), mkReq(), mkReq(), mkReq()}
	f.pushBatch(reqs)
	f.advance(4)
	for i := range f.buf {
		if f.buf[i].batch != nil {
			t.Fatalf("buf[%d] not zeroed after advance: %#v", i, f.buf[i])
		}
	}
}
