package redis

import (
	"math/rand"
	"testing"
)

// fdQueueTag builds a request whose attempts field carries a sequence number,
// so FIFO order can be checked without a real command.
func fdQueueTag(n int) fdReq { return fdReq{attempts: n} }

// TestFDQueueFIFOModel drives the queue with random pushes, batch pushes,
// partial takes and give-backs (pushFront), and checks every take against a
// plain slice model. It pins the queue's order contract across the head
// offset, the reset-on-empty and the compaction.
func TestFDQueueFIFOModel(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		q := newFDQueue(512)
		var model []int
		next := 0
		for op := 0; op < 5000; op++ {
			switch r := rng.Intn(10); {
			case r < 4: // push
				if q.push(fdQueueTag(next)) == fdPushOK {
					model = append(model, next)
					next++
				} else if len(model) < 512 {
					t.Fatalf("seed %d: push refused at depth %d", seed, len(model))
				}
			case r < 6: // a burst of pushes
				for k := 1 + rng.Intn(40); k > 0; k-- {
					if q.push(fdQueueTag(next)) != fdPushOK {
						if len(model) < 512 {
							t.Fatalf("seed %d: push refused at depth %d", seed, len(model))
						}
						break
					}
					model = append(model, next)
					next++
				}
			default: // take, maybe give some back
				got := q.takeInto(nil, 1+rng.Intn(64))
				for i, r := range got {
					if r.attempts != model[i] {
						t.Fatalf("seed %d op %d: took %d at %d, want %d", seed, op, r.attempts, i, model[i])
					}
				}
				model = model[len(got):]
				if g := rng.Intn(len(got) + 1); g > 0 && rng.Intn(3) == 0 {
					back := got[len(got)-g:]
					q.pushFront(back)
					head := make([]int, 0, g+len(model))
					for _, r := range back {
						head = append(head, r.attempts)
					}
					model = append(head, model...)
				}
			}
			if d := q.depth(); d != len(model) {
				t.Fatalf("seed %d op %d: depth %d, model %d", seed, op, d, len(model))
			}
		}
		got := q.drainAll(nil)
		if len(got) != len(model) {
			t.Fatalf("seed %d: drained %d, model %d", seed, len(got), len(model))
		}
		for i, r := range got {
			if r.attempts != model[i] {
				t.Fatalf("seed %d: drained %d at %d, want %d", seed, r.attempts, i, model[i])
			}
		}
	}
}

// FullDuplexFastSubmit shipped in v9.23.0-beta.1. It is now a no-op, but code
// that sets it must keep compiling and validating.
func TestFullDuplexFastSubmitStillAccepted(t *testing.T) {
	opt := &AutoPipelineOptions{FullDuplex: true, FullDuplexFastSubmit: true}
	if err := opt.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// BenchmarkFDQueueDeepDrain is the writer draining a full default-size queue
// in 200-command waves while submitters refill it. Before the head offset,
// every take shifted the whole remaining queue under the lock.
func BenchmarkFDQueueDeepDrain(b *testing.B) {
	const depth, wave = 65536, 200
	q := newFDQueue(depth)
	for i := 0; i < depth; i++ {
		q.push(fdQueueTag(i))
	}
	refill := make([]fdReq, wave)
	dst := make([]fdReq, 0, wave)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = q.takeInto(dst[:0], wave)
		for _, r := range refill {
			q.push(r)
		}
	}
}
