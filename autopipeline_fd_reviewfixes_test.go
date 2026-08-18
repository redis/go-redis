package redis

import (
	"context"
	"testing"
	"time"
)

// TestFullDuplexHandoffRecyclesPromptly verifies the writer observes a
// connection marked for maintenance handoff and ends the session with a clean
// recycle well before FullDuplexMaxHold, so the pool can queue the handoff at
// Put instead of the conn continuing to take writes to a moving node until
// max-hold. (Review thread #3789192590.)
func TestFullDuplexHandoffRecyclesPromptly(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Long max-hold and idle so ONLY a handoff can cause a prompt recycle.
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		Unordered:             false,
		MaxConcurrentBatches:  1,
		FullDuplexMaxHold:     30 * time.Second,
		FullDuplexIdleTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Start a session and wait until the engine holds a connection.
	if err := ap.Set(ctx, "fd:handoff:k", "v", 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	cn := ap.fd.curConn.Load()
	for deadline := time.Now().Add(2 * time.Second); cn == nil && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
		cn = ap.fd.curConn.Load()
	}
	if cn == nil {
		t.Fatal("engine never exposed a held connection")
	}

	before := ap.fd.recycles.Load()
	// Mark the held conn for handoff, as a MOVING push handler would.
	if err := cn.MarkForHandoff(":6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	// Keep issuing commands so the writer loops and observes ShouldHandoff; assert
	// a recycle happens far faster than the 30s max-hold.
	start := time.Now()
	recycled := false
	for time.Since(start) < 5*time.Second {
		_ = ap.Get(ctx, "fd:handoff:k").Err() // errors are fine; drives the loop
		if ap.fd.recycles.Load() > before {
			recycled = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !recycled {
		t.Fatalf("session did not recycle after handoff mark within 5s (max-hold is 30s) — writer is not observing ShouldHandoff")
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Fatalf("recycle took %v, far above expectation", el)
	}
}
