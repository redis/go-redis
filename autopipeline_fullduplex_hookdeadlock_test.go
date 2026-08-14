package redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// fdSelfReadHook calls next and then reads the command's OWN result
// (cmd.Err()), the documented pattern also exercised by the async autopipeline
// hook tests. On the full-duplex path the host goroutine is the only code that
// closes the command's batch, so without the executor guard this read blocks on
// batch.done forever (#3964, P1). It records the error it observed.
type fdSelfReadHook struct {
	watchName string
	observed  chan error
}

func (h *fdSelfReadHook) DialHook(next DialHook) DialHook { return next }

func (h *fdSelfReadHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() != h.watchName {
			return err
		}
		// Read own result after next: must return the just-executed view, not
		// block on the batch this very goroutine is responsible for closing.
		got := cmd.Err()
		select {
		case h.observed <- got:
		default:
		}
		return err
	}
}

func (h *fdSelfReadHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFullDuplexHookReadingOwnResultDoesNotDeadlock verifies a ProcessHook that
// reads its command's result after next() completes instead of hanging, on the
// full-duplex path. Before the fix the FD host goroutine did not mark itself as
// the batch executor, so cmd.Err() inside the hook blocked on batch.done (which
// only that goroutine closes) — a hard deadlock.
func TestFullDuplexHookReadingOwnResultDoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	hook := &fdSelfReadHook{watchName: "set", observed: make(chan error, 1)}
	c.AddHook(hook)

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Guard the whole command with a timeout: a deadlock leaves the hook (and so
	// the caller awaiting the batch) blocked forever.
	var callerErr atomic.Value
	done := make(chan struct{})
	go func() {
		if e := ap.Set(ctx, "fd:selfread:k", "v", 0).Err(); e != nil {
			callerErr.Store(e)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deadlock: hook read its command result after next and blocked on its own FD batch")
	}
	if v := callerErr.Load(); v != nil {
		t.Fatalf("set: %v", v)
	}
	select {
	case got := <-hook.observed:
		if got != nil {
			t.Fatalf("hook observed err=%v after next, want nil (just-executed view)", got)
		}
	default:
		t.Fatal("hook did not run on the FD path")
	}
}
