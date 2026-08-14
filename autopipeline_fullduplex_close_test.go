package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fdCloseHook is a ProcessHook that does work AFTER next() returns, to prove the
// full-duplex engine tracks its hook-host goroutines so AutoPipeliner.Close waits
// for post-next hook work before returning (#3964). It signals once when it has
// entered the post-next phase, then holds briefly and records completion.
type fdCloseHook struct {
	entered   chan struct{}
	once      sync.Once
	finished  atomic.Bool
	holdFor   time.Duration
	watchName string
}

func (h *fdCloseHook) DialHook(next DialHook) DialHook { return next }

func (h *fdCloseHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() != h.watchName {
			return err
		}
		h.once.Do(func() { close(h.entered) })
		// Post-next work: if Close does not wait for this host goroutine, Close
		// returns while we are still sleeping and finished is still false.
		time.Sleep(h.holdFor)
		h.finished.Store(true)
		return err
	}
}

func (h *fdCloseHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFullDuplexCloseWaitsForHookHosts verifies AutoPipeliner.Close does not
// return until a full-duplex command's post-next ProcessHook has finished. The FD
// hook host is the only goroutine that closes such a command's batch; before the
// fix it was untracked, so Close could return while accepted commands were still
// blocked behind post-reply hooks (drain-before-return contract violated).
func TestFullDuplexCloseWaitsForHookHosts(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	hook := &fdCloseHook{entered: make(chan struct{}), holdFor: 120 * time.Millisecond, watchName: "set"}
	c.AddHook(hook)

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Fire the command on the async face and do NOT read its result (reading would
	// itself block on the host closing the batch, hiding the bug). Wait until the
	// host is in its post-next phase, then Close must wait for it to finish.
	ap.Set(ctx, "fd:close:k", "v", 0)
	select {
	case <-hook.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("post-next hook never ran — command did not reach the FD reader")
	}

	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !hook.finished.Load() {
		t.Fatal("Close returned before the post-next ProcessHook finished — FD hook host not waited (drain-before-return violated)")
	}
}
