package redis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// resultPeekHook reads every command's result inside the hook chain — the
// redisotel span-status pattern (after next) and the stricter before-next
// variant from the #3867 deadlock report. Reading a deferred command's result
// on the dispatch goroutine used to block on the batch's done channel, which
// that same goroutine closes only after the hook chain returns: a
// deterministic deadlock on the async face.
type resultPeekHook struct{ before bool }

func (h resultPeekHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h resultPeekHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.before {
			_ = cmd.Err()
		}
		err := next(ctx, cmd)
		if !h.before {
			_ = cmd.Err()
		}
		return err
	}
}

func (h resultPeekHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.before {
			for _, c := range cmds {
				_ = c.Err()
			}
		}
		err := next(ctx, cmds)
		if !h.before {
			for _, c := range cmds {
				_ = c.Err()
			}
		}
		return err
	}
}

// runWithWatchdog fails the test instead of hanging the suite if fn deadlocks.
func runWithWatchdog(t *testing.T, timeout time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("deadlock: hook reading deferred results blocked the dispatch")
	}
}

func testAsyncHookPeek(t *testing.T, before bool) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.AddHook(resultPeekHook{before: before})
	c.FlushDB(ctx)

	ap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	runWithWatchdog(t, 30*time.Second, func() {
		const N = 200
		sets := make([]*redis.StatusCmd, N)
		for i := 0; i < N; i++ {
			sets[i] = ap.Set(ctx, fmt.Sprintf("hookpeek:%d", i), i, 0)
		}
		for i, s := range sets {
			if s.Err() != nil {
				t.Errorf("set %d: %v", i, s.Err())
			}
		}
		for i := 0; i < N; i++ {
			v, err := ap.Get(ctx, fmt.Sprintf("hookpeek:%d", i)).Result()
			if err != nil || v != fmt.Sprint(i) {
				t.Errorf("get %d: v=%q err=%v", i, v, err)
			}
		}
	})
}

// TestAsyncAutoPipelineHookReadsAfterNext: redisotel-style hook (reads results
// after next()). Deadlocked before the innermost-close fix.
func TestAsyncAutoPipelineHookReadsAfterNext(t *testing.T) { testAsyncHookPeek(t, false) }

// TestAsyncAutoPipelineHookReadsBeforeNext: hook reads results before next().
// Deadlocked before the dispatcher-goroutine guard in await().
func TestAsyncAutoPipelineHookReadsBeforeNext(t *testing.T) { testAsyncHookPeek(t, true) }

// TestAsyncAutoPipelineHookSoloCommand drives the lone-command fast path
// (Process chain, not the pipeline chain) with both hook variants.
func TestAsyncAutoPipelineHookSoloCommand(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("before=%v", before), func(t *testing.T) {
			ctx := context.Background()
			c := redis.NewClient(&redis.Options{Addr: ":6379"})
			defer c.Close()
			c.AddHook(resultPeekHook{before: before})
			c.FlushDB(ctx)

			ap, err := c.AsyncAutoPipeline()
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()

			runWithWatchdog(t, 30*time.Second, func() {
				// One command, then an immediate read: the lone-caller flush
				// dispatches it through the solo Process path.
				cmd := ap.Set(ctx, "hookpeek:solo", "v", 0)
				if err := cmd.Err(); err != nil {
					t.Errorf("solo set: %v", err)
				}
				if v, err := ap.Get(ctx, "hookpeek:solo").Result(); err != nil || v != "v" {
					t.Errorf("solo get: v=%q err=%v", v, err)
				}
			})
		})
	}
}

// TestBlockingAutoPipelineHookReads: the blocking face was never affected;
// pin that with the same hooks.
func TestBlockingAutoPipelineHookReads(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("before=%v", before), func(t *testing.T) {
			ctx := context.Background()
			c := redis.NewClient(&redis.Options{Addr: ":6379"})
			defer c.Close()
			c.AddHook(resultPeekHook{before: before})
			c.FlushDB(ctx)

			ap, err := c.AutoPipeline()
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()

			runWithWatchdog(t, 30*time.Second, func() {
				for i := 0; i < 50; i++ {
					if err := ap.Set(ctx, fmt.Sprintf("hookpeek:b:%d", i), i, 0).Err(); err != nil {
						t.Errorf("set %d: %v", i, err)
					}
				}
			})
		})
	}
}

// TestAsyncAutoPipelineDoHookReads drives Do's background goroutine (its own
// close path) under a result-reading ProcessHook.
func TestAsyncAutoPipelineDoHookReads(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("before=%v", before), func(t *testing.T) {
			ctx := context.Background()
			c := redis.NewClient(&redis.Options{Addr: ":6379"})
			defer c.Close()
			c.AddHook(resultPeekHook{before: before})

			ap, err := c.AsyncAutoPipeline()
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()

			runWithWatchdog(t, 30*time.Second, func() {
				cmd := ap.Do(ctx, "PING")
				if v, err := cmd.Result(); err != nil || v != "PONG" {
					t.Errorf("do ping: v=%v err=%v", v, err)
				}
			})
		})
	}
}
