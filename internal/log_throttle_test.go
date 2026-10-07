package internal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// recordingLogger captures every Printf line.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (r *recordingLogger) Printf(_ context.Context, format string, v ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, fmt.Sprintf(format, v...))
}

func (r *recordingLogger) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.lines)
}

func TestThrottledLogger(t *testing.T) {
	rec := &recordingLogger{}
	prev := Logger.Load()
	Logger.Store(rec)
	t.Cleanup(func() { Logger.Store(prev) })

	ctx := context.Background()

	t.Run("one line per interval, Reset re-arms", func(t *testing.T) {
		l := NewThrottledLogger(time.Hour)
		if !l.Printf(ctx, "first %d", 1) {
			t.Fatal("the first line must log")
		}
		if l.Printf(ctx, "second %d", 2) {
			t.Fatal("a second line within the interval must be suppressed")
		}
		if got := rec.count(); got != 1 {
			t.Fatalf("logged %d lines, want 1", got)
		}

		l.Reset()
		if !l.Printf(ctx, "after reset") {
			t.Fatal("the first line after Reset must log")
		}
		if got := rec.count(); got != 2 {
			t.Fatalf("logged %d lines, want 2", got)
		}
	})

	t.Run("logs again once the interval elapsed", func(t *testing.T) {
		l := NewThrottledLogger(10 * time.Millisecond)
		if !l.Printf(ctx, "a") {
			t.Fatal("the first line must log")
		}
		time.Sleep(20 * time.Millisecond)
		if !l.Printf(ctx, "b") {
			t.Fatal("a line after the interval elapsed must log")
		}
	})

	t.Run("zero interval and zero value never throttle", func(t *testing.T) {
		for _, l := range []*ThrottledLogger{NewThrottledLogger(0), {}} {
			for i := range 3 {
				if !l.Printf(ctx, "always %d", i) {
					t.Fatalf("call %d must log with no interval", i)
				}
			}
		}
	})

	t.Run("concurrent callers emit exactly one line per interval", func(t *testing.T) {
		before := rec.count()
		l := NewThrottledLogger(time.Hour)
		var wg sync.WaitGroup
		var logged sync.Map
		for i := range 16 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if l.Printf(ctx, "racing %d", i) {
					logged.Store(i, struct{}{})
				}
			}(i)
		}
		wg.Wait()
		var winners int
		logged.Range(func(any, any) bool { winners++; return true })
		if winners != 1 || rec.count()-before != 1 {
			t.Fatalf("%d callers reported logging and %d lines were written, want 1 and 1",
				winners, rec.count()-before)
		}
	})
}
