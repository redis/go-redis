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

func (r *recordingLogger) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.lines) == 0 {
		return ""
	}
	return r.lines[len(r.lines)-1]
}

func TestThrottledLogger(t *testing.T) {
	ctx := context.Background()

	t.Run("one line per interval, suppressed calls counted on the next", func(t *testing.T) {
		rec := &recordingLogger{}
		l := NewThrottledLogger(10*time.Millisecond, rec)
		l.Printf(ctx, "first %d", 1)
		l.Printf(ctx, "second %d", 2)
		l.Printf(ctx, "third %d", 3)
		if got, last := rec.count(), rec.last(); got != 1 || last != "first 1" {
			t.Fatalf("logged %d line(s), last %q; want 1 and \"first 1\"", got, last)
		}

		time.Sleep(20 * time.Millisecond)
		l.Printf(ctx, "fourth %d", 4)
		want := "fourth 4 (2 similar line(s) suppressed in the last 10ms)"
		if got, last := rec.count(), rec.last(); got != 2 || last != want {
			t.Fatalf("logged %d line(s), last %q; want 2 and %q", got, last, want)
		}

		// The count was handed over: a quiet period leaves no suffix.
		time.Sleep(20 * time.Millisecond)
		l.Printf(ctx, "fifth")
		if last := rec.last(); last != "fifth" {
			t.Fatalf("last line %q, want \"fifth\" with no suppression suffix", last)
		}
	})

	t.Run("zero interval and zero value never throttle", func(t *testing.T) {
		rec := &recordingLogger{}
		for _, l := range []Logging{NewThrottledLogger(0, rec), &ThrottledLogger{next: rec}} {
			before := rec.count()
			for i := range 3 {
				l.Printf(ctx, "always %d", i)
			}
			if got := rec.count() - before; got != 3 {
				t.Fatalf("logged %d line(s) with no interval, want 3", got)
			}
		}
	})

	t.Run("nil sink is the package Logger, read at call time", func(t *testing.T) {
		l := NewThrottledLogger(time.Hour, nil)
		rec := &recordingLogger{}
		prev := Logger.Load()
		Logger.Store(rec) // swapped after construction, like a late SetLogger
		t.Cleanup(func() { Logger.Store(prev) })

		l.Printf(ctx, "via the package logger")
		if got, last := rec.count(), rec.last(); got != 1 || last != "via the package logger" {
			t.Fatalf("logged %d line(s), last %q; want the line through the swapped Logger", got, last)
		}
	})

	t.Run("concurrent callers emit exactly one line per interval", func(t *testing.T) {
		rec := &recordingLogger{}
		l := NewThrottledLogger(time.Hour, rec)
		var wg sync.WaitGroup
		for i := range 16 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				l.Printf(ctx, "racing %d", i)
			}(i)
		}
		wg.Wait()
		if got := rec.count(); got != 1 {
			t.Fatalf("%d lines written by concurrent callers, want 1", got)
		}
	})
}
