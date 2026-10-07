package internal

import (
	"context"
	"sync"
	"time"
)

// ThrottledLogger rate-limits one recurring log line: Printf emits at
// most one line per interval through Logger and reports whether it did,
// so the caller can reset whatever it accumulates between lines (a drop
// count, an attempt number) exactly when a line goes out. Every emitter
// owns its own instance — the throttle is per source, not per message
// text, so a noisy source never silences another source's first
// warning. Safe for concurrent use. The zero value (and a zero or
// negative interval) logs every call.
type ThrottledLogger struct {
	mu       sync.Mutex
	interval time.Duration
	last     time.Time
}

// NewThrottledLogger returns a logger that emits at most one line per
// interval; its first Printf always logs.
func NewThrottledLogger(interval time.Duration) *ThrottledLogger {
	return &ThrottledLogger{interval: interval}
}

// Printf logs format and args through Logger unless a line went out
// less than the interval ago, reporting whether it logged.
func (l *ThrottledLogger) Printf(ctx context.Context, format string, args ...any) bool {
	now := time.Now()
	l.mu.Lock()
	if now.Sub(l.last) < l.interval {
		l.mu.Unlock()
		return false
	}
	l.last = now
	l.mu.Unlock()

	Logger.Printf(ctx, format, args...)
	return true
}

// Reset forgets the last emission so the next Printf logs immediately:
// for when the reported condition has cleared (e.g. a reconnect
// succeeded) and a recurrence deserves a fresh first line.
func (l *ThrottledLogger) Reset() {
	l.mu.Lock()
	l.last = time.Time{}
	l.mu.Unlock()
}
