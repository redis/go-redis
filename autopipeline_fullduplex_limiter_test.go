package redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// fdCountLimiter counts Allow/ReportResult to verify the full-duplex engine
// accounts the Limiter once per session (Allow on acquire, ReportResult before
// release), balanced 1:1.
type fdCountLimiter struct{ allow, report atomic.Int64 }

func (l *fdCountLimiter) Allow() error         { l.allow.Add(1); return nil }
func (l *fdCountLimiter) ReportResult(_ error) { l.report.Add(1) }

// TestFullDuplexLimiterPerSession verifies FullDuplex honors opt.Limiter with
// per-session accounting: every session's conn acquisition is bracketed by
// exactly one Allow and one ReportResult (before release). Previously the FD
// engine bypassed the Limiter entirely (#3964).
func TestFullDuplexLimiterPerSession(t *testing.T) {
	ctx := context.Background()
	lim := &fdCountLimiter{}
	c := NewClient(&Options{
		Addr:                    ":6379",
		Protocol:                3,
		PipelinePoolSize:        4,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                4,
		Limiter:                 lim,
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	if err := ap.Set(ctx, "fd:lim:k", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	// A tiny idle window so any late session settles before we read the counters.
	time.Sleep(20 * time.Millisecond)
	// Close waits for the engine (ap.wg), so every session's deferred
	// ReportResult has run by the time Close returns.
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	a, r := lim.allow.Load(), lim.report.Load()
	if a < 1 {
		t.Fatalf("FullDuplex never called Limiter.Allow (allow=%d) — Limiter bypassed", a)
	}
	if a != r {
		t.Fatalf("FullDuplex Limiter unbalanced: Allow=%d ReportResult=%d (must be 1:1 per session)", a, r)
	}
}
