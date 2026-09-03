package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

type spillCountingLimiter struct {
	allow  atomic.Int64
	report atomic.Int64
}

func (l *spillCountingLimiter) Allow() error         { l.allow.Add(1); return nil }
func (l *spillCountingLimiter) ReportResult(_ error) { l.report.Add(1) }

// TestWithPipelineConnSpillAccountsLimiterOnce drives the spill branch that the
// #3959 fix restructured: with the pipeline pool's only connection held, a
// pipeline must spill to the main pool AND account the Limiter exactly once. The
// pre-fix code re-entered withConn on spill, calling Allow()/ReportResult() a
// second time (2/2). Deterministic: the pipeline pool is genuinely saturated
// (its one conn is held), so Get times out after DefaultPipelinePoolTimeout and
// spills — no timing race.
func TestWithPipelineConnSpillAccountsLimiterOnce(t *testing.T) {
	ctx := context.Background()
	lim := &spillCountingLimiter{}
	c := NewClient(&Options{Addr: ":6379", PipelinePoolSize: 1, Limiter: lim})
	defer c.Close()
	if err := probeRedis(":6379"); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	ref := c.loadPipelinePool()
	if ref == nil {
		t.Fatal("expected a dedicated pipeline pool")
	}

	// Hold the pipeline pool's only connection so the next pipeline Get times out
	// and withPipelineConn is forced to spill to the main pool.
	held, err := ref.pool.Get(ctx)
	if err != nil {
		t.Fatalf("hold pipeline conn: %v", err)
	}
	defer ref.pool.Remove(ctx, held, errors.New("test cleanup"))

	// Measure only the spill call (the Ping above used the main pool).
	lim.allow.Store(0)
	lim.report.Store(0)

	var ranOn string
	err = c.withPipelineConn(ctx, func(_ context.Context, cn *pool.Conn) error {
		ranOn = cn.PoolName()
		return nil
	})
	if err != nil {
		t.Fatalf("withPipelineConn (expected spill success): %v", err)
	}
	if ranOn == ref.name {
		t.Fatalf("expected the spill to run on the MAIN pool, but it ran on the pipeline pool (%q)", ranOn)
	}
	if a, r := lim.allow.Load(), lim.report.Load(); a != 1 || r != 1 {
		t.Fatalf("spill Limiter accounting: Allow=%d ReportResult=%d, want 1/1 (double-count regression)", a, r)
	}
}
