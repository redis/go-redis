package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestPipelinePoolMaintNotifications verifies maintnotifications is wired to the
// DEDICATED pipeline pool: the CLIENT MAINT_NOTIFICATIONS handshake runs on
// pipeline-pool conns during initConn and the pipeline read path drains push
// frames, so pipelines and autopipelines keep working with the feature enabled.
// (The manager registers the pipeline pool via InitPoolHookForPool and tracks
// each conn via TrackMaintNotificationsConn.)
func TestPipelinePoolMaintNotifications(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                     ":6379",
		Protocol:                 3, // maintnotifications requires RESP3
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeAuto},
		PipelineReadBufferSize:   64 << 10,
		PipelineWriteBufferSize:  64 << 10,
		PipelinePoolSize:         2,
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	// Manual pipeline on the pipeline pool.
	pipe := c.Pipeline()
	pipe.Set(ctx, "mn:k", "v", 0)
	g := pipe.Get(ctx, "mn:k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline with maintnotifications enabled: %v", err)
	}
	if g.Val() != "v" {
		t.Fatalf("pipeline get = %q, want v", g.Val())
	}
	if st := c.PoolStats(); st.PipelineStats == nil {
		t.Fatal("expected a dedicated pipeline pool (PipelineStats nil)")
	}

	// AutoPipeline (batched) on the pipeline pool.
	ap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 300, MaxFlushDelay: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	s1 := ap.Set(ctx, "mn:ap1", "v1", 0)
	s2 := ap.Incr(ctx, "mn:n")
	if err := s1.Err(); err != nil {
		t.Fatalf("autopipeline set with maintnotifications: %v", err)
	}
	if err := s2.Err(); err != nil {
		t.Fatalf("autopipeline incr with maintnotifications: %v", err)
	}
	if s2.Val() != 1 {
		t.Fatalf("autopipeline incr = %d, want 1", s2.Val())
	}

	c.Del(ctx, "mn:k", "mn:ap1", "mn:n")
}
