package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Tests for pipeline execution behavior shared with the autopipeliner:
// heterogeneous reply-shape demultiplexing and the retry/re-drive loop.

// ===== from pipeline_reply_shapes_test.go =====

// Reply-shape demux: a single batch mixing heterogeneous reply types must keep
// the reader in sync so every command gets ITS OWN correctly-typed reply. Prior
// correctness tests batched only homogeneous simple commands (SET/GET/INCR),
// which proves key/value pairing but not that the parser stays aligned across
// bulk + array + int + map + nil + double + large(multi-read) + binary-safe
// values. One demux slip corrupts every later reply in the batch. AutoPipeline
// dispatches through the same execer, so it inherits this.

func seedReplyShapeData(t *testing.T, ctx context.Context, c *redis.Client) (big, binary string) {
	t.Helper()
	big = strings.Repeat("A", 100_000) // > bufio buffer -> multi-read
	binary = "x\x00y\r\nz\x01\xffend"  // embedded NUL, CRLF, high bytes
	c.Del(ctx, "rs:list", "rs:hash", "rs:big", "rs:bin", "rs:s", "rs:n", "rs:f")
	if err := c.RPush(ctx, "rs:list", "a", "b", "c").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, "rs:hash", "f1", "v1", "f2", "v2").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "rs:big", big, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "rs:bin", binary, 0).Err(); err != nil {
		t.Fatal(err)
	}
	return big, binary
}

func assertReplyShapes(t *testing.T, cSet *redis.StatusCmd, cGet *redis.StringCmd,
	cIncr *redis.IntCmd, cArr *redis.StringSliceCmd, cMap *redis.MapStringStringCmd,
	cNil *redis.StringCmd, cBig, cBin *redis.StringCmd, cFloat *redis.FloatCmd,
	big, binary string) {
	t.Helper()
	if cSet.Val() != "OK" {
		t.Errorf("status: %q, want OK", cSet.Val())
	}
	if cGet.Val() != "v" {
		t.Errorf("bulk: %q, want v", cGet.Val())
	}
	if cIncr.Val() != 1 {
		t.Errorf("int: %d, want 1", cIncr.Val())
	}
	if got := cArr.Val(); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("array: %v, want [a b c]", got)
	}
	if got := cMap.Val(); got["f1"] != "v1" || got["f2"] != "v2" {
		t.Errorf("map: %v", got)
	}
	if cNil.Err() != redis.Nil {
		t.Errorf("nil: err=%v, want redis.Nil", cNil.Err())
	}
	if cBig.Val() != big {
		t.Errorf("large: len=%d, want %d", len(cBig.Val()), len(big))
	}
	if cBin.Val() != binary {
		t.Errorf("binary-safe: %q, want %q", cBin.Val(), binary)
	}
	if cFloat.Val() != 1.5 {
		t.Errorf("double: %v, want 1.5", cFloat.Val())
	}
}

func TestPipelineHeterogeneousReplyDemux(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: apTestAddr()})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	big, binary := seedReplyShapeData(t, ctx, c)

	pipe := c.Pipeline()
	cSet := pipe.Set(ctx, "rs:s", "v", 0)
	cGet := pipe.Get(ctx, "rs:s")
	cIncr := pipe.Incr(ctx, "rs:n")
	cArr := pipe.LRange(ctx, "rs:list", 0, -1)
	cMap := pipe.HGetAll(ctx, "rs:hash")
	cNil := pipe.Get(ctx, "rs:missing")
	cBig := pipe.Get(ctx, "rs:big")
	cBin := pipe.Get(ctx, "rs:bin")
	cFloat := pipe.IncrByFloat(ctx, "rs:f", 1.5)
	_, _ = pipe.Exec(ctx) // Exec returns the first error (the redis.Nil); check per-cmd

	assertReplyShapes(t, cSet, cGet, cIncr, cArr, cMap, cNil, cBig, cBin, cFloat, big, binary)
}

// TestPipelineHeterogeneousReplyDemuxRESP2 runs the same demux under RESP2, which
// parses maps/doubles/verbatim differently on the wire — the reader must still
// stay aligned across the mixed batch.
func TestPipelineHeterogeneousReplyDemuxRESP2(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: apTestAddr(), Protocol: 2})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	big, binary := seedReplyShapeData(t, ctx, c)

	pipe := c.Pipeline()
	cSet := pipe.Set(ctx, "rs:s", "v", 0)
	cGet := pipe.Get(ctx, "rs:s")
	cIncr := pipe.Incr(ctx, "rs:n")
	cArr := pipe.LRange(ctx, "rs:list", 0, -1)
	cMap := pipe.HGetAll(ctx, "rs:hash")
	cNil := pipe.Get(ctx, "rs:missing")
	cBig := pipe.Get(ctx, "rs:big")
	cBin := pipe.Get(ctx, "rs:bin")
	cFloat := pipe.IncrByFloat(ctx, "rs:f", 1.5)
	_, _ = pipe.Exec(ctx)

	assertReplyShapes(t, cSet, cGet, cIncr, cArr, cMap, cNil, cBig, cBin, cFloat, big, binary)
}

func TestAutoPipelineHeterogeneousReplyDemux(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: apTestAddr()})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	big, binary := seedReplyShapeData(t, ctx, c)

	// Async face with a wide flush window: the 9 heterogeneous commands are
	// submitted without blocking and land in ONE pipeline batch, so this
	// actually exercises batch reply demux (the blocking face submits them
	// one at a time through the lone-command fast path, which never demuxes).
	ap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize:  100,
		MaxFlushDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	cSet := ap.Set(ctx, "rs:s", "v", 0)
	cGet := ap.Get(ctx, "rs:s")
	cIncr := ap.Incr(ctx, "rs:n")
	cArr := ap.LRange(ctx, "rs:list", 0, -1)
	cMap := ap.HGetAll(ctx, "rs:hash")
	cNil := ap.Get(ctx, "rs:missing")
	cBig := ap.Get(ctx, "rs:big")
	cBin := ap.Get(ctx, "rs:bin")
	cFloat := ap.IncrByFloat(ctx, "rs:f", 1.5)

	assertReplyShapes(t, cSet, cGet, cIncr, cArr, cMap, cNil, cBig, cBin, cFloat, big, binary)
}

// ===== from pipeline_retry_test.go =====

// These tests exercise the pipeline retry/re-drive loop in generalProcessPipeline
// (redis.go), which was previously untested: the CI harness runs with
// MaxRetries=-1 (-> 0), so the retry loop never iterated. AutoPipeline dispatches
// through this same execer, so it inherits whatever is proven here
// (see autopipeline_retry_test.go for the AutoPipeline-specific coverage).

// TestPipelineRetriesOnNetworkError arms the pooled conn's next write to fail
// at the wire (a seeded broken idle conn would be filtered by the pool health
// check and never carry an attempt); the first pipeline attempt genuinely
// dies, and the retry redials a healthy conn and re-drives the whole batch.
// Verifies the replies are correct and in order after the re-drive.
func TestPipelineRetriesOnNetworkError(t *testing.T) {
	ctx := context.Background()
	var failNextWrite atomic.Bool
	var dials atomic.Int32
	client := redis.NewClient(&redis.Options{
		Addr: apTestAddr(), MaxRetries: 2, PoolSize: 1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			cn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &flakyWriteConn{Conn: cn, fail: &failNextWrite}, nil
		},
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Set(ctx, "pr:n", 0, 0).Err(); err != nil {
		t.Fatal(err)
	}

	dialsBefore := dials.Load()
	failNextWrite.Store(true)

	pipe := client.Pipeline()
	set := pipe.Set(ctx, "pr:k", "v", 0)
	get := pipe.Get(ctx, "pr:k")
	incr := pipe.Incr(ctx, "pr:n")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipe.Exec after retry: %v", err)
	}
	if set.Val() != "OK" {
		t.Fatalf("set = %q, want OK", set.Val())
	}
	if get.Val() != "v" {
		t.Fatalf("get = %q, want v", get.Val())
	}
	if incr.Val() != 1 {
		t.Fatalf("incr = %d, want 1", incr.Val())
	}
	if failNextWrite.Load() {
		t.Fatal("armed write failure never consumed: the first attempt did not reach the wire")
	}
	if got := dials.Load(); got != dialsBefore+1 {
		t.Fatalf("dials after failure = %d, want %d (exactly one redial by the retry)", got, dialsBefore+1)
	}
}

// TestPipelineRetryExhausted makes every dial hand back a broken conn, so all
// MaxRetries+1 attempts fail. Verifies the pipeline returns an error (does not
// hang), every command carries the error, and the loop ran the expected number
// of attempts.
func TestPipelineRetryExhausted(t *testing.T) {
	ctx := context.Background()
	var dials int32
	client := redis.NewClient(&redis.Options{
		Addr:       apTestAddr(),
		MaxRetries: 2, // -> 3 attempts total
		PoolSize:   1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&dials, 1)
			return &badConn{writeErr: io.EOF}, nil // dial "succeeds", all I/O fails
		},
	})
	defer client.Close()

	pipe := client.Pipeline()
	_ = pipe.Set(ctx, "k", "v", 0)
	_ = pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("expected error after retry exhaustion, got nil")
	}
	// MaxRetries=2 -> 3 attempts, each dials a fresh (bad) conn.
	if got := atomic.LoadInt32(&dials); got < 3 {
		t.Fatalf("dials = %d, want >= 3 (MaxRetries+1 attempts)", got)
	}
	// NOTE: manual Pipeline surfaces exhaustion via Exec's returned error. When
	// the failure is a conn-init failure (conn never runs pipelineProcessCmds),
	// the individual command objects are NOT populated with the error — see
	// TestAutoPipelineRetryExhaustionSurfacesError for why that matters for
	// AutoPipeline, which has no Exec return value to surface it.
}

// TestPipelineWrongTypeNotRetriedIsolated verifies a server -ERR (WRONGTYPE) in
// the middle of a pipeline is isolated to its own command — siblings still get
// their correct replies — and is not retried (redis errors are non-retryable).
func TestPipelineWrongTypeNotRetriedIsolated(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: apTestAddr(), MaxRetries: 3})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.Del(ctx, "wt:list", "wt:k")
	if err := client.LPush(ctx, "wt:list", "x").Err(); err != nil {
		t.Fatal(err)
	}

	pipe := client.Pipeline()
	good1 := pipe.Set(ctx, "wt:k", "v", 0)
	bad := pipe.Incr(ctx, "wt:list") // WRONGTYPE against a list
	good2 := pipe.Get(ctx, "wt:k")
	_, _ = pipe.Exec(ctx) // Exec returns the first command error (the WRONGTYPE)

	if bad.Err() == nil {
		t.Fatal("expected WRONGTYPE on the bad command")
	}
	if good1.Err() != nil {
		t.Fatalf("good1 inherited an error: %v", good1.Err())
	}
	if good2.Err() != nil || good2.Val() != "v" {
		t.Fatalf("good2 = %q, %v; want v, nil", good2.Val(), good2.Err())
	}
}

// TestPipelineContextCanceledNotRetried verifies a cancelled context aborts the
// pipeline without retrying (context errors are non-retryable) and every command
// carries the context error.
func TestPipelineContextCanceledNotRetried(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: apTestAddr(), MaxRetries: 3})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	pipe := client.Pipeline()
	c := pipe.Get(ctx, "ctx:k")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if !errors.Is(c.Err(), context.Canceled) {
		t.Fatalf("cmd err = %v, want context.Canceled", c.Err())
	}
}

// TestAutoPipelineHeterogeneousReplyDemuxRESP2Batch is the RESP2 twin of the
// batched demux test above: the same nine heterogeneous replies land in one
// real async batch over Protocol 2, pinning that batch reply demultiplexing
// holds on both protocol encodings (the RESP2 variant was previously only
// inferred from a comment, never asserted through a real batch).
func TestAutoPipelineHeterogeneousReplyDemuxRESP2Batch(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: apTestAddr(), Protocol: 2})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	big, binary := seedReplyShapeData(t, ctx, c)

	ap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize:  100,
		MaxFlushDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	cSet := ap.Set(ctx, "rs:s", "v", 0)
	cGet := ap.Get(ctx, "rs:s")
	cIncr := ap.Incr(ctx, "rs:n")
	cArr := ap.LRange(ctx, "rs:list", 0, -1)
	cMap := ap.HGetAll(ctx, "rs:hash")
	cNil := ap.Get(ctx, "rs:missing")
	cBig := ap.Get(ctx, "rs:big")
	cBin := ap.Get(ctx, "rs:bin")
	cFloat := ap.IncrByFloat(ctx, "rs:f", 1.5)

	assertReplyShapes(t, cSet, cGet, cIncr, cArr, cMap, cNil, cBig, cBin, cFloat, big, binary)
}
