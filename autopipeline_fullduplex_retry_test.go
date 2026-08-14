package redis

import (
	"context"
	"testing"
	"time"
)

// TestFullDuplexRetryDivertsToNormalConn verifies the mechanism the FD reader
// uses for a retryable Redis error (LOADING/READONLY/…) or a redirect (MOVED/ASK):
// the command is re-run on the client's normal path and the caller is settled with
// that result — not left with the FD error. It drives retryOnNormalConn directly
// (a deterministic stand-in for the reader's divert) since inducing a real LOADING
// on a live server is not reproducible.
func TestFullDuplexRetryDivertsToNormalConn(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	if err := c.Set(ctx, "fd:retry:k", "v", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// As the reader does on a retryable error: hand an FD request to the divert.
	cmd := NewStringCmd(ctx, "get", "fd:retry:k")
	b := newAPBatch()
	cmd.setReady(b)
	ap.fd.retryOnNormalConn(fdReq{cmd: cmd, batch: b})

	select {
	case <-b.done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryOnNormalConn did not complete the diverted command")
	}
	if v, err := cmd.Result(); err != nil || v != "v" {
		t.Fatalf("diverted GET = %q err=%v, want \"v\" (re-run on the normal path)", v, err)
	}
}
