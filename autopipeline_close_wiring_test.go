package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestClientCloseClosesAutoPipeliner verifies that closing the client also
// closes the shared AutoPipeliner, so its background flusher goroutines don't
// outlive the client. IsClosed() is the direct signal — checking it (rather than
// a downstream command error, which could just reflect the closed pool) proves
// Client.Close stopped the pipeliner itself. Without the wiring in Client.Close
// the autopipeliner would report open after the client is gone.
func TestClientCloseClosesAutoPipeliner(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	c.FlushDB(ctx)

	ap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Set(ctx, "cw", "1", 0).Err(); err != nil {
		t.Fatalf("set via autopipeline before close: %v", err)
	}
	if ap.IsClosed() {
		t.Fatal("autopipeliner reported closed before client.Close")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	if !ap.IsClosed() {
		t.Fatal("client.Close did not close the shared autopipeliner")
	}

	// A second client Close must not panic; like the base client it reports the
	// already-closed error (the autopipeliner part is idempotent and nil here).
	if err := c.Close(); err != nil && err != redis.ErrClosed {
		t.Fatalf("second client close: unexpected error %v", err)
	}
}
