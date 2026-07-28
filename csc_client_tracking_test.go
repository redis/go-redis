package redis

import (
	"context"
	"errors"
	"testing"
)

// TestIsClientTrackingCmd pins the guard's matcher: any CLIENT TRACKING
// subcommand matches, other CLIENT subcommands (incl. TRACKINGINFO) do not.
func TestIsClientTrackingCmd(t *testing.T) {
	matching := []Cmder{
		makeCmd("client", "tracking", "on"),
		makeCmd("client", "tracking", "off"),
		makeCmd("CLIENT", "TRACKING", "on", "bcast"),
		makeCmd("Client", "Tracking"),
		makeCmd([]byte("client"), []byte("tracking"), "off"), // raw []byte args
	}
	for _, cmd := range matching {
		if !isClientTrackingCmd(cmd) {
			t.Errorf("expected %v to match CLIENT TRACKING", cmd.Args())
		}
	}
	nonMatching := []Cmder{
		makeCmd("client", "trackinginfo"),
		makeCmd("client", "info"),
		makeCmd("client", "kill", "id", "1"),
		makeCmd("get", "tracking"),
		makeCmd("client"),
	}
	for _, cmd := range nonMatching {
		if isClientTrackingCmd(cmd) {
			t.Errorf("expected %v NOT to match CLIENT TRACKING", cmd.Args())
		}
	}
}

// TestClientTrackingRejectedWithCSC: on a client with the built-in cache
// configured, CLIENT TRACKING must be rejected before it reaches a connection —
// it would flip an arbitrary pool conn's tracking state and leave it filling
// the cache with entries the server never invalidates. The guard fires without
// dialing, so no server is needed.
func TestClientTrackingRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.ClientTrackingOff(ctx).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}
	if err := c.ClientTrackingOn(ctx, nil).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}
	// The raw escape hatch is caught too: the guard matches leading args.
	// (Non-tracking CLIENT subcommands are covered by TestIsClientTrackingCmd's
	// non-matching cases — probing one here would dial for seconds.)
	if err := c.Do(ctx, "client", "tracking", "off").Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("raw Do(client tracking off) must be rejected with CSC enabled, got %v", err)
	}
}

// TestClientTrackingRejectedWithCSC_Pipeline: pipelines bypass process(), so
// generalProcessPipeline mirrors the guard — a CLIENT TRACKING frame inside a
// Pipeline or TxPipeline must be rejected on a CSC client too.
func TestClientTrackingRejectedWithCSC_Pipeline(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOff(ctx)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("Pipelined ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}

	_, err = c.TxPipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOn(ctx, nil)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("TxPipelined ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}
}

// TestClientTrackingAllowedWithoutCSC: without the built-in cache the guard
// predicate is off entirely (asserted directly — a live dial would prove
// nothing more and costs seconds against an unreachable address).
func TestClientTrackingAllowedWithoutCSC(t *testing.T) {
	c := NewClient(&Options{Addr: "localhost:1", Protocol: 3})
	t.Cleanup(func() { _ = c.Close() })

	if c.baseClient.cscRejectsClientTracking() {
		t.Fatal("a client without CSC must not reject CLIENT TRACKING")
	}
}
