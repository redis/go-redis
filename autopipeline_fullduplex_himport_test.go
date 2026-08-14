package redis

import (
	"context"
	"testing"
)

// TestFullDuplexDivertsHImportOffPipe verifies the full-duplex engine routes a
// managed HIMPORT command off the shared pipe to the normal Process path (#3964).
// The FD writer streams raw commands and never injects the registered HIMPORT
// PREPARE, so an HIMPORT SET riding the pipe can fail "no such fieldset"; diverting
// it through Process lets _process inject the PREPARE from the registry.
//
// The assertion is routing, not end-to-end HIMPORT (which needs Redis 8.10+): a
// diverted command executes on the MAIN pool via process(), while an FD-pipe
// command uses the dedicated pipeline pool and never touches the main pool. So a
// main-pool Hits/Misses bump after a lone HImportSet proves it diverted. The
// command's own result is irrelevant here — it may error on an old server.
func TestFullDuplexDivertsHImportOffPipe(t *testing.T) {
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

	// Warm the main pool so a later reuse registers as a Hit, then snapshot. The
	// FD engine uses the pipeline pool, so nothing but the diverted HImportSet
	// touches the main pool between the two snapshots.
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("warm ping: %v", err)
	}
	before := c.PoolStats()

	// Err() on the async face blocks until the diverted command has executed, so
	// the "after" snapshot reflects its pool use. Ignore the result: it may be
	// "no such fieldset" (registry empty) or "unknown command" (Redis < 8.10) —
	// either way it ran on the main pool, which is what we assert.
	_ = ap.HImportSet(ctx, "fd:himport:k", "fd:himport:fs", "v").Err()

	after := c.PoolStats()
	beforeN := before.Hits + before.Misses
	afterN := after.Hits + after.Misses
	if afterN <= beforeN {
		t.Fatalf("HImportSet did not use the main pool (before=%d after=%d) — it rode the FD pipe instead of diverting to the normal path", beforeN, afterN)
	}
}
