package redis

import "testing"

// TestAutoPipelineCapturesPipelinePool guards the regression that motivated the
// getter: the straggler-hold pool gate reaches the pipeline pool through an
// in-package interface assertion (`getPipelinePool() pool.Pooler`) in
// newAutoPipeliner. Without the getter that assertion fails silently,
// ap.pipelinePool stays nil, pipelineHasFreeConn() always returns false, and the
// gate degrades to a no-op — compiling and passing every functional test while
// doing nothing. This asserts the pool is actually captured and the gate is live.
func TestAutoPipelineCapturesPipelinePool(t *testing.T) {
	// Buffer sizes trigger creation of the dedicated pipeline pool.
	c := NewClient(&Options{
		Addr:                    ":6379",
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PipelinePoolSize:        4,
	})
	defer c.Close()

	if c.getPipelinePool() == nil {
		t.Fatal("getPipelinePool() returned nil for a client configured with a pipeline pool")
	}
	if c.getPipelinePool() != c.pipelinePool.pool {
		t.Fatal("getPipelinePool() did not return the client's pipeline pool")
	}

	ap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	if ap.pipelinePool == nil {
		t.Fatal("AutoPipeliner captured a nil pipeline pool: the getPipelinePool assertion failed, " +
			"so the straggler-hold gate is inert")
	}
	// Fresh, un-dialed pool: no idle conns yet, but room to dial (Len < Size),
	// so the gate should see spare capacity.
	if !ap.pipelineHasFreeConn() {
		t.Fatal("pipelineHasFreeConn() should be true for a fresh dial-able pipeline pool")
	}
}

// TestAutoPipelineNilPipelinePoolConservative verifies the safe fallback: a
// client with no pipeline pool captures nil and the gate stays conservative
// (pipelineHasFreeConn() == false → the original long hold is kept).
//
// The pipeline pool is now always-on (PipelinePoolSize >= 0), so the nil path
// is reached via the explicit opt-out, PipelinePoolSize: -1.
func TestAutoPipelineNilPipelinePoolConservative(t *testing.T) {
	c := NewClient(&Options{Addr: ":6379", PipelinePoolSize: -1}) // opt out => no pipeline pool
	defer c.Close()

	if c.getPipelinePool() != nil {
		t.Fatal("expected a nil pipeline pool when none is configured")
	}

	ap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	if ap.pipelinePool != nil {
		t.Fatal("expected AutoPipeliner to capture a nil pipeline pool")
	}
	if ap.pipelineHasFreeConn() {
		t.Fatal("pipelineHasFreeConn() must be false when there is no pipeline pool")
	}
}
