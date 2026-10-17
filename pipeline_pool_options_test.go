package redis

import (
	"testing"
	"time"
)

// TestPipelinePoolOptionsCapsAndTimeout pins the pipeline-pool clone semantics:
// it does NOT inherit MaxActiveConns (per-pool cap, so the client's total socket
// ceiling stays non-additive) and uses the short fast-spill PoolTimeout instead
// of the main pool's (which the clone would otherwise inherit).
func TestPipelinePoolOptionsCapsAndTimeout(t *testing.T) {
	opt := &Options{
		Addr:           ":6379",
		PoolSize:       20,
		MaxActiveConns: 100,             // must NOT be inherited
		PoolTimeout:    9 * time.Second, // must NOT be inherited
	}
	opt.init()

	po := pipelinePoolOptions(opt)
	if po.MaxActiveConns != 0 {
		t.Errorf("pipeline MaxActiveConns = %d, want 0 (per-pool cap, not additive with the main pool)", po.MaxActiveConns)
	}
	if po.PoolTimeout != DefaultPipelinePoolTimeout {
		t.Errorf("pipeline PoolTimeout = %v, want %v (fast spill)", po.PoolTimeout, DefaultPipelinePoolTimeout)
	}
	if po.MinIdleConns != 0 {
		t.Errorf("pipeline MinIdleConns = %d, want 0 (burst capacity, no pre-dial)", po.MinIdleConns)
	}
	if po.PoolSize != DefaultPipelinePoolSize {
		t.Errorf("pipeline PoolSize = %d, want %d (default when PipelinePoolSize unset)", po.PoolSize, DefaultPipelinePoolSize)
	}
}

// TestUniversalOptionsPipelinePassthrough guards that the pipeline-pool knobs
// reach every client shape through UniversalOptions (they were Options/Cluster/
// Failover/Ring-only, so UniversalClient users could neither size nor opt out of
// the now-always-created pipeline pool). PipelinePoolSize:-1 (opt-out) is used as
// a distinctive value.
func TestUniversalOptionsPipelinePassthrough(t *testing.T) {
	u := &UniversalOptions{
		PipelineReadBufferSize:  11,
		PipelineWriteBufferSize: 22,
		PipelinePoolSize:        -1,
	}
	check := func(name string, rbs, wbs, ps int) {
		if rbs != 11 || wbs != 22 || ps != -1 {
			t.Errorf("%s dropped pipeline fields: read=%d write=%d pool=%d, want 11/22/-1", name, rbs, wbs, ps)
		}
	}
	s := u.Simple()
	check("Simple()", s.PipelineReadBufferSize, s.PipelineWriteBufferSize, s.PipelinePoolSize)
	c := u.Cluster()
	check("Cluster()", c.PipelineReadBufferSize, c.PipelineWriteBufferSize, c.PipelinePoolSize)
	f := u.Failover()
	check("Failover()", f.PipelineReadBufferSize, f.PipelineWriteBufferSize, f.PipelinePoolSize)
}
