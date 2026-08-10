package redis

import "testing"

// TestPipelinePoolSizeAloneCreatesThePool covers a silent no-op.
//
// The dedicated pipeline pool was created only when a pipeline BUFFER size was set, so
// Options{PipelinePoolSize: 8} produced no pipeline pool and no diagnostic: every
// pipelined operation quietly kept using the main pool, competing with ordinary
// commands for the same connections. The gating was backwards — the pool SIZE is the
// primary intent while the buffer sizes are tuning, and the buffer sizes already
// supplied a default for the size.
func TestPipelinePoolSizeAloneCreatesThePool(t *testing.T) {
	cases := []struct {
		name string
		opt  *Options
		want bool
	}{
		{
			name: "pool size alone is enough",
			opt:  &Options{Addr: "127.0.0.1:0", PipelinePoolSize: 8},
			want: true,
		},
		{
			name: "read buffer alone still works",
			opt:  &Options{Addr: "127.0.0.1:0", PipelineReadBufferSize: 64 * 1024},
			want: true,
		},
		{
			name: "write buffer alone still works",
			opt:  &Options{Addr: "127.0.0.1:0", PipelineWriteBufferSize: 64 * 1024},
			want: true,
		},
		{
			name: "none set means no dedicated pool",
			opt:  &Options{Addr: "127.0.0.1:0"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(tc.opt)
			defer c.Close()
			if got := c.baseClient.loadPipelinePool() != nil; got != tc.want {
				t.Fatalf("pipelinePool present = %v, want %v. A pipeline option that is "+
					"silently ignored sends every pipelined operation to the main pool",
					got, tc.want)
			}
		})
	}
}

// TestPipelinePoolSizeAloneInheritsBuffers: enabling by size must inherit the regular
// buffer sizes rather than build the pool with unusable ones.
func TestPipelinePoolSizeAloneInheritsBuffers(t *testing.T) {
	c := NewClient(&Options{
		Addr:             "127.0.0.1:0",
		ReadBufferSize:   48 * 1024,
		WriteBufferSize:  48 * 1024,
		PipelinePoolSize: 4,
	})
	defer c.Close()
	if c.baseClient.loadPipelinePool() == nil {
		t.Fatal("no pipeline pool")
	}
	if c.opt.ReadBufferSize != 48*1024 || c.opt.WriteBufferSize != 48*1024 {
		t.Fatalf("regular buffers = %d/%d; want 49152 each",
			c.opt.ReadBufferSize, c.opt.WriteBufferSize)
	}
}

// TestAutoPipelineOptionsDefaultsThePipelinePool: configuring the autopipeliner on the
// client declares pipeline-heavy usage, so the dedicated pipeline pool defaults in —
// otherwise every autopipeline batch competes with regular commands for main-pool
// connections and the whole point of a bounded pipeline pool is lost unless the user
// discovers a second option. Explicit sizes win; a negative size opts out; a plain
// client without AutoPipelineOptions is untouched.
func TestAutoPipelineOptionsDefaultsThePipelinePool(t *testing.T) {
	cases := []struct {
		name     string
		opt      *Options
		wantPool bool
		wantSize int // asserted on opt.PipelinePoolSize after init; 0 = skip
	}{
		{
			name:     "AutoPipelineOptions alone defaults the pool in",
			opt:      &Options{Addr: "127.0.0.1:0", AutoPipelineOptions: &AutoPipelineOptions{}},
			wantPool: true,
			wantSize: DefaultPipelinePoolSize,
		},
		{
			name: "explicit size wins over the default",
			opt: &Options{Addr: "127.0.0.1:0", AutoPipelineOptions: &AutoPipelineOptions{},
				PipelinePoolSize: 4},
			wantPool: true,
			wantSize: 4,
		},
		{
			name: "negative size opts out of the dedicated pool",
			opt: &Options{Addr: "127.0.0.1:0", AutoPipelineOptions: &AutoPipelineOptions{},
				PipelinePoolSize: -1},
			wantPool: false,
		},
		{
			name:     "no AutoPipelineOptions: plain clients are untouched",
			opt:      &Options{Addr: "127.0.0.1:0"},
			wantPool: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(tc.opt)
			defer c.Close()
			if got := c.baseClient.loadPipelinePool() != nil; got != tc.wantPool {
				t.Fatalf("pipelinePool present = %v, want %v", got, tc.wantPool)
			}
			if tc.wantSize != 0 && c.opt.PipelinePoolSize != tc.wantSize {
				t.Fatalf("PipelinePoolSize = %d, want %d", c.opt.PipelinePoolSize, tc.wantSize)
			}
		})
	}
}

// TestClusterAutoPipelineOptionsDefaultsNodePipelinePools: node clients never see
// ClusterOptions.AutoPipelineOptions (the cluster autopipeliner lives at cluster level
// and dispatches into the node clients' pipeline hooks), so the Options.init default
// cannot fire on them; clientOptions must apply it. Without this, a cluster configured
// for autopipelining would batch through every node's MAIN pool.
func TestClusterAutoPipelineOptionsDefaultsNodePipelinePools(t *testing.T) {
	base := func(mut func(*ClusterOptions)) *Options {
		co := &ClusterOptions{Addrs: []string{"127.0.0.1:0"}}
		if mut != nil {
			mut(co)
		}
		co.init()
		return co.clientOptions()
	}

	if got := base(func(co *ClusterOptions) {
		co.AutoPipelineOptions = &AutoPipelineOptions{}
	}).PipelinePoolSize; got != DefaultPipelinePoolSize {
		t.Fatalf("node PipelinePoolSize = %d, want %d", got, DefaultPipelinePoolSize)
	}
	if got := base(func(co *ClusterOptions) {
		co.AutoPipelineOptions = &AutoPipelineOptions{}
		co.PipelinePoolSize = 4
	}).PipelinePoolSize; got != 4 {
		t.Fatalf("node PipelinePoolSize = %d, want 4 (explicit wins)", got)
	}
	if got := base(func(co *ClusterOptions) {
		co.AutoPipelineOptions = &AutoPipelineOptions{}
		co.PipelinePoolSize = -1
	}).PipelinePoolSize; got != -1 {
		t.Fatalf("node PipelinePoolSize = %d, want -1 (opt-out preserved)", got)
	}
	if got := base(nil).PipelinePoolSize; got != 0 {
		t.Fatalf("node PipelinePoolSize = %d, want 0 (no autopipelining declared)", got)
	}
}

// TestAutoPipelineCreatesThePipelinePoolLazily: the config-time default only covers
// clients whose Options declared AutoPipelineOptions. The common pattern configures the
// autopipeliner at runtime — AutoPipelineWithOptions(cfg) on a plain client — where
// pool creation has already happened, so the pool must be created lazily when the
// autopipeliner is built. Without this, every batch of a runtime-configured
// autopipeliner competes with regular commands for main-pool connections.
func TestAutoPipelineCreatesThePipelinePoolLazily(t *testing.T) {
	t.Run("blocking face", func(t *testing.T) {
		c := NewClient(&Options{Addr: "127.0.0.1:0"})
		defer c.Close()
		if c.baseClient.loadPipelinePool() != nil {
			t.Fatal("plain client must not have a pipeline pool before AutoPipeline")
		}
		if _, err := c.AutoPipeline(); err != nil {
			t.Fatalf("AutoPipeline: %v", err)
		}
		if c.baseClient.loadPipelinePool() == nil {
			t.Fatal("AutoPipeline must create the dedicated pipeline pool lazily")
		}
	})

	t.Run("async face", func(t *testing.T) {
		c := NewClient(&Options{Addr: "127.0.0.1:0"})
		defer c.Close()
		if _, err := c.AsyncAutoPipeline(); err != nil {
			t.Fatalf("AsyncAutoPipeline: %v", err)
		}
		if c.baseClient.loadPipelinePool() == nil {
			t.Fatal("AsyncAutoPipeline must create the dedicated pipeline pool lazily")
		}
	})

	t.Run("negative size opts out of lazy creation too", func(t *testing.T) {
		c := NewClient(&Options{Addr: "127.0.0.1:0", PipelinePoolSize: -1})
		defer c.Close()
		if _, err := c.AutoPipeline(); err != nil {
			t.Fatalf("AutoPipeline: %v", err)
		}
		if c.baseClient.loadPipelinePool() != nil {
			t.Fatal("PipelinePoolSize < 0 is the documented opt-out; lazy creation must respect it")
		}
	})

	t.Run("WithTimeout clone shares the lazily created pool", func(t *testing.T) {
		c := NewClient(&Options{Addr: "127.0.0.1:0"})
		defer c.Close()
		clone := c.WithTimeout(0)
		if _, err := c.AutoPipeline(); err != nil {
			t.Fatalf("AutoPipeline: %v", err)
		}
		parent := c.baseClient.loadPipelinePool()
		if parent == nil {
			t.Fatal("no pool on parent")
		}
		// The clone shares the slot, so it must observe the SAME pool: two
		// sharers must never hold two different pipeline pools over one shared
		// pool set.
		if got := clone.baseClient.loadPipelinePool(); got != parent {
			t.Fatalf("clone sees pool %p, parent created %p", got, parent)
		}
	})

	t.Run("existing pool is reused, not replaced", func(t *testing.T) {
		c := NewClient(&Options{Addr: "127.0.0.1:0", PipelinePoolSize: 4})
		defer c.Close()
		before := c.baseClient.loadPipelinePool()
		if before == nil {
			t.Fatal("explicit PipelinePoolSize must create the pool at NewClient")
		}
		if _, err := c.AutoPipeline(); err != nil {
			t.Fatalf("AutoPipeline: %v", err)
		}
		if after := c.baseClient.loadPipelinePool(); after != before {
			t.Fatal("ensurePipelinePool must not replace an existing pool")
		}
	})
}

// TestFailoverPipelinePoolSizeAloneCreatesThePool: NewFailoverClient duplicated the
// pool-creation block and kept the pre-fix gate, so PipelinePoolSize alone silently
// created no pipeline pool there even after the gate fix landed for NewClient.
func TestFailoverPipelinePoolSizeAloneCreatesThePool(t *testing.T) {
	c := NewFailoverClient(&FailoverOptions{
		MasterName:       "mymaster",
		SentinelAddrs:    []string{"127.0.0.1:0"},
		PipelinePoolSize: 8,
	})
	defer c.Close()
	if c.baseClient.loadPipelinePool() == nil {
		t.Fatal("PipelinePoolSize alone must create the pipeline pool on failover clients too")
	}
}
