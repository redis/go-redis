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
			if got := c.baseClient.pipelinePool != nil; got != tc.want {
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
	if c.baseClient.pipelinePool == nil {
		t.Fatal("no pipeline pool")
	}
	if c.opt.ReadBufferSize != 48*1024 || c.opt.WriteBufferSize != 48*1024 {
		t.Fatalf("regular buffers = %d/%d; want 49152 each",
			c.opt.ReadBufferSize, c.opt.WriteBufferSize)
	}
}
