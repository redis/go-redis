package redis

import (
	"context"
	"testing"
)

// Two clients built from the same ClusterOptions must get the same settings:
// values the caller disabled with a negative number stay disabled for the
// second client.
func TestClusterOptionsReuseKeepsDisabledValues(t *testing.T) {
	const addr = "127.0.0.1:1"
	opt := &ClusterOptions{
		Addrs:           []string{addr},
		MaxRedirects:    -1,
		ReadTimeout:     -1,
		WriteTimeout:    -1,
		MinRetryBackoff: -1,
		MaxRetryBackoff: -1,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
	}

	first := NewClusterClient(opt)
	defer first.Close()
	second := NewClusterClient(opt)
	defer second.Close()

	for i, c := range []*ClusterClient{first, second} {
		o := c.opt
		if o.MaxRedirects != 0 || o.ReadTimeout != 0 || o.WriteTimeout != 0 ||
			o.MinRetryBackoff != 0 || o.MaxRetryBackoff != 0 {
			t.Errorf("client %d: MaxRedirects=%d ReadTimeout=%v WriteTimeout=%v MinRetryBackoff=%v MaxRetryBackoff=%v, want all 0",
				i+1, o.MaxRedirects, o.ReadTimeout, o.WriteTimeout, o.MinRetryBackoff, o.MaxRetryBackoff)
		}

		node, err := c.nodes.GetOrCreate(addr)
		if err != nil {
			t.Fatal(err)
		}
		if got := node.Client.opt.ReadTimeout; got != 0 {
			t.Errorf("client %d: node ReadTimeout = %v, want 0", i+1, got)
		}
	}

	// Defaults that init fills in stay visible to the caller.
	if opt.FailingTimeoutSeconds != 15 {
		t.Errorf("FailingTimeoutSeconds = %d, want 15", opt.FailingTimeoutSeconds)
	}
}
