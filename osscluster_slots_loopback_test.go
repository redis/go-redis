package redis

import (
	"context"
	"testing"
)

// Loopback node addresses are rewritten to the host of the node that returned
// CLUSTER SLOTS. ClusterOptions.ClusterSlots has no such node, so the addresses
// it returns must be used as they are.
func TestClusterSlotsFuncKeepsLoopbackAddrs(t *testing.T) {
	addrs := []string{"127.0.0.1:7000", "[::1]:7001"}
	c := NewClusterClient(&ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{
				{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: addrs[0]}}},
				{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: addrs[1]}}},
			}, nil
		},
	})
	defer c.Close()

	state, err := c.state.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Masters) != len(addrs) {
		t.Fatalf("got %d masters, want %d", len(state.Masters), len(addrs))
	}
	for i, node := range state.Masters {
		if got := node.Client.opt.Addr; got != addrs[i] {
			t.Errorf("node %d Addr = %q, want %q", i, got, addrs[i])
		}
	}
}
