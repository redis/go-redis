package redis

import (
	"context"
	"testing"
)

// openPubSubConn opens and tracks one pub/sub connection on the client, so its
// pub/sub pool reports Created=1 and Active=1 without a server.
func openPubSubConn(t *testing.T, c *Client) {
	t.Helper()
	cn, err := c.pubSubPool.NewConn(context.Background(), "tcp", c.opt.Addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.pubSubPool.TrackConn(cn); err != nil {
		t.Fatal(err)
	}
}

// PoolStats of a cluster or ring client sums the pub/sub stats of its nodes,
// as Client.PoolStats reports them for a single node.
func TestClusterPoolStatsSumsPubSubStats(t *testing.T) {
	addrs := []string{"127.0.0.1:1", "127.0.0.1:2"}
	c := NewClusterClient(&ClusterOptions{
		Addrs:  addrs,
		Dialer: pipeDialer,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{
				{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: addrs[0]}}},
				{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: addrs[1]}}},
			}, nil
		},
	})
	defer c.Close()

	state, err := c.state.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range state.Masters {
		openPubSubConn(t, node.Client)
	}

	got := c.PoolStats().PubSubStats
	if got.Created != 2 || got.Active != 2 {
		t.Errorf("PubSubStats = %+v, want Created=2 Active=2", got)
	}
}

func TestRingPoolStatsSumsPubSubStats(t *testing.T) {
	ring := NewRing(&RingOptions{
		Addrs:  map[string]string{"a": "127.0.0.1:1", "b": "127.0.0.1:2"},
		Dialer: pipeDialer,
	})
	defer ring.Close()

	for _, shard := range ring.sharding.List() {
		openPubSubConn(t, shard.Client)
	}

	got := ring.PoolStats().PubSubStats
	if got.Created != 2 || got.Active != 2 {
		t.Errorf("PubSubStats = %+v, want Created=2 Active=2", got)
	}
}
