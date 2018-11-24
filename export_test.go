package redis

import (
	"fmt"
	"net"
	"strings"

	"github.com/go-redis/redis/internal/hashtag"
	"github.com/go-redis/redis/internal/pool"
)

func (c *baseClient) Pool() pool.Pooler {
	return c.connPool
}

func (c *PubSub) SetNetConn(netConn net.Conn) {
	c.cn = pool.NewConn(netConn)
}

func (c *ClusterClient) LoadState() (*clusterState, error) {
	return c.loadState()
}

func (c *ClusterClient) SlotAddrs(slot int) []string {
	state, err := c.state.Get()
	if err != nil {
		panic(err)
	}

	var addrs []string
	for _, n := range state.slotNodes(slot) {
		addrs = append(addrs, n.Client.getAddr())
	}
	return addrs
}

func (c *ClusterClient) Nodes(key string) ([]*clusterNode, error) {
	state, err := c.state.Reload()
	if err != nil {
		return nil, err
	}

	slot := hashtag.Slot(key)
	nodes := state.slotNodes(slot)
	if len(nodes) != 2 {
		return nil, fmt.Errorf("slot=%d does not have enough nodes: %v", slot, nodes)
	}
	return nodes, nil
}

func (c *ClusterClient) SwapNodes(key string) error {
	nodes, err := c.Nodes(key)
	if err != nil {
		return err
	}
	nodes[0], nodes[1] = nodes[1], nodes[0]
	return nil
}

func (state *clusterState) IsConsistent() bool {
	if len(state.Masters) < 3 {
		return false
	}
	for _, master := range state.Masters {
		s := master.Client.Info("replication").Val()
		if !strings.Contains(s, "role:master") {
			return false
		}
	}

	if len(state.Slaves) < 3 {
		return false
	}
	for _, slave := range state.Slaves {
		s := slave.Client.Info("replication").Val()
		if !strings.Contains(s, "role:slave") {
			return false
		}
	}

	return true
}
