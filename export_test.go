package redis

import (
	"fmt"
	"net"
	"time"

	"github.com/go-redis/redis/internal/hashtag"
	"github.com/go-redis/redis/internal/pool"
)

func (c *baseClient) Pool() pool.Pooler {
	return c.connPool
}

func (c *PubSub) SetNetConn(netConn net.Conn) {
	c.cn = pool.NewConn(netConn)
}

func (c *PubSub) ReceiveMessageTimeout(timeout time.Duration) (*Message, error) {
	return c.receiveMessage(timeout)
}

func (c *ClusterClient) GetState() (*clusterState, error) {
	return c.state.Get()
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
	nodes := state.slots[slot]
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
