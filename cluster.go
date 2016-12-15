package redis

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/redis.v4/internal"
	"gopkg.in/redis.v4/internal/errors"
	"gopkg.in/redis.v4/internal/hashtag"
	"gopkg.in/redis.v4/internal/pool"
)

type clusterNode struct {
	Client  *Client
	Latency time.Duration
}

// ClusterClient is a Redis Cluster client representing a pool of zero
// or more underlying connections. It's safe for concurrent use by
// multiple goroutines.
type ClusterClient struct {
	cmdable

	opt *ClusterOptions

	mu     sync.RWMutex
	addrs  []string
	nodes  map[string]*clusterNode
	slots  [][]*clusterNode
	closed bool

	cmdsInfo     map[string]*CommandInfo
	cmdsInfoOnce *sync.Once

	// Reports where slots reloading is in progress.
	reloading uint32
}

var _ Cmdable = (*ClusterClient)(nil)

// NewClusterClient returns a Redis Cluster client as described in
// http://redis.io/topics/cluster-spec.
func NewClusterClient(opt *ClusterOptions) *ClusterClient {
	opt.init()

	c := &ClusterClient{
		opt:   opt,
		nodes: make(map[string]*clusterNode),

		cmdsInfoOnce: new(sync.Once),
	}
	c.cmdable.process = c.Process

	for _, addr := range opt.Addrs {
		_, _ = c.nodeByAddr(addr)
	}
	c.reloadSlots()

	if opt.IdleCheckFrequency > 0 {
		go c.reaper(opt.IdleCheckFrequency)
	}

	return c
}

func (c *ClusterClient) cmdInfo(name string) *CommandInfo {
	c.cmdsInfoOnce.Do(func() {
		for _, node := range c.nodes {
			cmdsInfo, err := node.Client.Command().Result()
			if err == nil {
				c.cmdsInfo = cmdsInfo
				return
			}
		}
		c.cmdsInfoOnce = &sync.Once{}
	})
	return c.cmdsInfo[name]
}

func (c *ClusterClient) getNodes() map[string]*clusterNode {
	var nodes map[string]*clusterNode
	c.mu.RLock()
	if !c.closed {
		nodes = make(map[string]*clusterNode, len(c.nodes))
		for addr, node := range c.nodes {
			nodes[addr] = node
		}
	}
	c.mu.RUnlock()
	return nodes
}

func (c *ClusterClient) Watch(fn func(*Tx) error, keys ...string) error {
	node, err := c.slotMasterNode(hashtag.Slot(keys[0]))
	if err != nil {
		return err
	}
	return node.Client.Watch(fn, keys...)
}

// PoolStats returns accumulated connection pool stats.
func (c *ClusterClient) PoolStats() *PoolStats {
	var acc PoolStats
	for _, node := range c.getNodes() {
		s := node.Client.connPool.Stats()
		acc.Requests += s.Requests
		acc.Hits += s.Hits
		acc.Timeouts += s.Timeouts
		acc.TotalConns += s.TotalConns
		acc.FreeConns += s.FreeConns
	}
	return &acc
}

// Close closes the cluster client, releasing any open resources.
//
// It is rare to Close a ClusterClient, as the ClusterClient is meant
// to be long-lived and shared between many goroutines.
func (c *ClusterClient) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closeClients()
		c.addrs = nil
		c.nodes = nil
		c.slots = nil
		c.cmdsInfo = nil
	}
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *ClusterClient) nodeByAddr(addr string) (*clusterNode, error) {
	c.mu.RLock()
	node, ok := c.nodes[addr]
	c.mu.RUnlock()
	if ok {
		return node, nil
	}

	defer c.mu.Unlock()
	c.mu.Lock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	node, ok = c.nodes[addr]
	if !ok {
		node = c.newNode(addr)
		c.nodes[addr] = node
		c.addrs = append(c.addrs, addr)
	}

	return node, nil
}

func (c *ClusterClient) newNode(addr string) *clusterNode {
	opt := c.opt.clientOptions()
	opt.Addr = addr
	return &clusterNode{
		Client: NewClient(opt),
	}
}

func (c *ClusterClient) slotNodes(slot int) (nodes []*clusterNode) {
	c.mu.RLock()
	if slot < len(c.slots) {
		nodes = c.slots[slot]
	}
	c.mu.RUnlock()
	return nodes
}

// randomNode returns random live node.
func (c *ClusterClient) randomNode() (*clusterNode, error) {
	var nodeErr error
	for i := 0; i < 10; i++ {
		c.mu.RLock()
		closed := c.closed
		addrs := c.addrs
		c.mu.RUnlock()

		if closed {
			return nil, pool.ErrClosed
		}

		n := rand.Intn(len(addrs))

		node, err := c.nodeByAddr(addrs[n])
		if err != nil {
			return nil, err
		}

		nodeErr = node.Client.ClusterInfo().Err()
		if nodeErr == nil {
			return node, nil
		}
	}
	return nil, nodeErr
}

func (c *ClusterClient) slotMasterNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) == 0 {
		return c.randomNode()
	}
	return nodes[0], nil
}

func (c *ClusterClient) slotSlaveNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	switch len(nodes) {
	case 0:
		return c.randomNode()
	case 1:
		return nodes[0], nil
	case 2:
		return nodes[1], nil
	default:
		n := rand.Intn(len(nodes)-1) + 1
		return nodes[n], nil
	}
}

func (c *ClusterClient) slotClosestNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) == 0 {
		return c.randomNode()
	}

	var node *clusterNode
	for _, n := range nodes {
		if node == nil || n.Latency < node.Latency {
			node = n
		}
	}
	return node, nil
}

func (c *ClusterClient) cmdSlotAndNode(cmd Cmder) (int, *clusterNode, error) {
	cmdInfo := c.cmdInfo(cmd.arg(0))
	if cmdInfo == nil {
		internal.Logf("info for cmd=%s not found", cmd.arg(0))
		node, err := c.randomNode()
		return 0, node, err
	}

	if cmdInfo.FirstKeyPos == -1 {
		node, err := c.randomNode()
		return 0, node, err
	}

	firstKey := cmd.arg(int(cmdInfo.FirstKeyPos))
	slot := hashtag.Slot(firstKey)

	if cmdInfo.ReadOnly && c.opt.ReadOnly {
		if c.opt.RouteByLatency {
			node, err := c.slotClosestNode(slot)
			return slot, node, err
		}

		node, err := c.slotSlaveNode(slot)
		return slot, node, err
	}

	node, err := c.slotMasterNode(slot)
	return slot, node, err
}

func (c *ClusterClient) Process(cmd Cmder) error {
	slot, node, err := c.cmdSlotAndNode(cmd)
	if err != nil {
		cmd.setErr(err)
		return err
	}

	var ask bool
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if attempt > 0 {
			cmd.reset()
		}

		if ask {
			pipe := node.Client.Pipeline()
			pipe.Process(NewCmd("ASKING"))
			pipe.Process(cmd)
			_, _ = pipe.Exec()
			pipe.Close()
			ask = false
		} else {
			node.Client.Process(cmd)
		}

		// If there is no (real) error, we are done!
		err := cmd.Err()
		if err == nil {
			return nil
		}

		// On network errors try random node.
		if errors.IsRetryable(err) {
			node, err = c.randomNode()
			continue
		}

		var moved bool
		var addr string
		moved, ask, addr = errors.IsMoved(err)
		if moved || ask {
			master, _ := c.slotMasterNode(slot)
			if moved && (master == nil || master.Client.getAddr() != addr) {
				c.lazyReloadSlots()
			}

			node, err = c.nodeByAddr(addr)
			if err != nil {
				cmd.setErr(err)
				return err
			}
			continue
		}

		break
	}

	return cmd.Err()
}

// ForEachMaster concurrently calls the fn on each master node in the cluster.
// It returns the first error if any.
func (c *ClusterClient) ForEachMaster(fn func(client *Client) error) error {
	c.mu.RLock()
	slots := c.slots
	c.mu.RUnlock()

	var wg sync.WaitGroup
	visited := make(map[*clusterNode]struct{})
	errCh := make(chan error, 1)
	for _, nodes := range slots {
		if len(nodes) == 0 {
			continue
		}

		master := nodes[0]
		if _, ok := visited[master]; ok {
			continue
		}
		visited[master] = struct{}{}

		wg.Add(1)
		go func(node *clusterNode) {
			defer wg.Done()
			err := fn(node.Client)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(master)
	}
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// closeClients closes all clients and returns the first error if there are any.
func (c *ClusterClient) closeClients() error {
	var retErr error
	for _, node := range c.nodes {
		if err := node.Client.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

func (c *ClusterClient) setSlots(cs []ClusterSlot) {
	slots := make([][]*clusterNode, hashtag.SlotNumber)
	for _, s := range cs {
		var nodes []*clusterNode
		for _, n := range s.Nodes {
			node, err := c.nodeByAddr(n.Addr)
			if err == nil {
				nodes = append(nodes, node)
			}
		}

		for i := s.Start; i <= s.End; i++ {
			slots[i] = nodes
		}
	}

	c.mu.Lock()
	if !c.closed {
		c.slots = slots
	}
	c.mu.Unlock()
}

func (c *ClusterClient) lazyReloadSlots() {
	if !atomic.CompareAndSwapUint32(&c.reloading, 0, 1) {
		return
	}
	go c.reloadSlots()
}

func (c *ClusterClient) reloadSlots() {
	defer atomic.StoreUint32(&c.reloading, 0)

	node, err := c.randomNode()
	if err != nil {
		return
	}

	slots, err := node.Client.ClusterSlots().Result()
	if err != nil {
		internal.Logf("ClusterSlots on addr=%q failed: %s", node.Client.getAddr(), err)
		return
	}

	c.setSlots(slots)
	if c.opt.RouteByLatency {
		c.setNodesLatency()
	}
}

func (c *ClusterClient) setNodesLatency() {
	const n = 10
	wg := &sync.WaitGroup{}
	for _, node := range c.getNodes() {
		wg.Add(1)
		go func(node *clusterNode) {
			defer wg.Done()
			var latency time.Duration
			for i := 0; i < n; i++ {
				t1 := time.Now()
				node.Client.Ping()
				latency += time.Since(t1)
			}
			node.Latency = latency / n
		}(node)
	}
	wg.Wait()
}

// reaper closes idle connections to the cluster.
func (c *ClusterClient) reaper(idleCheckFrequency time.Duration) {
	ticker := time.NewTicker(idleCheckFrequency)
	defer ticker.Stop()

	for _ = range ticker.C {
		nodes := c.getNodes()
		if nodes == nil {
			break
		}

		var n int
		for _, node := range nodes {
			nn, err := node.Client.connPool.(*pool.ConnPool).ReapStaleConns()
			if err != nil {
				internal.Logf("ReapStaleConns failed: %s", err)
			} else {
				n += nn
			}
		}

		s := c.PoolStats()
		internal.Logf(
			"reaper: removed %d stale conns (TotalConns=%d FreeConns=%d Requests=%d Hits=%d Timeouts=%d)",
			n, s.TotalConns, s.FreeConns, s.Requests, s.Hits, s.Timeouts,
		)
	}
}

func (c *ClusterClient) Pipeline() *Pipeline {
	pipe := Pipeline{
		exec: c.pipelineExec,
	}
	pipe.cmdable.process = pipe.Process
	pipe.statefulCmdable.process = pipe.Process
	return &pipe
}

func (c *ClusterClient) Pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	return c.Pipeline().pipelined(fn)
}

func (c *ClusterClient) pipelineExec(cmds []Cmder) error {
	var retErr error
	setRetErr := func(err error) {
		if retErr == nil {
			retErr = err
		}
	}

	cmdsMap := make(map[*clusterNode][]Cmder)
	for _, cmd := range cmds {
		_, node, err := c.cmdSlotAndNode(cmd)
		if err != nil {
			cmd.setErr(err)
			setRetErr(err)
			continue
		}
		cmdsMap[node] = append(cmdsMap[node], cmd)
	}

	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		failedCmds := make(map[*clusterNode][]Cmder)

		for node, cmds := range cmdsMap {
			if node == nil {
				var err error
				node, err = c.randomNode()
				if err != nil {
					setCmdsErr(cmds, err)
					setRetErr(err)
					continue
				}
			}

			cn, _, err := node.Client.conn()
			if err != nil {
				setCmdsErr(cmds, err)
				setRetErr(err)
				continue
			}

			failedCmds, err = c.execClusterCmds(cn, cmds, failedCmds)
			if err != nil {
				setRetErr(err)
			}
			node.Client.putConn(cn, err, false)
		}

		cmdsMap = failedCmds
	}

	return retErr
}

func (c *ClusterClient) execClusterCmds(
	cn *pool.Conn, cmds []Cmder, failedCmds map[*clusterNode][]Cmder,
) (map[*clusterNode][]Cmder, error) {
	if err := writeCmd(cn, cmds...); err != nil {
		setCmdsErr(cmds, err)
		return failedCmds, err
	}

	var retErr error
	setRetErr := func(err error) {
		if retErr == nil {
			retErr = err
		}
	}

	for i, cmd := range cmds {
		err := cmd.readReply(cn)
		if err == nil {
			continue
		}
		if errors.IsNetwork(err) {
			cmd.reset()
			failedCmds[nil] = append(failedCmds[nil], cmds[i:]...)
			break
		} else if moved, ask, addr := errors.IsMoved(err); moved {
			c.lazyReloadSlots()
			cmd.reset()
			node, err := c.nodeByAddr(addr)
			if err != nil {
				setRetErr(err)
				continue
			}
			failedCmds[node] = append(failedCmds[node], cmd)
		} else if ask {
			cmd.reset()
			node, err := c.nodeByAddr(addr)
			if err != nil {
				setRetErr(err)
				continue
			}
			failedCmds[node] = append(failedCmds[node], NewCmd("ASKING"), cmd)
		} else {
			setRetErr(err)
		}
	}

	return failedCmds, retErr
}

//------------------------------------------------------------------------------

// ClusterOptions are used to configure a cluster client and should be
// passed to NewClusterClient.
type ClusterOptions struct {
	// A seed list of host:port addresses of cluster nodes.
	Addrs []string

	// The maximum number of retries before giving up. Command is retried
	// on network errors and MOVED/ASK redirects.
	// Default is 16.
	MaxRedirects int

	// Enables read queries for a connection to a Redis Cluster slave node.
	ReadOnly bool

	// Enables routing read-only queries to the closest master or slave node.
	RouteByLatency bool

	// Following options are copied from Options struct.

	Password string

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// PoolSize applies per cluster node and not for the whole cluster.
	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

func (opt *ClusterOptions) init() {
	if opt.MaxRedirects == -1 {
		opt.MaxRedirects = 0
	} else if opt.MaxRedirects == 0 {
		opt.MaxRedirects = 16
	}

	if opt.RouteByLatency {
		opt.ReadOnly = true
	}
}

func (opt *ClusterOptions) clientOptions() *Options {
	const disableIdleCheck = -1

	return &Options{
		Password: opt.Password,
		ReadOnly: opt.ReadOnly,

		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:    opt.PoolSize,
		PoolTimeout: opt.PoolTimeout,
		IdleTimeout: opt.IdleTimeout,

		// IdleCheckFrequency is not copied to disable reaper
		IdleCheckFrequency: disableIdleCheck,
	}
}
