package pubsub

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// fakeCluster is a set of fake servers keyed by node addr, plus the
// slot-owner table the manager's ResolveOwners hook reads. The empty
// addr (the broadcast connection) dials the broadcast server.
type fakeCluster struct {
	mu     sync.Mutex
	nodes  map[string]*fakeServer
	owners map[string]string // shard channel => owning node addr

	broadcast *fakeServer
	reloads   chan struct{}
}

func newFakeCluster(addrs ...string) *fakeCluster {
	c := &fakeCluster{
		nodes:     make(map[string]*fakeServer, len(addrs)),
		owners:    make(map[string]string),
		broadcast: newFakeServer(),
		reloads:   make(chan struct{}, 64),
	}
	c.broadcast.autoConfirm = true
	for _, addr := range addrs {
		srv := newFakeServer()
		srv.autoConfirm = true
		c.nodes[addr] = srv
	}
	return c
}

// own assigns the owner of a shard channel.
func (c *fakeCluster) own(ch, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owners[ch] = addr
}

// fakeCluster implements ClusterHooks: the fake servers are its nodes.
var _ ClusterHooks = (*fakeCluster)(nil)

func (c *fakeCluster) NewConn(ctx context.Context, addr string) (*pool.Conn, error) {
	if addr == "" {
		return c.broadcast.dial(ctx, addr)
	}
	c.mu.Lock()
	srv := c.nodes[addr]
	c.mu.Unlock()
	if srv == nil {
		return nil, errors.New("fake cluster: unknown node " + addr)
	}
	return srv.dial(ctx, addr)
}

func (c *fakeCluster) CloseConn(cn *pool.Conn) error { return cn.Close() }

func (c *fakeCluster) ProcessPush(context.Context, string, *pool.Conn, *proto.Reader) error {
	return nil
}

func (c *fakeCluster) IsBadConn(err error, allowTimeout bool, _ string) bool {
	return testIsBadConn(err, allowTimeout)
}

func (c *fakeCluster) ShouldReplaceConn(cn *pool.Conn) bool { return testShouldHandoff(cn) }

func (c *fakeCluster) ResolveAddress(prev *pool.Conn, current string) string {
	return testResolveHandoff(prev, current)
}

// ResolveOwners mirrors the cluster client's sticky resolve: a channel
// stays where it is while that node still owns it.
func (c *fakeCluster) ResolveOwners(_ context.Context, channels []string, current map[string]string) (map[string]map[string]struct{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	groups := make(map[string]map[string]struct{})
	for _, ch := range channels {
		owner, ok := c.owners[ch]
		if !ok {
			return nil, errors.New("fake cluster: no owner for " + ch)
		}
		if cur, placed := current[ch]; placed && cur == owner {
			owner = cur
		}
		if groups[owner] == nil {
			groups[owner] = make(map[string]struct{})
		}
		groups[owner][ch] = struct{}{}
	}
	return groups, nil
}

func (c *fakeCluster) RequestReload() {
	select {
	case c.reloads <- struct{}{}:
	default:
	}
}

func (c *fakeCluster) waitReload(t *testing.T) {
	t.Helper()
	select {
	case <-c.reloads:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a reload request")
	}
}

func newTestClusterManager(t *testing.T, c *fakeCluster) *ClusterManager {
	t.Helper()
	m := NewClusterManager(testConfig(""), c)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// recvSubscription receives the next subscription confirmation from an
// all-events channel, skipping anything else.
func recvSubscription(t *testing.T, ch <-chan any) *Subscription {
	t.Helper()
	for {
		if sub, ok := recvEvent(t, ch).(*Subscription); ok {
			return sub
		}
	}
}

// TestClusterSSubscribeRoutesByOwner pins the core routing: shard
// channels go to their owning node's connection (one connection per
// node), and messages from every node land on the one composite
// stream, with the composite's own running Count on confirmations.
func TestClusterSSubscribeRoutesByOwner(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379", "n2:6379")
	c.own("a", "n1:6379")
	c.own("b", "n2:6379")
	m := newTestClusterManager(t, c)

	h := m.NewHandle()
	events := h.ChannelWithSubscriptions()
	if _, err := h.SSubscribe(ctx, "a", "b"); err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	n1 := c.nodes["n1:6379"].waitDial(t)
	n1.expectCmd(t, "ssubscribe", "a")
	n2 := c.nodes["n2:6379"].waitDial(t)
	n2.expectCmd(t, "ssubscribe", "b")

	// Two confirmations, counted across both nodes: 1 then 2, whichever
	// node answers first.
	var counts []int
	for range 2 {
		counts = append(counts, recvSubscription(t, events).Count)
	}
	slices.Sort(counts)
	if !slices.Equal(counts, []int{1, 2}) {
		t.Fatalf("confirmation counts = %v, want [1 2]", counts)
	}

	n1.sendMessage(t, "smessage", "a", "from-n1")
	n2.sendMessage(t, "smessage", "b", "from-n2")
	var got []string
	for range 2 {
		msg := recvEvent(t, events).(*Message)
		got = append(got, msg.Channel+"="+msg.Payload)
	}
	slices.Sort(got)
	if want := []string{"a=from-n1", "b=from-n2"}; !slices.Equal(got, want) {
		t.Fatalf("messages = %v, want %v", got, want)
	}

	_, _, schannels := h.Subscriptions()
	slices.Sort(schannels)
	if !slices.Equal(schannels, []string{"a", "b"}) {
		t.Fatalf("Subscriptions shard channels = %v, want [a b]", schannels)
	}
}

// TestClusterSubscribeUsesBroadcast pins that regular and pattern
// subscriptions share the one broadcast connection, and that a manager-
// level Subscribe hands out a composite handle (so a later SSubscribe on
// it is slot-routed, not sent down the broadcast connection).
func TestClusterSubscribeUsesBroadcast(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379")
	c.own("s", "n1:6379")
	m := newTestClusterManager(t, c)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	bc := c.broadcast.waitDial(t)
	bc.expectCmd(t, "subscribe", "ch")
	if _, err := h.PSubscribe(ctx, "p*"); err != nil {
		t.Fatalf("PSubscribe: %v", err)
	}
	bc.expectCmd(t, "psubscribe", "p*")

	if _, err := h.SSubscribe(ctx, "s"); err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	c.nodes["n1:6379"].waitDial(t).expectCmd(t, "ssubscribe", "s")
	bc.expectNoCmd(t, 50*time.Millisecond)

	msgs := h.Channel()
	bc.sendMessage(t, "message", "ch", "hello")
	if msg := recvMsg(t, msgs); msg.Channel != "ch" || msg.Payload != "hello" {
		t.Fatalf("message = %v", msg)
	}

	channels, patterns, schannels := h.Subscriptions()
	if !slices.Equal(channels, []string{"ch"}) || !slices.Equal(patterns, []string{"p*"}) || !slices.Equal(schannels, []string{"s"}) {
		t.Fatalf("Subscriptions = %v %v %v", channels, patterns, schannels)
	}
}

// TestClusterRerouteMovesChannel pins the re-route sweep: after the
// owner of a shard channel changes, the sweep subscribes on the new
// owner first, releases the channel on the old one, and prunes the
// emptied node's connection. The consumer sees the resubscription with
// an unchanged Count, and no unsubscribe: the composite never lost the
// channel.
func TestClusterRerouteMovesChannel(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379", "n2:6379")
	c.own("a", "n1:6379")
	m := newTestClusterManager(t, c)

	h, err := m.SSubscribe(ctx, "a")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	events := h.ChannelWithSubscriptions()
	n1 := c.nodes["n1:6379"].waitDial(t)
	n1.expectCmd(t, "ssubscribe", "a")
	if sub := recvSubscription(t, events); sub.Kind != "ssubscribe" || sub.Count != 1 {
		t.Fatalf("first confirmation = %v (count %d)", sub, sub.Count)
	}

	// Nothing to move while the owner is unchanged.
	m.rerouteShards(ctx)
	n1.expectNoCmd(t, 50*time.Millisecond)

	c.own("a", "n2:6379")
	m.ScheduleReroute()
	n2 := c.nodes["n2:6379"].waitDial(t)
	n2.expectCmd(t, "ssubscribe", "a")
	n1.expectCmd(t, "sunsubscribe", "a")
	n1.expectClosed(t) // emptied part closed, node manager pruned

	if sub := recvSubscription(t, events); sub.Kind != "ssubscribe" || sub.Channel != "a" || sub.Count != 1 {
		t.Fatalf("resubscription = %v (count %d), want ssubscribe a with count 1", sub, sub.Count)
	}
	n2.sendMessage(t, "smessage", "a", "after-move")
	switch ev := recvEvent(t, events).(type) {
	case *Message:
		if ev.Payload != "after-move" {
			t.Fatalf("message = %v", ev)
		}
	default:
		t.Fatalf("event after the move = %T %v, want the message (no sunsubscribe)", ev, ev)
	}

	_, _, schannels := h.Subscriptions()
	if !slices.Equal(schannels, []string{"a"}) {
		t.Fatalf("Subscriptions shard channels = %v, want [a]", schannels)
	}
}

// TestClusterRerouteJoinsExistingPart pins that a channel moving to a
// node the handle already has a part on joins that part (one
// connection per node), and that a channel whose owner is unknown
// stays desired-but-unplaced until a sweep can place it.
func TestClusterRerouteJoinsExistingPart(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379", "n2:6379")
	c.own("a", "n1:6379")
	c.own("b", "n2:6379")
	m := newTestClusterManager(t, c)

	h, err := m.SSubscribe(ctx, "a", "b")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	n1 := c.nodes["n1:6379"].waitDial(t)
	n1.expectCmd(t, "ssubscribe", "a")
	n2 := c.nodes["n2:6379"].waitDial(t)
	n2.expectCmd(t, "ssubscribe", "b")

	// An unresolvable channel is recorded as intent and reported; the
	// manager asks for the reload whose sweep retries it.
	if _, err := h.SSubscribe(ctx, "c"); err == nil {
		t.Fatal("SSubscribe of a channel with no owner succeeded")
	}
	c.waitReload(t)
	_, _, schannels := h.Subscriptions()
	slices.Sort(schannels)
	if !slices.Equal(schannels, []string{"a", "b", "c"}) {
		t.Fatalf("Subscriptions shard channels = %v, want [a b c]", schannels)
	}

	c.own("a", "n2:6379")
	c.own("c", "n2:6379")
	m.rerouteShards(ctx)
	// Both land on n2's existing connection (one SSUBSCRIBE per hash
	// slot, in sorted-name order).
	n2.expectCmd(t, "ssubscribe", "a")
	n2.expectCmd(t, "ssubscribe", "c")
	n1.expectCmd(t, "sunsubscribe", "a")
	n1.expectClosed(t)
	select {
	case <-c.nodes["n2:6379"].dialCh:
		t.Fatal("a second connection to n2 was dialed")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestClusterSUnsubscribePrunes pins that releasing a node's last shard
// channel closes that node's connection, through the handle and
// through the manager-wide SUnsubscribe alike.
func TestClusterSUnsubscribePrunes(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379", "n2:6379")
	c.own("a", "n1:6379")
	c.own("b", "n2:6379")
	m := newTestClusterManager(t, c)

	h, err := m.SSubscribe(ctx, "a", "b")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	n1 := c.nodes["n1:6379"].waitDial(t)
	n1.expectCmd(t, "ssubscribe", "a")
	n2 := c.nodes["n2:6379"].waitDial(t)
	n2.expectCmd(t, "ssubscribe", "b")

	if err := h.SUnsubscribe(ctx, "a"); err != nil {
		t.Fatalf("SUnsubscribe: %v", err)
	}
	n1.expectCmd(t, "sunsubscribe", "a")
	// The part is emptied but the handle keeps it until a sweep or
	// close; the node manager stays while the part handle lives. A
	// manager-wide SUnsubscribe of everything releases b and prunes.
	if err := m.SUnsubscribe(ctx); err != nil {
		t.Fatalf("manager SUnsubscribe: %v", err)
	}
	n2.expectCmd(t, "sunsubscribe", "b")
	_, _, schannels := h.Subscriptions()
	if len(schannels) != 0 {
		t.Fatalf("Subscriptions shard channels = %v, want none", schannels)
	}

	// Closing the handle releases its parts; both nodes' connections go.
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	n1.expectClosed(t)
	n2.expectClosed(t)
}

// TestClusterCloseEndsStreams pins the teardown contract: the manager's
// Close ends every handle's stream and every connection; a second Close
// reports ErrClosed; closing a handle of a closed manager is a nil
// no-op, while a handle closed twice on a live manager reports
// ErrClosed; and a handle born of a closed manager is born closed.
func TestClusterCloseEndsStreams(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379")
	c.own("a", "n1:6379")
	m := newTestClusterManager(t, c)

	h, err := m.SSubscribe(ctx, "a")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	if _, err := h.Subscribe(ctx, "ch"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	n1 := c.nodes["n1:6379"].waitDial(t)
	bc := c.broadcast.waitDial(t)
	msgs := h.Channel()

	h2 := m.NewHandle()
	if err := h2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h2.Close(); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("second Close = %v, want pool.ErrClosed", err)
	}
	if _, err := h2.SSubscribe(ctx, "a"); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("SSubscribe after Close = %v, want pool.ErrClosed", err)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("manager Close: %v", err)
	}
	expectChanClosed(t, msgs)
	n1.expectClosed(t)
	bc.expectClosed(t)
	if err := m.Close(); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("second manager Close = %v, want pool.ErrClosed", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("handle Close after manager Close = %v, want nil", err)
	}

	born := m.NewHandle()
	if _, ok := <-born.Events(); ok {
		t.Fatal("a handle of a closed manager has an open events stream")
	}
	if _, err := born.Subscribe(ctx, "x"); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("Subscribe on a born-closed handle = %v, want pool.ErrClosed", err)
	}
}

// TestClusterDialFailureKeepsIntent pins that a shard subscribe whose
// node cannot be dialed is reported but kept: the part is attached with
// its registration, and the engine's reconnect establishes it once the
// node is back — no sweep needed.
func TestClusterDialFailureKeepsIntent(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379")
	c.own("a", "n1:6379")
	dialErr := errors.New("node down")
	c.nodes["n1:6379"].setDialErr(dialErr)
	m := newTestClusterManager(t, c)

	h := m.NewHandle()
	events := h.ChannelWithSubscriptions()
	if _, err := h.SSubscribe(ctx, "a"); !errors.Is(err, dialErr) {
		t.Fatalf("SSubscribe with the node down = %v, want %v", err, dialErr)
	}
	c.waitReload(t)
	_, _, schannels := h.Subscriptions()
	if !slices.Equal(schannels, []string{"a"}) {
		t.Fatalf("Subscriptions shard channels = %v, want [a]", schannels)
	}

	c.nodes["n1:6379"].setDialErr(nil)
	n1 := c.nodes["n1:6379"].waitDial(t)
	n1.expectCmd(t, "ssubscribe", "a")
	if sub := recvSubscription(t, events); sub.Kind != "ssubscribe" || sub.Channel != "a" {
		t.Fatalf("confirmation = %v", sub)
	}
}

// TestClusterPingOnePong pins that a Ping over several parts surfaces
// exactly one pong, and that DeliverError reaches the stream.
func TestClusterPingOnePong(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379", "n2:6379")
	c.own("a", "n1:6379")
	c.own("b", "n2:6379")
	for _, srv := range c.nodes {
		srv.autoPong = true
	}
	m := newTestClusterManager(t, c)

	h, err := m.SSubscribe(ctx, "a", "b")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	n1 := c.nodes["n1:6379"].waitDial(t)
	n1.expectCmd(t, "ssubscribe", "a")
	n2 := c.nodes["n2:6379"].waitDial(t)
	n2.expectCmd(t, "ssubscribe", "b")
	// Drain the two confirmations.
	for range 2 {
		recvSubscription(t, h.Events())
	}

	if err := h.Ping(ctx, "hi"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	n1.expectCmd(t, "ping", "hi")
	n2.expectCmd(t, "ping", "hi")
	if pong, ok := recvEvent(t, h.Events()).(*Pong); !ok || pong.Payload != "hi" {
		t.Fatalf("event after Ping = %v, want one pong", pong)
	}
	select {
	case ev := <-h.Events():
		t.Fatalf("second event after one Ping: %v", ev)
	case <-time.After(50 * time.Millisecond):
	}

	wantErr := errors.New("initial")
	h.DeliverError(wantErr)
	if ev := recvEvent(t, h.Events()); ev != wantErr {
		t.Fatalf("event after DeliverError = %v, want %v", ev, wantErr)
	}
}

// TestClusterChannelOptionsReachEveryNode pins that a ChannelOption on
// a composite handle updates the seed config (handles created after it)
// and every owned node manager.
func TestClusterChannelOptionsReachEveryNode(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379")
	c.own("a", "n1:6379")
	m := newTestClusterManager(t, c)

	h, err := m.SSubscribe(ctx, "a")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	c.nodes["n1:6379"].waitDial(t)

	h.Channel(func(cfg PubSubConfiger) { cfg.UpdateChannelSize(7) })
	if size, _ := m.consumerSettings(); size != 7 {
		t.Fatalf("seed ChanSize = %d, want 7", size)
	}
	nm := m.nodeManager("n1:6379")
	if size, _ := nm.consumerSettings(); size != 7 {
		t.Fatalf("node manager ChanSize = %d, want 7", size)
	}
	if size, _ := m.broadcast.consumerSettings(); size != 7 {
		t.Fatalf("broadcast ChanSize = %d, want 7", size)
	}
	if next := m.NewHandle().(*clusterShardHandle); cap(next.events) != 7 {
		t.Fatalf("next handle's events cap = %d, want 7", cap(next.events))
	}
}

// TestClusterMovedTriggersReroute pins the MOVED path: a node rejecting
// SSUBSCRIBE with MOVED surfaces the error on the composite stream and
// asks for a topology reload; the sweep that follows the reload moves
// the channel to its new owner and releases the old node.
func TestClusterMovedTriggersReroute(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster("n1:6379", "n2:6379")
	c.own("a", "n1:6379") // stale view: n1 no longer serves the slot
	c.nodes["n1:6379"].autoConfirm = false
	m := newTestClusterManager(t, c)

	h, err := m.SSubscribe(ctx, "a")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	n1 := c.nodes["n1:6379"].waitDial(t)
	n1.expectCmd(t, "ssubscribe", "a")
	n1.sendError(t, "MOVED 15495 n2:6379")

	if ev, ok := recvEvent(t, h.Events()).(error); !ok || !isMovedError(ev) {
		t.Fatalf("event after MOVED = %v, want the MOVED error", ev)
	}
	c.waitReload(t)

	// The reload brings the fresh view; its sweep moves the channel.
	c.own("a", "n2:6379")
	m.ScheduleReroute()
	n2 := c.nodes["n2:6379"].waitDial(t)
	n2.expectCmd(t, "ssubscribe", "a")
	n1.expectCmd(t, "sunsubscribe", "a")
	n1.expectClosed(t)
	if sub, ok := recvEvent(t, h.Events()).(*Subscription); !ok || sub.Kind != "ssubscribe" || sub.Count != 1 {
		t.Fatalf("event after the move = %v, want ssubscribe a (count 1)", sub)
	}
}
