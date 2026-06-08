package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
)

func TestShardedPubSubChannelGrouping(t *testing.T) {
	// Verify that channels hash to different slots (the core issue).
	// In the bug report, ch1-ch9 hash to different slots, but only
	// messages from one shard's channels are received.
	slots := make(map[int][]string)
	for i := 1; i <= 9; i++ {
		ch := "ch" + string(rune('0'+i))
		slot := hashtag.Slot(ch)
		slots[slot] = append(slots[slot], ch)
	}

	// There should be multiple different slots for ch1-ch9.
	if len(slots) <= 1 {
		t.Fatalf("expected channels to hash to multiple slots, got %d", len(slots))
	}
	t.Logf("channels hash to %d different slots: %v", len(slots), slots)
}

func TestShardedPubSubNewAndClose(t *testing.T) {
	// Test that ShardedPubSub can be created and closed without panics.
	// We can't test actual cluster connectivity without a running cluster,
	// but we can verify the struct initializes correctly.
	cluster := &ClusterClient{
		opt: &ClusterOptions{},
	}

	sps := newShardedPubSub(cluster)
	if sps == nil {
		t.Fatal("newShardedPubSub returned nil")
	}
	if sps.closed {
		t.Fatal("new ShardedPubSub should not be closed")
	}
	if len(sps.shards) != 0 {
		t.Fatalf("expected 0 shards, got %d", len(sps.shards))
	}

	// Close should work on empty ShardedPubSub.
	err := sps.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Double close should return ErrClosed.
	err = sps.Close()
	if err == nil {
		t.Fatal("expected error on double close")
	}
}

func TestShardedPubSubSSubscribeWhenClosed(t *testing.T) {
	cluster := &ClusterClient{
		opt: &ClusterOptions{},
	}
	sps := newShardedPubSub(cluster)
	_ = sps.Close()

	err := sps.SSubscribe(context.Background(), "ch1")
	if err == nil {
		t.Fatal("expected error when subscribing on closed ShardedPubSub")
	}
}

func TestShardedPubSubChannelOptionSize(t *testing.T) {
	cluster := &ClusterClient{
		opt: &ClusterOptions{},
	}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	// WithChannelSize must be honored even when a later option (which does not
	// touch chanSize) is applied. A previous bug reset the probe on every
	// iteration, so the size from an earlier option was lost.
	ch := sps.Channel(WithChannelSize(500), WithChannelSendTimeout(time.Second))
	if cap(ch) != 500 {
		t.Fatalf("expected channel buffer size 500, got %d", cap(ch))
	}
}

func TestShardedPubSubChannelDefaultSize(t *testing.T) {
	cluster := &ClusterClient{
		opt: &ClusterOptions{},
	}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	ch := sps.Channel()
	if cap(ch) != 100 {
		t.Fatalf("expected default channel buffer size 100, got %d", cap(ch))
	}
}

func TestShardedPubSubRegistration(t *testing.T) {
	cluster := &ClusterClient{
		opt: &ClusterOptions{},
	}

	sps1 := newShardedPubSub(cluster)
	sps2 := newShardedPubSub(cluster)

	cluster.spsMu.Lock()
	if len(cluster.shardedPubSubs) != 2 {
		t.Fatalf("expected 2 registered, got %d", len(cluster.shardedPubSubs))
	}
	cluster.spsMu.Unlock()

	// Close sps1 should deregister it.
	_ = sps1.Close()

	cluster.spsMu.Lock()
	if len(cluster.shardedPubSubs) != 1 {
		t.Fatalf("expected 1 registered after close, got %d", len(cluster.shardedPubSubs))
	}
	if cluster.shardedPubSubs[0] != sps2 {
		t.Fatal("expected sps2 to remain registered")
	}
	cluster.spsMu.Unlock()

	_ = sps2.Close()

	cluster.spsMu.Lock()
	if len(cluster.shardedPubSubs) != 0 {
		t.Fatalf("expected 0 registered after all closed, got %d", len(cluster.shardedPubSubs))
	}
	cluster.spsMu.Unlock()
}

// failingPubSub returns a *PubSub whose subscribe/unsubscribe calls always fail
// because its newConn factory returns err. This lets us exercise error paths in
// ShardedPubSub without a live server.
func failingPubSub(err error) *PubSub {
	ps := &PubSub{
		opt: &Options{},
		newConn: func(ctx context.Context, addr string, channels []string) (*pool.Conn, error) {
			return nil, err
		},
		closeConn: func(cn *pool.Conn) error { return nil },
	}
	ps.init()
	return ps
}

func TestShardedPubSubSUnsubscribeWhenClosed(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)
	_ = sps.Close()

	if err := sps.SUnsubscribe(context.Background(), "ch1"); err == nil {
		t.Fatal("expected error when unsubscribing on closed ShardedPubSub")
	}
}

func TestShardedPubSubSUnsubscribeUntracked(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	// Unsubscribing channels that were never subscribed is a no-op.
	if err := sps.SUnsubscribe(context.Background(), "never-subscribed"); err != nil {
		t.Fatalf("expected nil error for untracked channel, got %v", err)
	}
}

func TestShardedPubSubSUnsubscribeNoShardConnection(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	// Channel is tracked but there is no live shard connection for its addr.
	// SUnsubscribe should drop the mapping and return nil (nothing to send).
	sps.mu.Lock()
	sps.chanShard["ch1"] = "127.0.0.1:7000"
	sps.mu.Unlock()

	if err := sps.SUnsubscribe(context.Background(), "ch1"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	sps.mu.Lock()
	_, ok := sps.chanShard["ch1"]
	sps.mu.Unlock()
	if ok {
		t.Fatal("expected mapping to be removed after successful unsubscribe")
	}
}

func TestShardedPubSubSUnsubscribeKeepsMappingOnError(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	errBoom := errors.New("boom")
	addr := "127.0.0.1:7000"

	sps.mu.Lock()
	sps.shards[addr] = failingPubSub(errBoom)
	sps.chanShard["ch1"] = addr
	sps.mu.Unlock()

	// The underlying SUnsubscribe fails, so the channel mapping must be
	// preserved — otherwise the client would forget a channel whose server-side
	// subscription is still active.
	if err := sps.SUnsubscribe(context.Background(), "ch1"); err == nil {
		t.Fatal("expected error from failing shard SUnsubscribe")
	}

	sps.mu.Lock()
	_, ok := sps.chanShard["ch1"]
	sps.mu.Unlock()
	if !ok {
		t.Fatal("mapping must be retained when the underlying SUnsubscribe fails")
	}
}

func TestShardedPubSubSUnsubscribeAll(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	// Two tracked channels with no live shard connections. Calling
	// SUnsubscribe with no arguments must unsubscribe from all of them,
	// mirroring PubSub.SUnsubscribe ("unsubscribe from all").
	sps.mu.Lock()
	sps.chanShard["ch1"] = "127.0.0.1:7000"
	sps.chanShard["ch2"] = "127.0.0.1:7001"
	sps.mu.Unlock()

	if err := sps.SUnsubscribe(context.Background()); err != nil {
		t.Fatalf("expected nil error for unsubscribe-all, got %v", err)
	}

	sps.mu.Lock()
	n := len(sps.chanShard)
	sps.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected all channel mappings removed, got %d", n)
	}
}

func TestShardedPubSubChannelAfterClose(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)

	if err := sps.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Calling Channel() after Close() must return an already-closed channel
	// rather than a new open channel that would block consumers forever.
	ch := sps.Channel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel from Channel()-after-Close to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("Channel() after Close() returned a channel that never closes")
	}
}

func TestShardedPubSubChannelIdempotent(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	ch1 := sps.Channel(WithChannelSize(250))
	ch2 := sps.Channel(WithChannelSize(999))

	// Channel must be created once (sync.Once); later calls return the same
	// channel and must not reset the buffer size.
	if ch1 != ch2 {
		t.Fatal("expected Channel to return the same channel on repeated calls")
	}
	if cap(ch1) != 250 {
		t.Fatalf("expected buffer size 250 from the first call, got %d", cap(ch1))
	}
}

func TestShardedPubSubChannelClosedAfterClose(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)

	ch := sps.Channel()
	if err := sps.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// The multiplexed channel must be closed once Close completes.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed and drained")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel to close")
	}

	// ReceiveMessage must report closed after Close.
	if _, err := sps.ReceiveMessage(context.Background()); err == nil {
		t.Fatal("expected error from ReceiveMessage after Close")
	}
}

func TestShardedPubSubReceiveMessageBeforeChannel(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)
	defer func() { _ = sps.Close() }()

	// ReceiveMessage must error if Channel was never called.
	if _, err := sps.ReceiveMessage(context.Background()); err == nil {
		t.Fatal("expected error when ReceiveMessage is called before Channel")
	}
}

func TestShardedPubSubPingClosedAndEmpty(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	sps := newShardedPubSub(cluster)

	// Ping with no shards is a no-op success.
	if err := sps.Ping(context.Background()); err != nil {
		t.Fatalf("expected nil error pinging with no shards, got %v", err)
	}

	_ = sps.Close()
	if err := sps.Ping(context.Background()); err == nil {
		t.Fatal("expected error pinging a closed ShardedPubSub")
	}
}

// waitClusterFlagsZero waits until the notifier flags settle back to zero. A
// stuck notifier (e.g. a lost wakeup in the coalescing loop) would leave
// spsNotifying or spsPending set and trip the timeout.
func waitClusterFlagsZero(t *testing.T, c *ClusterClient, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if atomic.LoadUint32(&c.spsNotifying) == 0 && atomic.LoadUint32(&c.spsPending) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("notifier flags did not settle: notifying=%d pending=%d",
		atomic.LoadUint32(&c.spsNotifying), atomic.LoadUint32(&c.spsPending))
}

func TestClusterClientNotifyShardedPubSubsCoalesce(t *testing.T) {
	cluster := &ClusterClient{opt: &ClusterOptions{}}
	// Registered subs with empty channel maps make onTopologyChange a no-op,
	// so this exercises the coalescing state machine, not cluster state access.
	sps1 := newShardedPubSub(cluster)
	sps2 := newShardedPubSub(cluster)
	defer func() { _ = sps1.Close(); _ = sps2.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cluster.notifyShardedPubSubs()
		}()
	}
	wg.Wait()
	waitClusterFlagsZero(t, cluster, 2*time.Second)

	// A later notification must still spawn a notifier and settle, proving the
	// notifier slot was released (not stuck at 1) after the burst.
	cluster.notifyShardedPubSubs()
	waitClusterFlagsZero(t, cluster, 2*time.Second)
}
