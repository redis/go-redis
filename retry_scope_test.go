package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestSingleNodeDialRetryScope(t *testing.T) {
	dialErr := fmt.Errorf("dial failed: %w", &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded})
	if !shouldRetry(dialErr, true) {
		t.Fatal("dial failures must remain retryable for cluster and MultiDB failover")
	}
	if !shouldRetrySingleNode(dialErr, true) {
		t.Fatal("a credential provider dial error must remain retryable")
	}
	poolErr := fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, dialErr)
	if shouldRetrySingleNode(poolErr, true) {
		t.Fatal("a single-node command must not restart the exhausted pool dial budget")
	}
	if !shouldRetrySingleNode(io.EOF, true) {
		t.Fatal("non-dial connection errors should still retry")
	}
}

func TestFailoverDialRetryScope(t *testing.T) {
	dialErr := fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, &net.OpError{
		Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused"),
	})

	client := NewClient(&Options{Addr: "127.0.0.1:0", DialerRetries: 1, MaxRetries: 2})
	defer client.Close()
	if client.shouldRetryCommand(dialErr, true) {
		t.Fatal("fixed-address client must not repeat an exhausted dial sequence")
	}

	failover := NewFailoverClient(&FailoverOptions{
		MasterName:    "test",
		SentinelAddrs: []string{"127.0.0.1:0"},
		ReplicaOnly:   true,
		DialerRetries: 1,
		MaxRetries:    2,
	})
	defer failover.Close()
	if !failover.shouldRetryCommand(dialErr, true) {
		t.Fatal("Sentinel client must be able to retry a different replica")
	}
	if !failover.clone().shouldRetryCommand(dialErr, true) {
		t.Fatal("derived Sentinel client must retain its endpoint-retry policy")
	}
	conn := failover.Conn()
	defer conn.Close()
	if !conn.shouldRetryCommand(dialErr, true) {
		t.Fatal("Sentinel Conn wrapper must retain its endpoint-retry policy")
	}
	ephemeral := failover.EphemeralConn()
	defer ephemeral.Close()
	if !ephemeral.shouldRetryCommand(dialErr, true) {
		t.Fatal("Sentinel EphemeralConn wrapper must retain its endpoint-retry policy")
	}
	tx := failover.newTx()
	defer tx.Close(context.Background())
	if !tx.shouldRetryCommand(dialErr, true) {
		t.Fatal("Sentinel Watch transaction must retain its endpoint-retry policy")
	}

	fd := &fdEngine{client: failover}
	if !fd.canRetryLease(dialErr, 0) {
		t.Fatal("Sentinel full-duplex engine must retry an exhausted dial against another endpoint")
	}
	if fd.canRetryLease(dialErr, 2) {
		t.Fatal("full-duplex lease must respect its retry budget")
	}
	fd.ownerRetries = true
	fd.reroute = func(context.Context, []Cmder, int) error { return nil }
	if fd.canRetryLease(dialErr, 0) {
		t.Fatal("full-duplex lease should defer to an owner's reroute policy")
	}
	fd.ownerRetries = false
	fd.reroute = nil
	fd.client = client
	if fd.canRetryLease(dialErr, 0) {
		t.Fatal("fixed-address full-duplex engine must not restart an exhausted dial sequence")
	}
}

func TestRingDoesNotRestartPoolDialSequence(t *testing.T) {
	var calls atomic.Int32
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"shard": "127.0.0.1:0"},
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			calls.Add(1)
			return nil, &net.OpError{Op: "dial", Err: context.DeadlineExceeded}
		},
		DialerRetries:      1,
		MaxRetries:         3,
		DialTimeout:        100 * time.Millisecond,
		PoolSize:           8,
		HeartbeatFrequency: time.Hour,
		HeartbeatFn:        func(context.Context, *Client) bool { return true },
	})
	defer ring.Close()
	if err := ring.Get(context.Background(), "key").Err(); err == nil {
		t.Fatal("expected a dial failure")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("ring restarted an exhausted pool dial sequence: %d calls", got)
	}
}

type fixedRingShard string

func (h fixedRingShard) Get(string) string { return string(h) }

func TestRingRetriesAfterShardRebalance(t *testing.T) {
	var ring *Ring
	var badDials, goodDials atomic.Int32
	ring = NewRing(&RingOptions{
		Addrs: map[string]string{"bad": "bad:6379", "good": "good:6379"},
		NewConsistentHash: func(shards []string) ConsistentHash {
			for _, shard := range shards {
				if shard == "bad" {
					return fixedRingShard("bad")
				}
			}
			return fixedRingShard("good")
		},
		Dialer: func(_ context.Context, _, addr string) (net.Conn, error) {
			if addr == "bad:6379" {
				badDials.Add(1)
				ring.sharding.mu.Lock()
				for range 3 {
					ring.sharding.shards.m["bad"].Vote(false)
				}
				ring.sharding.rebalanceLocked()
				ring.sharding.mu.Unlock()
			} else {
				goodDials.Add(1)
			}
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
		},
		DialerRetries:      1,
		MaxRetries:         2,
		DialTimeout:        100 * time.Millisecond,
		PoolSize:           8,
		HeartbeatFrequency: time.Hour,
		HeartbeatFn:        func(context.Context, *Client) bool { return true },
	})
	defer ring.Close()

	if err := ring.Get(context.Background(), "key").Err(); err == nil {
		t.Fatal("expected a dial failure")
	}
	if badDials.Load() != 1 || goodDials.Load() != 1 {
		t.Fatalf("dial attempts bad=%d good=%d, want one attempt per shard", badDials.Load(), goodDials.Load())
	}
}

func TestWrappedDialErrorKeepsNetworkMetrics(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"network", &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}, "NETWORK"},
		{"timeout", &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}, "TIMEOUT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, tc.err)
			kind, status, internal := classifyCommandError(wrapped)
			if kind != tc.want || status != tc.want || !internal {
				t.Fatalf("classifyCommandError(%v) = (%q, %q, %v), want %q", wrapped, kind, status, internal, tc.want)
			}
		})
	}
}

type scriptedRingHash struct{ picks atomic.Int32 }

func (h *scriptedRingHash) Get(string) string {
	if h.picks.Add(1) <= 2 {
		return "bad"
	}
	return "good"
}

func TestRingKeylessReselectsAfterExhaustedDial(t *testing.T) {
	var hash scriptedRingHash
	var badDials, goodDials atomic.Int32
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"bad": "bad:6379", "good": "good:6379"},
		NewConsistentHash: func([]string) ConsistentHash { return &hash },
		Dialer: func(_ context.Context, _, addr string) (net.Conn, error) {
			if addr == "bad:6379" {
				badDials.Add(1)
			} else {
				goodDials.Add(1)
			}
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
		},
		DialerRetries:      1,
		MaxRetries:         3,
		DialTimeout:        100 * time.Millisecond,
		PoolSize:           8,
		HeartbeatFrequency: time.Hour,
		HeartbeatFn:        func(context.Context, *Client) bool { return true },
	})
	defer ring.Close()

	if err := ring.Ping(context.Background()).Err(); err == nil {
		t.Fatal("expected a dial failure")
	}
	if badDials.Load() != 1 || goodDials.Load() != 1 {
		t.Fatalf("dial attempts bad=%d good=%d, want one per shard", badDials.Load(), goodDials.Load())
	}
}

func TestRingKeyedWaitsForRebalanceWithoutRedial(t *testing.T) {
	var hash scriptedRingHash
	var badDials, goodDials atomic.Int32
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"bad": "bad:6379", "good": "good:6379"},
		NewConsistentHash: func([]string) ConsistentHash { return &hash },
		Dialer: func(_ context.Context, _, addr string) (net.Conn, error) {
			if addr == "bad:6379" {
				badDials.Add(1)
			} else {
				goodDials.Add(1)
			}
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
		},
		DialerRetries:      1,
		MaxRetries:         3,
		DialTimeout:        100 * time.Millisecond,
		PoolSize:           8,
		HeartbeatFrequency: time.Hour,
		HeartbeatFn:        func(context.Context, *Client) bool { return true },
	})
	defer ring.Close()

	if err := ring.Get(context.Background(), "key").Err(); err == nil {
		t.Fatal("expected a dial failure")
	}
	if badDials.Load() != 1 || goodDials.Load() != 1 {
		t.Fatalf("dial attempts bad=%d good=%d, want one per shard", badDials.Load(), goodDials.Load())
	}
}

type alternatingRingHash struct{ picks atomic.Int32 }

func (h *alternatingRingHash) Get(string) string {
	if h.picks.Add(1)%2 == 1 {
		return "bad"
	}
	return "good"
}

func TestRingDoesNotRedialPreviouslyFailedShard(t *testing.T) {
	var hash alternatingRingHash
	var badDials, goodDials atomic.Int32
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"bad": "bad:6379", "good": "good:6379"},
		NewConsistentHash: func([]string) ConsistentHash { return &hash },
		Dialer: func(_ context.Context, _, addr string) (net.Conn, error) {
			if addr == "bad:6379" {
				badDials.Add(1)
			} else {
				goodDials.Add(1)
			}
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
		},
		DialerRetries:      1,
		MaxRetries:         2,
		DialTimeout:        100 * time.Millisecond,
		PoolSize:           8,
		HeartbeatFrequency: time.Hour,
		HeartbeatFn:        func(context.Context, *Client) bool { return true },
	})
	defer ring.Close()

	if err := ring.Ping(context.Background()).Err(); err == nil {
		t.Fatal("expected a dial failure")
	}
	if badDials.Load() != 1 || goodDials.Load() != 1 {
		t.Fatalf("dial attempts bad=%d good=%d, want one per shard", badDials.Load(), goodDials.Load())
	}
}

func TestPoolDialDeadlineIsReroutable(t *testing.T) {
	poolErr := fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, context.DeadlineExceeded)
	if !shouldRetry(poolErr, false) {
		t.Fatal("pool DialTimeout must allow Cluster/MultiDB endpoint failover")
	}
	if shouldRetrySingleNode(poolErr, true) {
		t.Fatal("fixed-address client must not restart a completed dial budget")
	}
	if shouldRetry(context.DeadlineExceeded, true) {
		t.Fatal("an unmarked caller deadline must not trigger retries")
	}
	canceled := fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, context.Canceled)
	if shouldRetry(canceled, true) {
		t.Fatal("a canceled dial must not be retried")
	}
}

func TestCustomDialerCanRetryDifferentEndpoint(t *testing.T) {
	dial := func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	}
	poolErr := fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, context.DeadlineExceeded)
	fixed := NewClient(&Options{Dialer: dial, MaxRetries: 2})
	defer fixed.Close()
	if fixed.shouldRetryCommand(poolErr, true) {
		t.Fatal("fixed-address custom dialer must not restart a completed dial sequence")
	}
	switching := NewClient(&Options{Dialer: dial, DialerCanChangeAddress: true, MaxRetries: 2})
	defer switching.Close()
	if !switching.shouldRetryCommand(poolErr, true) {
		t.Fatal("opted-in custom dialer should retry another endpoint")
	}
	if !switching.clone().shouldRetryCommand(poolErr, true) {
		t.Fatal("clone lost the custom dialer retry policy")
	}
	conn := switching.Conn()
	defer conn.Close()
	if !conn.shouldRetryCommand(poolErr, true) {
		t.Fatal("Conn wrapper lost the custom dialer retry policy")
	}
	tx := switching.newTx()
	defer tx.Close(context.Background())
	if !tx.shouldRetryCommand(poolErr, true) {
		t.Fatal("Watch wrapper lost the custom dialer retry policy")
	}
	fd := &fdEngine{client: switching}
	if !fd.canRetryLease(poolErr, 0) {
		t.Fatal("full-duplex lease must retain the endpoint-change retry policy")
	}
}

func TestSentinelClientCustomDialerRetryPolicy(t *testing.T) {
	poolErr := fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, context.DeadlineExceeded)
	client := NewSentinelClient(&Options{
		Addr: "127.0.0.1:0", DialerCanChangeAddress: true,
		DialerRetries: 1, MaxRetries: 2,
	})
	defer client.Close()
	if !client.shouldRetryCommand(poolErr, true) {
		t.Fatal("SentinelClient lost the opted-in endpoint-change retry policy")
	}

	fixed := NewSentinelClient(&Options{
		Addr: "127.0.0.1:0", DialerRetries: 1, MaxRetries: 2,
	})
	defer fixed.Close()
	if fixed.shouldRetryCommand(poolErr, true) {
		t.Fatal("fixed-address SentinelClient must not restart an exhausted dial sequence")
	}
}

type clusterPoolDialFailHook struct{ calls *atomic.Int32 }

func (h *clusterPoolDialFailHook) DialHook(next DialHook) DialHook { return next }
func (h *clusterPoolDialFailHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}
func (h *clusterPoolDialFailHook) ProcessHook(ProcessHook) ProcessHook {
	return func(context.Context, Cmder) error {
		h.calls.Add(1)
		return fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, context.DeadlineExceeded)
	}
}

func TestClusterDoesNotRedialExhaustedNode(t *testing.T) {
	var calls atomic.Int32
	cluster := NewClusterClient(&ClusterOptions{
		Addrs: []string{"127.0.0.1:6379"},
		MaxRedirects: 3,
		DisableRoutingPolicies: true,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{
				Start: 0, End: 16383,
				Nodes: []ClusterNode{{Addr: "127.0.0.1:6379"}},
			}}, nil
		},
	})
	defer cluster.Close()
	cluster.OnNewNode(func(client *Client) {
		client.AddHook(&clusterPoolDialFailHook{calls: &calls})
	})

	err := cluster.Get(context.Background(), "test").Err()
	if !errors.Is(err, pool.ErrDialSequenceFailure) {
		t.Fatalf("expected exhausted pool dial marker, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("cluster sent %d commands to the same exhausted node, want 1", got)
	}
}

func TestRingChangingDialerRetainsOuterRetries(t *testing.T) {
	var calls atomic.Int32
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"shard": "127.0.0.1:0"},
		NewClient: func(opt *Options) *Client {
			opt.DialerCanChangeAddress = true
			return NewClient(opt)
		},
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			calls.Add(1)
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
		},
		DialerRetries:      1,
		MaxRetries:         2,
		DialTimeout:        100 * time.Millisecond,
		PoolSize:           8,
		HeartbeatFrequency: time.Hour,
		HeartbeatFn:        func(context.Context, *Client) bool { return true },
	})
	defer ring.Close()

	if err := ring.Get(context.Background(), "key").Err(); err == nil {
		t.Fatal("expected a dial failure")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("changing dialer made %d attempts, want 3", got)
	}
}

func TestClusterChangingDialerRetainsOuterRetries(t *testing.T) {
	var calls atomic.Int32
	cluster := NewClusterClient(&ClusterOptions{
		Addrs: []string{"127.0.0.1:6379"},
		NewClient: func(opt *Options) *Client {
			opt.DialerCanChangeAddress = true
			return NewClient(opt)
		},
		MaxRedirects:          2,
		DisableRoutingPolicies: true,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{
				Start: 0, End: 16383,
				Nodes: []ClusterNode{{Addr: "127.0.0.1:6379"}},
			}}, nil
		},
	})
	defer cluster.Close()
	cluster.OnNewNode(func(client *Client) {
		client.AddHook(&clusterPoolDialFailHook{calls: &calls})
	})

	err := cluster.Get(context.Background(), "test").Err()
	if !errors.Is(err, pool.ErrDialSequenceFailure) {
		t.Fatalf("expected exhausted dial marker, got %v", err)
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("changing dialer retried %d times, want more than one", got)
	}
}

func TestUniversalClusterDialerRetryPolicy(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		opt := (&UniversalOptions{
			Addrs:                  []string{"127.0.0.1:6379", "127.0.0.1:6380"},
			DialerCanChangeAddress: enabled,
		}).Cluster()
		if opt.DialerCanChangeAddress != enabled {
			t.Fatalf("cluster option lost endpoint-change policy: enabled=%v", enabled)
		}
		if got := opt.clientOptions().DialerCanChangeAddress; got != enabled {
			t.Fatalf("node option lost endpoint-change policy: got=%v want=%v", got, enabled)
		}
	}
}

func TestClusterPipelinesDoNotRestartFixedNodeDialBudget(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		name := "pipeline"
		if transaction {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			c := NewClusterClient(&ClusterOptions{
				Addrs: []string{"127.0.0.1:6379"},
				DisableRoutingPolicies: true,
				MaxRedirects:           3,
				DialerRetries:          1,
				PipelinePoolSize:       -1,
				Dialer: func(context.Context, string, string) (net.Conn, error) {
					calls.Add(1)
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
				},
				ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
					return []ClusterSlot{{
						Start: 0, End: 16383,
						Nodes: []ClusterNode{{Addr: "127.0.0.1:6379"}},
					}}, nil
				},
			})
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			enqueue := func(p Pipeliner) error {
				p.Get(ctx, "key")
				return nil
			}
			var err error
			if transaction {
				_, err = c.TxPipelined(ctx, enqueue)
			} else {
				_, err = c.Pipelined(ctx, enqueue)
			}
			if !errors.Is(err, pool.ErrDialSequenceFailure) {
				t.Fatalf("expected exhausted pool dial error, got %v", err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("%s redialed exhausted fixed node %d times; want 1", name, got)
			}
		})
	}
}


type countingFixedRingHash struct{ picks *atomic.Int32 }

func (h *countingFixedRingHash) Get(string) string {
	h.picks.Add(1)
	return "shard"
}

type cancelledDialReviewHook struct{ calls *atomic.Int32 }

func (h *cancelledDialReviewHook) DialHook(next DialHook) DialHook { return next }
func (h *cancelledDialReviewHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}
func (h *cancelledDialReviewHook) ProcessHook(ProcessHook) ProcessHook {
	return func(context.Context, Cmder) error {
		h.calls.Add(1)
		return fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, context.Canceled)
	}
}

func TestRingStopsReroutingCanceledPoolDial(t *testing.T) {
	var calls, picks atomic.Int32
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"shard": "127.0.0.1:6379"},
		NewConsistentHash: func([]string) ConsistentHash {
			return &countingFixedRingHash{picks: &picks}
		},
		NewClient: func(opt *Options) *Client {
			client := NewClient(opt)
			client.AddHook(&cancelledDialReviewHook{calls: &calls})
			return client
		},
		MaxRetries: 3, PoolSize: 8, HeartbeatFrequency: time.Hour,
		HeartbeatFn: func(context.Context, *Client) bool { return true },
	})
	defer ring.Close()

	err := ring.Get(context.Background(), "key").Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled error, got %v", err)
	}
	if calls.Load() != 1 || picks.Load() != 1 {
		t.Fatalf("canceled dial retried: calls=%d shard selections=%d", calls.Load(), picks.Load())
	}
}

func TestClusterRoutingStopsFixedDialRetries(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := "multi-slot"
		if parallel {
			name = "parallel-target"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			const addr = "127.0.0.1:6379"
			cluster := NewClusterClient(&ClusterOptions{
				Addrs: []string{addr}, MaxRedirects: 3,
				ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
					return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
				},
			})
			defer cluster.Close()
			cluster.OnNewNode(func(client *Client) {
				client.AddHook(&clusterPoolDialFailHook{calls: &calls})
			})
			ctx := context.Background()
			cmd := NewCmd(ctx, "get", "key")
			var err error
			if parallel {
				var node *clusterNode
				node, err = cluster.nodes.GetOrCreate(addr)
				if err != nil {
					t.Fatal(err)
				}
				err = cluster.executeParallelTarget(ctx, node, -1, cmd)
			} else {
				decision := cluster.commandRoutingDecision(ctx, cmd)
				err = cluster.executeMultiSlotCommand(ctx, cmd, decision.naturalSlot, &decision)
			}
			if !errors.Is(err, pool.ErrDialSequenceFailure) {
				t.Fatalf("want exhausted dial marker, got %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("restarted fixed target dial %d times", calls.Load())
			}
		})
	}
}


func TestClusterWatchDoesNotRestartFixedDialBudget(t *testing.T) {
	var calls atomic.Int32
	cluster := NewClusterClient(&ClusterOptions{
		Addrs: []string{"127.0.0.1:6379"},
		MaxRedirects: 3,
		DialerRetries: 1,
		DialTimeout: 50 * time.Millisecond,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			calls.Add(1)
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
		},
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{
				Start: 0, End: 16383,
				Nodes: []ClusterNode{{Addr: "127.0.0.1:6379"}},
			}}, nil
		},
	})
	defer cluster.Close()
	err := cluster.Watch(context.Background(), func(*Tx) error { return nil }, "key")
	if !errors.Is(err, pool.ErrDialSequenceFailure) {
		t.Fatalf("expected exhausted dial marker, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("WATCH restarted an exhausted fixed endpoint: %d dials, want 1", got)
	}
}

func TestClusterParallelTargetSkipsFinalDiscovery(t *testing.T) {
	const addr = "127.0.0.1:6379"
	var dials, reloads atomic.Int32
	cluster := NewClusterClient(&ClusterOptions{
		Addrs:                      []string{addr},
		MaxRedirects:               -1, // One attempt: no later target can use a refreshed topology.
		ClusterStateReloadInterval: time.Hour,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			reloads.Add(1)
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
	})
	defer cluster.Close()
	cluster.OnNewNode(func(client *Client) {
		client.AddHook(&clusterPoolDialFailHook{calls: &dials})
	})

	ctx := context.Background()
	if _, err := cluster.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	node, err := cluster.nodes.GetOrCreate(addr)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.executeParallelTarget(ctx, node, 1, NewCmd(ctx, "get", "key"))
	if !errors.Is(err, pool.ErrDialSequenceFailure) {
		t.Fatalf("expected exhausted dial marker, got %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("target attempted %d dials, want 1", got)
	}
	if got := reloads.Load(); got != 1 {
		t.Fatalf("final exhausted target triggered %d topology loads, want initial load only", got)
	}
}

func TestClusterProcessSchedulesReloadAfterExhaustedDial(t *testing.T) {
	const addr = "127.0.0.1:6379"
	var dials, reloads atomic.Int32
	reloaded := make(chan struct{}, 1)
	cluster := NewClusterClient(&ClusterOptions{
		Addrs:                      []string{addr},
		MaxRedirects:               2,
		DisableRoutingPolicies:     true,
		ClusterStateReloadInterval: time.Hour,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			if reloads.Add(1) > 1 {
				select {
				case reloaded <- struct{}{}:
				default:
				}
			}
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
	})
	defer cluster.Close()
	cluster.OnNewNode(func(client *Client) {
		client.AddHook(&clusterPoolDialFailHook{calls: &dials})
	})

	ctx := context.Background()
	if _, err := cluster.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	err := cluster.Get(ctx, "key").Err()
	if !errors.Is(err, pool.ErrDialSequenceFailure) {
		t.Fatalf("expected exhausted dial marker, got %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("fixed node attempted %d dials, want 1", got)
	}
	select {
	case <-reloaded:
	case <-time.After(2 * time.Second):
		t.Fatal("cluster did not schedule a topology reload after dial exhaustion")
	}
}

func TestClusterBroadcastKeepsOriginalShardTarget(t *testing.T) {
	const badAddr, otherAddr = "127.0.0.1:6381", "127.0.0.1:6382"
	var badCalls, otherCalls, reloads atomic.Int32
	cluster := NewClusterClient(&ClusterOptions{
		Addrs:                      []string{badAddr, otherAddr},
		MaxRedirects:               2,
		ClusterStateReloadInterval: time.Hour,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			if reloads.Add(1) == 1 {
				return []ClusterSlot{
					{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: badAddr}}},
					{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: otherAddr}}},
				}, nil
			}
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: otherAddr}}}}, nil
		},
	})
	defer cluster.Close()
	cluster.OnNewNode(func(client *Client) {
		calls := &badCalls
		if client.opt.Addr == otherAddr {
			calls = &otherCalls
		}
		client.AddHook(&clusterPoolDialFailHook{calls: calls})
	})

	ctx := context.Background()
	if _, err := cluster.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	badNode, err := cluster.nodes.GetOrCreate(badAddr)
	if err != nil {
		t.Fatal(err)
	}
	// After scale-in the old representative slot points at another original
	// broadcast target, not a replacement for the failed shard.
	if _, err := cluster.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	err = cluster.executeParallelTarget(ctx, badNode, 0, NewCmd(ctx, "dbsize"))
	if !errors.Is(err, pool.ErrDialSequenceFailure) {
		t.Fatalf("expected original shard dial error, got %v", err)
	}
	if badCalls.Load() != 1 || otherCalls.Load() != 0 {
		t.Fatalf("broadcast rerouted a failed shard: original=%d duplicate=%d", badCalls.Load(), otherCalls.Load())
	}
}

type canceledClusterPipelineReviewHook struct{}

func (canceledClusterPipelineReviewHook) DialHook(next DialHook) DialHook { return next }
func (canceledClusterPipelineReviewHook) ProcessHook(next ProcessHook) ProcessHook { return next }
func (canceledClusterPipelineReviewHook) ProcessPipelineHook(ProcessPipelineHook) ProcessPipelineHook {
	return func(context.Context, []Cmder) error {
		return fmt.Errorf("%w: %w", pool.ErrDialSequenceFailure, context.Canceled)
	}
}

func TestCanceledClusterPipelineDoesNotFailNode(t *testing.T) {
	const addr = "127.0.0.1:6379"
	cluster := NewClusterClient(&ClusterOptions{
		Addrs: []string{addr}, MaxRedirects: 2,
		ClusterStateReloadInterval: time.Hour,
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
	})
	defer cluster.Close()
	cluster.OnNewNode(func(client *Client) {
		client.AddHook(canceledClusterPipelineReviewHook{})
	})
	ctx := context.Background()
	if _, err := cluster.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	node, err := cluster.nodes.GetOrCreate(addr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cluster.Pipelined(ctx, func(p Pipeliner) error {
		p.Get(ctx, "key")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled dial error, got %v", err)
	}
	if node.failing.Load() != 0 {
		t.Fatal("canceled pool dial incorrectly marked a healthy cluster node as failing")
	}
}
