package redis

import (
	"context"
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
