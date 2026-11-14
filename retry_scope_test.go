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
