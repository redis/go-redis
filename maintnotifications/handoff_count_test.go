package maintnotifications

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestFailedHandoffUncountsParkedConnAsIdle(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Name: "handoff-count",
		Dialer: func(context.Context) (net.Conn, error) {
			c, _ := net.Pipe()
			return c, nil
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })

	failingDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, errors.New("dial failed")
	}
	processor := NewPoolHook(failingDialer, "tcp", &Config{
		Mode:              ModeAuto,
		EndpointType:      EndpointTypeAuto,
		MaxWorkers:        1,
		HandoffQueueSize:  10,
		MaxHandoffRetries: 1,
		HandoffTimeout:    100 * time.Millisecond,
	}, nil)
	processor.SetPool(connPool)
	connPool.AddPoolHook(processor)
	t.Cleanup(func() { _ = processor.Shutdown(context.Background()) })

	ctx := context.Background()
	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
		t.Fatal(err)
	}
	// The handoff is queued and the connection is parked in idleConns until it fails.
	connPool.Put(ctx, cn)

	deadline := time.Now().Add(3 * time.Second)
	for connPool.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := connPool.Len(); got != 0 {
		t.Fatalf("pool length after failed handoff: got %d, want 0", got)
	}
	// The parked conn must leave idleConns too, or the idle count stays at 1.
	if s := connPool.Stats(); s.TotalConns != 0 || s.IdleConns != 0 {
		t.Fatalf("stats after failed handoff: total=%d idle=%d, want 0 and 0", s.TotalConns, s.IdleConns)
	}
}
