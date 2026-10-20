package redis

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// pubSubConnCounter records the pub/sub series of the connection-count
// metric; every other recorder method is a no-op.
type pubSubConnCounter struct {
	mu    sync.Mutex
	count int
	pools []string
}

func (r *pubSubConnCounter) RecordOperationDuration(context.Context, time.Duration, Cmder, int, error, ConnInfo, int) {
}

func (r *pubSubConnCounter) RecordPipelineOperationDuration(context.Context, time.Duration, string, int, int, error, ConnInfo, int) {
}
func (r *pubSubConnCounter) RecordConnectionCreateTime(context.Context, time.Duration, ConnInfo) {}
func (r *pubSubConnCounter) RecordConnectionRelaxedTimeout(context.Context, int, ConnInfo, string, string) {
}
func (r *pubSubConnCounter) RecordConnectionHandoff(context.Context, ConnInfo, string)           {}
func (r *pubSubConnCounter) RecordError(context.Context, string, ConnInfo, string, bool, int)    {}
func (r *pubSubConnCounter) RecordMaintenanceNotification(context.Context, ConnInfo, string)     {}
func (r *pubSubConnCounter) RecordConnectionWaitTime(context.Context, time.Duration, ConnInfo)   {}
func (r *pubSubConnCounter) RecordConnectionClosed(context.Context, ConnInfo, string, error)     {}
func (r *pubSubConnCounter) RecordPubSubMessage(context.Context, ConnInfo, string, string, bool) {}
func (r *pubSubConnCounter) RecordStreamLag(context.Context, time.Duration, ConnInfo, string, string, string) {
}
func (r *pubSubConnCounter) RecordPendingRequests(context.Context, int, ConnInfo, string) {}

func (r *pubSubConnCounter) RecordConnectionCount(_ context.Context, delta int, cn ConnInfo, _ string, isPubSub bool) {
	if !isPubSub {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count += delta
	r.pools = append(r.pools, cn.PoolName())
}

func (r *pubSubConnCounter) snapshot() (int, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count, append([]string(nil), r.pools...)
}

// TestPubSubConnCountsInOTel pins that the shared pub/sub connection is
// counted in db.client.connection.count (isPubSub, under the client's
// "_pubsub" pool name) while it lives, and uncounted once it closes.
func TestPubSubConnCountsInOTel(t *testing.T) {
	rec := &pubSubConnCounter{}
	SetOTelRecorder(rec)
	t.Cleanup(func() { SetOTelRecorder(nil) })

	var reject atomic.Bool
	client := NewClient(&Options{
		Addr:            listenSubscribeServer(t, &reject),
		Protocol:        2,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	ctx := context.Background()
	ps := client.Subscribe(ctx, "ch")
	if _, err := ps.ReceiveTimeout(ctx, 5*time.Second); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	count, pools := rec.snapshot()
	if count != 1 || len(pools) != 1 || !strings.HasSuffix(pools[0], "_pubsub") {
		t.Fatalf("while subscribed: count %d, pools %v; want 1 under a _pubsub pool", count, pools)
	}

	_ = ps.Close()
	_ = client.Close()
	if count, pools := rec.snapshot(); count != 0 {
		t.Fatalf("after Close: count %d (pools %v), want 0", count, pools)
	}
}
