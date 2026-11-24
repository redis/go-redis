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

// pubSubConnCounter registers pools, the way the connection-count metric
// reads them; every Record method is a no-op.
type pubSubConnCounter struct {
	mu     sync.Mutex
	pubsub map[string]PubSubPooler
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

func (r *pubSubConnCounter) RecordConnectionCount(context.Context, int, ConnInfo, string, bool) {}

func (r *pubSubConnCounter) RegisterPool(string, Pooler) {}
func (r *pubSubConnCounter) UnregisterPool(Pooler)       {}

func (r *pubSubConnCounter) RegisterPubSubPool(name string, p PubSubPooler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pubsub == nil {
		r.pubsub = make(map[string]PubSubPooler)
	}
	r.pubsub[name] = p
}

func (r *pubSubConnCounter) UnregisterPubSubPool(p PubSubPooler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, q := range r.pubsub {
		if q == p {
			delete(r.pubsub, name)
		}
	}
}

// active sums Stats().Active over the registered pub/sub pools of the
// client at addr (SetOTelRecorder also hands over other live clients'
// pools).
func (r *pubSubConnCounter) active(addr string) (count int, names []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, p := range r.pubsub {
		if !strings.HasPrefix(name, addr+"_") {
			continue
		}
		count += int(p.Stats().Active)
		names = append(names, name)
	}
	return count, names
}

// TestPubSubConnCountsInOTel pins that the shared pub/sub connection is
// counted for db.client.connection.count: the client registers its
// "_pubsub" pool, whose Active stat is 1 while the connection lives, and
// unregisters it on Close.
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
	addr := client.Options().Addr
	count, names := rec.active(addr)
	if count != 1 || len(names) != 1 || !strings.HasSuffix(names[0], "_pubsub") {
		t.Fatalf("while subscribed: active %d, pools %v; want 1 under a _pubsub pool", count, names)
	}

	_ = ps.Close()
	_ = client.Close()
	if count, names := rec.active(addr); count != 0 || len(names) != 0 {
		t.Fatalf("after Close: active %d, pools %v; want the pool unregistered", count, names)
	}
}
