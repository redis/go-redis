package redis

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestPubSubOnConnectPanicFailsSubscribe pins that a panic in OnConnect
// while the shared pub/sub connection is dialed (on the manager's
// goroutine) fails the dial instead of crashing the process: the
// subscribe reports it, the half-initialized connection is closed and
// untracked, and the Limiter admission is settled.
func TestPubSubOnConnectPanicFailsSubscribe(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	client := NewClient(&Options{
		Addr:          internalTestRedisAddr(),
		Protocol:      3,
		DialerRetries: 1,
		Limiter:       br,
		OnConnect: func(context.Context, *Conn) error {
			panic("test: OnConnect panic on the shared pub/sub connection")
		},
	})
	defer client.Close()

	ps := client.Subscribe(ctx, "dialpanic:ch")
	defer ps.Close()

	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := ps.Receive(rctx)
	if err == nil || !strings.Contains(err.Error(), "dial panicked") {
		t.Fatalf("Receive err = %v, want the dial panic", err)
	}
	if active := client.pubSubPool.Stats().Active; active != 0 {
		t.Fatalf("pub/sub connections tracked after the panic = %d, want 0", active)
	}
	if allows, reports := br.allows.Load(), br.reports.Load(); allows == 0 || allows != reports {
		t.Fatalf("Limiter: %d Allow vs %d ReportResult, want equal and > 0", allows, reports)
	}
}
