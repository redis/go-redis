package redis

import (
	"context"
	"errors"
	"testing"
)

// TestHandshakeDoesNotConsumeLimiter pins that the connection handshake
// (HELLO, AUTH, SELECT, CLIENT SETINFO, CLIENT MAINT_NOTIFICATIONS) is not
// admitted or reported against Options.Limiter on its own: the command that
// dials the connection is the one operation the Limiter sees.
func TestHandshakeDoesNotConsumeLimiter(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	client := NewClient(&Options{
		Addr:         internalTestRedisAddr(),
		Protocol:     3,
		DB:           1,
		PoolSize:     1,
		MinIdleConns: 0,
		Limiter:      br,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if got := br.allows.Load(); got != 1 {
		t.Fatalf("first command on a fresh connection: Limiter.Allow called %d times, want 1 (the handshake must not be admitted on its own)", got)
	}
	if got := br.reports.Load(); got != 1 {
		t.Fatalf("first command on a fresh connection: Limiter.ReportResult called %d times, want 1", got)
	}
	if got := br.errReports.Load(); got != 0 {
		t.Fatalf("handshake succeeded but %d failures were reported", got)
	}

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("second ping: %v", err)
	}
	if got := br.allows.Load(); got != 2 {
		t.Fatalf("second command: Limiter.Allow called %d times in total, want 2", got)
	}
}

// TestHandshakeFailureReachesLimiter pins that a failed handshake (here a
// SELECT on a database index the server does not have) is still reported,
// once, as the failure of the command that dialed the connection.
func TestHandshakeFailureReachesLimiter(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	client := NewClient(&Options{
		Addr:         internalTestRedisAddr(),
		Protocol:     3,
		DB:           1 << 20,
		PoolSize:     1,
		MinIdleConns: 0,
		MaxRetries:   -1,
		Limiter:      br,
	})
	defer client.Close()

	err := client.Ping(ctx).Err()
	if err == nil {
		t.Skip("server accepts database index 1<<20; cannot provoke a handshake failure")
	}
	if errors.Is(err, errBreakerOpen) {
		t.Fatalf("ping failed with the breaker's own error: %v", err)
	}
	if got := br.allows.Load(); got != 1 {
		t.Fatalf("Limiter.Allow called %d times, want 1", got)
	}
	if got := br.reports.Load(); got != 1 {
		t.Fatalf("Limiter.ReportResult called %d times, want 1", got)
	}
	if got := br.errReports.Load(); got != 1 {
		t.Fatalf("the handshake failure was reported %d times, want 1", got)
	}
}

// TestPubSubConnConsumesLimiterOnce pins that a PubSub connection, which
// dials outside getConnLimited, is admitted once and reported once.
func TestPubSubConnConsumesLimiterOnce(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	client := NewClient(&Options{
		Addr:         internalTestRedisAddr(),
		Protocol:     3,
		PoolSize:     1,
		MinIdleConns: 0,
		Limiter:      br,
	})
	defer client.Close()

	pubsub := client.Subscribe(ctx, "limiter-handshake")
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if got := br.allows.Load(); got != 1 {
		t.Fatalf("PubSub connection: Limiter.Allow called %d times, want 1", got)
	}
	if got := br.reports.Load(); got != 1 {
		t.Fatalf("PubSub connection: Limiter.ReportResult called %d times, want 1", got)
	}
	if got := br.errReports.Load(); got != 0 {
		t.Fatalf("handshake succeeded but %d failures were reported", got)
	}
}

// TestPubSubConnDeniedByLimiter pins that an open breaker still blocks a
// PubSub connection before the dial.
func TestPubSubConnDeniedByLimiter(t *testing.T) {
	ctx := context.Background()
	br := &fakeBreaker{}
	br.open.Store(true)
	client := NewClient(&Options{
		Addr:         "127.0.0.1:1",
		Protocol:     3,
		PoolSize:     1,
		MinIdleConns: 0,
		Limiter:      br,
	})
	defer client.Close()

	pubsub := client.Subscribe(ctx, "limiter-handshake")
	defer pubsub.Close()
	_, err := pubsub.Receive(ctx)
	if !errors.Is(err, errBreakerOpen) {
		t.Fatalf("Receive error = %v, want %v", err, errBreakerOpen)
	}
	if got := br.allows.Load(); got != 0 {
		t.Fatalf("open breaker: Limiter.Allow admitted %d times, want 0", got)
	}
	if got := br.reports.Load(); got != 0 {
		t.Fatalf("open breaker: Limiter.ReportResult called %d times, want 0 (a denial is not reported)", got)
	}
}
