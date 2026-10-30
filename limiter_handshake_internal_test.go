package redis

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
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

// TestReconnectInitConnConsumesLimiterOnce pins that a reconnect through the
// stored init function (a maintenance handoff) is admitted once and reported
// once.
func TestReconnectInitConnConsumesLimiterOnce(t *testing.T) {
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

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	cn, err := client.connPool.Get(ctx)
	if err != nil {
		t.Fatalf("get conn: %v", err)
	}
	client.connPool.Put(ctx, cn)
	netConn, err := net.Dial("tcp", internalTestRedisAddr())
	if err != nil {
		t.Skipf("no redis: %v", err)
	}
	allows, reports := br.allows.Load(), br.reports.Load()
	if err := cn.SetNetConnAndInitConn(ctx, netConn); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if got := br.allows.Load() - allows; got != 1 {
		t.Fatalf("reconnect: Limiter.Allow called %d times, want 1", got)
	}
	if got := br.reports.Load() - reports; got != 1 {
		t.Fatalf("reconnect: Limiter.ReportResult called %d times, want 1", got)
	}
	if got := br.errReports.Load(); got != 0 {
		t.Fatalf("reconnect succeeded but %d failures were reported", got)
	}
}

// TestFullDuplexInitConsumesLimiterOnce pins that the handshake of a fresh
// full-duplex session is admitted once and reported once, apart from the
// per-chunk admission.
func TestFullDuplexInitConsumesLimiterOnce(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	client := NewClient(&Options{
		Addr:             internalTestRedisAddr(),
		Protocol:         3,
		PipelinePoolSize: 1,
		PoolSize:         1,
		MinIdleConns:     0,
		Limiter:          br,
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	before := br.allows.Load()
	if err := ap.Set(ctx, "fd:limiter:init", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := br.allows.Load() - before; got != 2 {
		t.Fatalf("first full-duplex command: Limiter.Allow called %d times, want 2 (one handshake, one chunk)", got)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if a, r := br.allows.Load(), br.reports.Load(); a != r {
		t.Fatalf("Limiter unpaired: allows=%d reports=%d", a, r)
	}
	if got := br.errReports.Load(); got != 0 {
		t.Fatalf("handshake succeeded but %d failures were reported", got)
	}
}

// TestReconnectDeniedByLimiter pins that a reconnect an open breaker denies
// leaves the connection CLOSED, the same state as a failed handshake, so no
// waiter stalls on an INITIALIZING connection.
func TestReconnectDeniedByLimiter(t *testing.T) {
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

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	cn, err := client.connPool.Get(ctx)
	if err != nil {
		t.Fatalf("get conn: %v", err)
	}
	client.connPool.Put(ctx, cn)
	netConn, err := net.Dial("tcp", internalTestRedisAddr())
	if err != nil {
		t.Skipf("no redis: %v", err)
	}
	defer netConn.Close()
	reports := br.reports.Load()
	br.open.Store(true)
	err = cn.SetNetConnAndInitConn(ctx, netConn)
	if !errors.Is(err, errBreakerOpen) {
		t.Fatalf("reconnect error = %v, want %v", err, errBreakerOpen)
	}
	if got := cn.GetStateMachine().GetState(); got != pool.StateClosed {
		t.Fatalf("connection state after a denied reconnect = %s, want %s", got, pool.StateClosed)
	}
	if got := br.reports.Load() - reports; got != 0 {
		t.Fatalf("denied reconnect: Limiter.ReportResult called %d times, want 0", got)
	}
}

// TestFullDuplexInitPanicSettlesLimiter pins that a panic in the handshake of
// a fresh full-duplex session still pairs its admission with one report.
func TestFullDuplexInitPanicSettlesLimiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	br := &fakeBreaker{}
	cn := pool.NewConn(&fdPanicInitConn{})
	p := &fdRecordPool{conn: cn}
	fd := &fdEngine{
		ap: &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: ctx},
		client: &Client{baseClient: &baseClient{opt: &Options{
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
			Limiter:      br,
		}}},
		pool:     p,
		maxBatch: 1,
		window:   100,
	}
	carry := []fdReq{{cmd: NewStatusCmd(ctx, "get", "k"), attempts: 1}}

	_, result, aerr := fd.attempt(context.Background(), carry)

	if result != fdLeaseErr || !errors.Is(aerr, errFDPanicRecovered) {
		t.Fatalf("result = %v, aerr = %v; want fdLeaseErr from the attempt recover", result, aerr)
	}
	if got := br.allows.Load(); got != 1 {
		t.Fatalf("Limiter.Allow called %d times, want 1", got)
	}
	if got := br.reports.Load(); got != 1 {
		t.Fatalf("Limiter.ReportResult called %d times, want 1 (the admission must settle on panic)", got)
	}
	if got := br.errReports.Load(); got != 1 {
		t.Fatalf("the init panic was reported as %d failures, want 1", got)
	}
}

func recoverPanic(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

// TestReconnectInitPanicSettlesLimiter pins that a panic in the init of a
// reconnect still pairs its admission with one report.
func TestReconnectInitPanicSettlesLimiter(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	var panicOnConnect atomic.Bool
	client := NewClient(&Options{
		Addr:         internalTestRedisAddr(),
		Protocol:     3,
		PoolSize:     1,
		MinIdleConns: 0,
		Limiter:      br,
		OnConnect: func(context.Context, *Conn) error {
			if panicOnConnect.Load() {
				panic("test: OnConnect panic on reconnect")
			}
			return nil
		},
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	cn, err := client.connPool.Get(ctx)
	if err != nil {
		t.Fatalf("get conn: %v", err)
	}
	client.connPool.Put(ctx, cn)
	netConn, err := net.Dial("tcp", internalTestRedisAddr())
	if err != nil {
		t.Skipf("no redis: %v", err)
	}
	defer netConn.Close()
	allows, reports := br.allows.Load(), br.reports.Load()
	panicOnConnect.Store(true)
	if recovered := recoverPanic(func() { _ = cn.SetNetConnAndInitConn(ctx, netConn) }); recovered == nil {
		t.Fatal("the OnConnect panic did not propagate")
	}
	if got := br.allows.Load() - allows; got != 1 {
		t.Fatalf("reconnect: Limiter.Allow called %d times, want 1", got)
	}
	if got := br.reports.Load() - reports; got != 1 {
		t.Fatalf("reconnect: Limiter.ReportResult called %d times, want 1 (the admission must settle on panic)", got)
	}
}

// TestPubSubInitPanicSettlesLimiter pins that a panic in the init of a PubSub
// connection still pairs its admission with one report.
func TestPubSubInitPanicSettlesLimiter(t *testing.T) {
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
		OnConnect: func(context.Context, *Conn) error {
			panic("test: OnConnect panic on a PubSub connection")
		},
	})
	defer client.Close()

	var pubsub *PubSub
	recovered := recoverPanic(func() {
		pubsub = client.Subscribe(ctx, "limiter-handshake")
		_, _ = pubsub.Receive(ctx)
	})
	if pubsub != nil {
		defer pubsub.Close()
	}
	if recovered == nil {
		t.Fatal("the OnConnect panic did not propagate")
	}
	if got := br.allows.Load(); got != 1 {
		t.Fatalf("PubSub connection: Limiter.Allow called %d times, want 1", got)
	}
	if got := br.reports.Load(); got != 1 {
		t.Fatalf("PubSub connection: Limiter.ReportResult called %d times, want 1 (the admission must settle on panic)", got)
	}
}
