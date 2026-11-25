package redis

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
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

func TestFullDuplexInitDenialDoesNotSpill(t *testing.T) {
	for _, tc := range []struct {
		name     string
		warmMain bool
	}{
		{name: "empty main pool"},
		{name: "warm main pool", warmMain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := newCSCPolicyServer(t, nil)
			br := &fakeBreaker{}
			var dials atomic.Int64
			client := NewClient(&Options{
				Addr:                     s.addr,
				Protocol:                 3,
				PoolSize:                 1,
				PipelinePoolSize:         1,
				MaxRetries:               -1,
				Limiter:                  br,
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, addr)
				},
			})
			defer client.Close()
			if tc.warmMain {
				if err := client.Ping(ctx).Err(); err != nil {
					t.Fatal(err)
				}
			}
			mainHook := &fdCountHook{}
			client.connPool.AddPoolHook(mainHook)
			beforeDials, beforeReports := dials.Load(), br.reports.Load()
			br.open.Store(true)
			ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()
			if ap.fd == nil {
				t.Fatal("full-duplex engine is not active")
			}
			if err := ap.Get(ctx, "key").Err(); err != errBreakerOpen {
				t.Fatalf("GET error = %v, want the original Limiter denial %v", err, errBreakerOpen)
			}
			if got := dials.Load() - beforeDials; got != 1 {
				t.Errorf("dial attempts = %d, want 1", got)
			}
			if got := mainHook.gets.Load(); got != 0 {
				t.Errorf("main-pool gets = %d, want 0", got)
			}
			if got := br.denies.Load(); got != 1 {
				t.Errorf("Limiter denials = %d, want 1", got)
			}
			if got := br.reports.Load() - beforeReports; got != 0 {
				t.Errorf("denial reported %d times, want 0", got)
			}
			if got := s.count("get"); got != 0 {
				t.Errorf("GET reached the server %d times after denial, want 0", got)
			}
			if got := client.getPipelinePool().Stats().TotalConns; got != 0 {
				t.Errorf("connections retained after denial = %d, want 0", got)
			}

			br.open.Store(false)
			if got, err := ap.Get(ctx, "key").Result(); err != nil || got != "v" {
				t.Fatalf("GET after Limiter recovery = %q, %v; want v, nil", got, err)
			}
		})
	}
}

func TestFullDuplexInitFailureStillSpills(t *testing.T) {
	for _, withLimiter := range []bool{false, true} {
		name := "without limiter"
		if withLimiter {
			name = "admitted init returns limiter error"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := newCSCPolicyServer(t, nil)
			br := &fakeBreaker{}
			var dials, inits atomic.Int64
			opt := &Options{
				Addr:                     s.addr,
				Protocol:                 3,
				PoolSize:                 1,
				PipelinePoolSize:         1,
				MaxRetries:               -1,
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, addr)
				},
				OnConnect: func(context.Context, *Conn) error {
					if inits.Add(1) == 1 {
						// The error value alone cannot identify an admission denial.
						return errBreakerOpen
					}
					return nil
				},
			}
			if withLimiter {
				opt.Limiter = br
			}
			client := NewClient(opt)
			defer client.Close()
			mainHook := &fdCountHook{}
			client.connPool.AddPoolHook(mainHook)
			ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()
			if ap.fd == nil {
				t.Fatal("full-duplex engine is not active")
			}
			if got, err := ap.Get(ctx, "key").Result(); err != nil || got != "v" {
				t.Fatalf("GET after init failure = %q, %v; want v, nil", got, err)
			}
			if err := ap.Close(); err != nil {
				t.Fatal(err)
			}
			if got := dials.Load(); got != 2 {
				t.Errorf("dial attempts = %d, want 2", got)
			}
			if got := mainHook.gets.Load(); got != 1 {
				t.Errorf("main-pool gets = %d, want 1", got)
			}
			if got := s.count("get"); got != 1 {
				t.Errorf("GET reached the server %d times, want 1", got)
			}
			if withLimiter {
				if a, r := br.allows.Load(), br.reports.Load(); a != 3 || r != 3 {
					t.Errorf("Limiter allows=%d reports=%d, want 3 each (two inits and one chunk)", a, r)
				}
				if got := br.errReports.Load(); got != 1 {
					t.Errorf("failed init reported %d times, want 1", got)
				}
			}
		})
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
