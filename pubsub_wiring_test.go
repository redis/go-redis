package redis

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestSubscribeInitialDialErrorReachesReceive pins that the dial error of
// the subscribe Client.Subscribe runs — which it cannot return — is
// delivered to the PubSub, so the documented Subscribe-then-Receive
// pattern reports an unreachable address instead of blocking.
func TestSubscribeInitialDialErrorReachesReceive(t *testing.T) {
	// A port nothing listens on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	client := NewClient(&Options{
		Addr:          addr,
		DialerRetries: 1,
		DialTimeout:   200 * time.Millisecond,
	})
	defer client.Close()

	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		subscribe func() *PubSub
	}{
		{"Subscribe", func() *PubSub { return client.Subscribe(ctx, "ch") }},
		{"PSubscribe", func() *PubSub { return client.PSubscribe(ctx, "p*") }},
		{"SSubscribe", func() *PubSub { return client.SSubscribe(ctx, "sch") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps := tc.subscribe()
			defer ps.Close()

			msg, err := ps.ReceiveTimeout(ctx, 5*time.Second)
			if err == nil {
				t.Fatalf("Receive = %T, want the dial error", msg)
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() && strings.Contains(err.Error(), "receive timeout") {
				t.Fatal("Receive blocked until its timeout: the initial dial error was not delivered")
			}
			var opErr *net.OpError
			if !errors.As(err, &opErr) {
				t.Fatalf("Receive error = %v, want the dial's *net.OpError", err)
			}
		})
	}
}

// TestSubscribeInitialAuthErrorReachesReceiveThenRetries pins that a
// rejected handshake reaches the first Receive, and that delivering it
// keeps the registration: once the server accepts, the reconnect
// subscribes and the confirmation follows the error.
func TestSubscribeInitialAuthErrorReachesReceiveThenRetries(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	addr := listenSubscribeServer(t, &reject)

	client := NewClient(&Options{
		Addr:            addr,
		Password:        "wrong",
		Protocol:        2,
		DisableIdentity: true,
		DialerRetries:   1,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx := context.Background()
	ps := client.Subscribe(ctx, "ch")
	defer ps.Close()

	_, err := ps.ReceiveTimeout(ctx, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("first Receive error = %v, want the WRONGPASS handshake error", err)
	}

	reject.Store(false)
	for {
		ev, err := ps.ReceiveTimeout(ctx, 10*time.Second)
		if err != nil {
			if strings.Contains(err.Error(), "WRONGPASS") {
				continue // a retry that raced the flip; only the first failure is required
			}
			t.Fatalf("Receive after the server accepts: %v, want the reconnect's subscribe confirmation", err)
		}
		sub, ok := ev.(*Subscription)
		if !ok {
			continue
		}
		if sub.Kind != "subscribe" || sub.Channel != "ch" {
			t.Fatalf("confirmation = %+v, want subscribe ch", sub)
		}
		return
	}
}

// TestSubscribeCancelledContextStillSubscribes pins that Client.Subscribe
// with a context already done keeps the channels: Receive reports the
// cancellation, and the dial it started subscribes them.
func TestSubscribeCancelledContextStillSubscribes(t *testing.T) {
	var reject atomic.Bool
	client := NewClient(&Options{
		Addr:            listenSubscribeServer(t, &reject),
		Protocol:        2,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	ps := client.Subscribe(cancelled, "ch")
	defer ps.Close()

	if channels, _, _ := ps.handle.Subscriptions(); len(channels) != 1 || channels[0] != "ch" {
		t.Fatalf("subscriptions = %v, want [ch]", channels)
	}
	// The cancellation surfaces first, unless the local dial settled
	// before the abandoned wait was checked (Subscribe then succeeded).
	ctx := context.Background()
	ev, err := ps.ReceiveTimeout(ctx, 5*time.Second)
	if errors.Is(err, context.Canceled) {
		ev, err = ps.ReceiveTimeout(ctx, 5*time.Second)
	}
	if err != nil {
		t.Fatalf("second Receive: %v, want the subscribe confirmation", err)
	}
	if sub, ok := ev.(*Subscription); !ok || sub.Kind != "subscribe" || sub.Channel != "ch" {
		t.Fatalf("second Receive = %#v, want subscribe ch", ev)
	}
}

// listenSubscribeServer serves a minimal RESP2 server that confirms
// SUBSCRIBE ch, rejecting AUTH while reject is set; it returns its
// address.
// TestSubscribeCredentialsSeeCallerContext pins that the pub/sub
// handshake runs with the subscribing context's values: a
// CredentialsProviderContext selecting credentials from them must see
// them, as it did on the dedicated PubSub connection.
func TestSubscribeCredentialsSeeCallerContext(t *testing.T) {
	type tenantKey struct{}
	var reject atomic.Bool
	addr := listenSubscribeServer(t, &reject)

	tenants := make(chan any, 4)
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		DisableIdentity: true,
		DialerRetries:   1,
		CredentialsProviderContext: func(ctx context.Context) (string, string, error) {
			tenants <- ctx.Value(tenantKey{})
			return "", "secret", nil
		},
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx := context.WithValue(context.Background(), tenantKey{}, "acme")
	ps := client.Subscribe(ctx, "ch")
	defer ps.Close()

	if _, err := ps.ReceiveTimeout(ctx, 5*time.Second); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	select {
	case tenant := <-tenants:
		if tenant != "acme" {
			t.Fatalf("CredentialsProviderContext saw tenant %v, want \"acme\"", tenant)
		}
	default:
		t.Fatal("CredentialsProviderContext was not called for the pub/sub handshake")
	}
}

func listenSubscribeServer(t *testing.T, reject *atomic.Bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "-ERR unknown command 'hello'\r\n"
				case "auth":
					if reject.Load() {
						return "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
					}
					return "+OK\r\n"
				case "subscribe":
					return "*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n"
				case "ping":
					return "*2\r\n$4\r\npong\r\n$0\r\n\r\n"
				}
				return "+OK\r\n"
			})
		}
	}()
	return ln.Addr().String()
}

// TestFailoverPubSubHooksRetireOnMasterSwitch drives the failover
// client's pub/sub retirement through the real onFailover callback: a
// connection dialed under one master must be retired once Sentinel
// switches to another, and never by the initial master discovery.
func TestFailoverPubSubHooksRetireOnMasterSwitch(t *testing.T) {
	ctx := context.Background()

	// newConn simulates a pub/sub dial: the connection is tagged with
	// the generation current at dial time, as pubsubConnect does.
	newConn := func(t *testing.T, h *failoverPubSubHooks) *pool.Conn {
		t.Helper()
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
		cn := pool.NewConn(client)
		h.connGen.Store(cn, h.gen.Load())
		return cn
	}
	// setup also counts recheck calls: a switch must ask the manager at
	// once, so an idle connection moves without waiting for a frame.
	setup := func(replicaOnly bool, discovered string) (*sentinelFailover, *failoverPubSubHooks, *atomic.Int32) {
		failover := &sentinelFailover{
			opt:        &FailoverOptions{MasterName: "mymaster"},
			masterAddr: discovered,
		}
		c := &baseClient{}
		rechecks := &atomic.Int32{}
		h := &failoverPubSubHooks{
			baseClient:  c,
			replicaOnly: replicaOnly,
			recheck:     func() { rechecks.Add(1) },
		}
		installOnFailover(failover, c, h)
		return failover, h, rechecks
	}

	t.Run("master discovered before the callback was installed", func(t *testing.T) {
		// A MinIdleConns pool dial resolved the master during
		// construction, before onFailover existed: the first real switch
		// must still retire the connection.
		failover, h, rechecks := setup(false, "10.0.0.1:6379")
		if got := rechecks.Load(); got != 0 {
			t.Fatalf("seeding the discovered master rechecked %d times, want 0", got)
		}
		cn := newConn(t, h)

		failover.trySwitchMaster(ctx, "10.0.0.2:6379")
		if !h.pubsubShouldReplaceConn(cn) {
			t.Fatal("connection on the demoted master was not retired")
		}
		if got := rechecks.Load(); got != 1 {
			t.Fatalf("master switch rechecked %d times, want 1", got)
		}
	})

	t.Run("initial discovery retires nothing", func(t *testing.T) {
		failover, h, rechecks := setup(false, "")
		cn := newConn(t, h)

		failover.trySwitchMaster(ctx, "10.0.0.1:6379")
		if h.pubsubShouldReplaceConn(cn) {
			t.Fatal("initial master discovery retired the connection")
		}
		if got := rechecks.Load(); got != 0 {
			t.Fatalf("initial discovery rechecked %d times, want 0", got)
		}
		failover.trySwitchMaster(ctx, "10.0.0.2:6379")
		if !h.pubsubShouldReplaceConn(cn) {
			t.Fatal("connection on the demoted master was not retired")
		}
		if got := rechecks.Load(); got != 1 {
			t.Fatalf("master switch rechecked %d times, want 1", got)
		}
		if fresh := newConn(t, h); h.pubsubShouldReplaceConn(fresh) {
			t.Fatal("connection dialed after the switch was retired")
		}
	})

	t.Run("replica-only never retires on a master switch", func(t *testing.T) {
		failover, h, rechecks := setup(true, "10.0.0.1:6379")
		cn := newConn(t, h)

		failover.trySwitchMaster(ctx, "10.0.0.2:6379")
		if h.pubsubShouldReplaceConn(cn) {
			t.Fatal("replica-only connection was retired on a master switch")
		}
		if got := rechecks.Load(); got != 0 {
			t.Fatalf("replica-only master switch rechecked %d times, want 0", got)
		}
	})
}

// TestPubSubDialRetryBackoffFollowsOptions pins that the shared pub/sub
// connection's dial retries follow the client's dial retry policy, as
// the connection pool applies it, not the command-retry backoff.
func TestPubSubDialRetryBackoffFollowsOptions(t *testing.T) {
	t.Run("DialerRetryBackoff", func(t *testing.T) {
		opt := &Options{Addr: "localhost:6379"}
		opt.DialerRetryBackoff = func(attempt int) time.Duration {
			return time.Duration(attempt+1) * time.Second
		}
		opt.init()

		backoff := pubSubConfigFromOptions(opt).DialRetryBackoff
		for attempt, want := range []time.Duration{time.Second, 2 * time.Second} {
			if got := backoff(attempt); got != want {
				t.Fatalf("attempt %d backoff = %v, want %v", attempt, got, want)
			}
		}
	})

	t.Run("DialerRetryTimeout", func(t *testing.T) {
		opt := &Options{Addr: "localhost:6379", DialerRetryTimeout: 750 * time.Millisecond}
		opt.init()

		backoff := pubSubConfigFromOptions(opt).DialRetryBackoff
		for attempt := range 3 {
			if got := backoff(attempt); got != 750*time.Millisecond {
				t.Fatalf("attempt %d backoff = %v, want the constant DialerRetryTimeout 750ms", attempt, got)
			}
		}
	})

	t.Run("Default", func(t *testing.T) {
		opt := &Options{Addr: "localhost:6379"}
		opt.init()

		if got := pubSubConfigFromOptions(opt).DialRetryBackoff(0); got != 100*time.Millisecond {
			t.Fatalf("default backoff = %v, want 100ms", got)
		}
	})
}
