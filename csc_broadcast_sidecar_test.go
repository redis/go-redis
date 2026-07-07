package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// fakeSidecarServer is a minimal in-process RESP3 server for sidecar unit
// tests: it answers HELLO with an empty map, CLIENT TRACKING with +OK, and
// PING with +PONG (only when answerPing is set).
type fakeSidecarServer struct {
	ln         net.Listener
	addr       string
	answerPing bool

	pingCh chan struct{} // one send per PING received

	mu          sync.Mutex
	conns       int
	firstClosed chan struct{} // closed when the first accepted conn dies
	closedOnce  sync.Once
}

func startFakeSidecarServer(t *testing.T, answerPing bool) *fakeSidecarServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeSidecarServer{
		ln:          ln,
		addr:        ln.Addr().String(),
		answerPing:  answerPing,
		pingCh:      make(chan struct{}, 8),
		firstClosed: make(chan struct{}),
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		first := true
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns++
			s.mu.Unlock()
			go s.serveConn(conn, first)
			first = false
		}
	}()
	return s
}

func (s *fakeSidecarServer) numConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns
}

func (s *fakeSidecarServer) serveConn(conn net.Conn, first bool) {
	defer conn.Close()
	rd := proto.NewReader(conn)
	for {
		reply, err := rd.ReadReply()
		if err != nil {
			if first {
				s.closedOnce.Do(func() { close(s.firstClosed) })
			}
			return
		}
		args, _ := reply.([]interface{})
		if len(args) == 0 {
			continue
		}
		switch strings.ToLower(fmt.Sprint(args[0])) {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "client":
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "ping":
			select {
			case s.pingCh <- struct{}{}:
			default:
			}
			if s.answerPing {
				_, _ = conn.Write([]byte("+PONG\r\n"))
			}
		}
	}
}

func fakeSidecarOptions(addr string) *Options {
	opt := &Options{Addr: addr, Protocol: 3, DialTimeout: 2 * time.Second}
	opt.init()
	return opt
}

// TestBroadcastSidecar_DialAfterShutdownClosesConn: a dialAndHandshake racing
// Shutdown must not publish the freshly dialed conn — it must close it, or the
// fd (and its server-side BCAST client) leaks with nothing left to close it.
func TestBroadcastSidecar_DialAfterShutdownClosesConn(t *testing.T) {
	srv := startFakeSidecarServer(t, true)
	s := newBroadcastSidecar(fakeSidecarOptions(srv.addr), NewLocalCache(CacheConfig{MaxEntries: 16}), 0)

	// Simulate Shutdown having fired while the dial was in flight.
	s.doneOnce.Do(func() { close(s.done) })

	if err := s.dialAndHandshake(context.Background()); err == nil {
		t.Fatal("dialAndHandshake after shutdown must fail, got nil")
	}
	s.connMu.Lock()
	conn := s.conn
	s.connMu.Unlock()
	if conn != nil {
		t.Fatal("conn must not be published after shutdown")
	}
	// The freshly dialed socket must have been closed (server sees EOF).
	select {
	case <-srv.firstClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("dialed conn was not closed after shutdown — fd leak")
	}
}

// TestBroadcastSidecar_ProbeDetectsDeadConn: with a silent server that never
// answers the liveness PING, the sidecar must probe after one quiet window and
// tear the conn down after the second, flushing the cache.
func TestBroadcastSidecar_ProbeDetectsDeadConn(t *testing.T) {
	srv := startFakeSidecarServer(t, false) // swallow PINGs
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("get:foo", []string{"foo"}, []byte("bar"))

	s := newBroadcastSidecar(fakeSidecarOptions(srv.addr), cache, 0)
	s.healthCheckInterval = 50 * time.Millisecond
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Shutdown()

	select {
	case <-srv.pingCh:
	case <-time.After(2 * time.Second):
		t.Fatal("sidecar never sent a liveness PING on a quiet connection")
	}
	select {
	case <-srv.firstClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("sidecar did not tear down the conn after an unanswered probe")
	}
	// tearDownConn flushes the whole cache (invalidations may have been missed).
	deadline := time.After(2 * time.Second)
	for cache.Len() != 0 {
		select {
		case <-deadline:
			t.Fatalf("cache not flushed after teardown, Len=%d", cache.Len())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// TestBroadcastSidecar_ProbeKeepsHealthyConn: a server that answers PONG keeps
// the connection alive — no teardown, no reconnect, still ready.
func TestBroadcastSidecar_ProbeKeepsHealthyConn(t *testing.T) {
	srv := startFakeSidecarServer(t, true)
	s := newBroadcastSidecar(fakeSidecarOptions(srv.addr), NewLocalCache(CacheConfig{MaxEntries: 16}), 0)
	s.healthCheckInterval = 50 * time.Millisecond
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Shutdown()

	select {
	case <-srv.pingCh:
	case <-time.After(2 * time.Second):
		t.Fatal("sidecar never sent a liveness PING")
	}
	// Ride out several probe windows: the conn must survive them.
	time.Sleep(300 * time.Millisecond)
	select {
	case <-srv.firstClosed:
		t.Fatal("healthy conn was torn down despite answered probes")
	default:
	}
	if n := srv.numConns(); n != 1 {
		t.Fatalf("expected a single long-lived sidecar conn, got %d", n)
	}
	if !s.ready.Load() {
		t.Fatal("sidecar must stay ready on a healthy conn")
	}
}

// TestProcessCached_BypassesCacheWhileSidecarDown: under Broadcast, a false
// readiness flag must send the command to the server (here: a failing dialer)
// instead of serving a cached value with no invalidation coverage.
func TestProcessCached_BypassesCacheWhileSidecarDown(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})

	cmd := NewStringCmd(ctx, "get", "foo")
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		t.Fatal("buildCacheKey failed for GET")
	}
	key := dbNamespacedKey(0, rawKey)
	cache.Set(key, []string{dbNamespacedKey(0, "foo")}, []byte("$3\r\nbar\r\n"))

	cp := pool.NewConnPool(&pool.Options{
		Dialer:      func(context.Context) (net.Conn, error) { return nil, errors.New("no network in test") },
		PoolSize:    1,
		PoolTimeout: time.Second,
		DialTimeout: time.Second,
	})
	defer cp.Close()

	var ready atomic.Bool
	c := &baseClient{opt: &Options{Protocol: 3}, connPool: cp, csc: cache, cscBcastReady: &ready}

	// Sidecar down: the cached value must NOT be served; the fetch hits the
	// (failing) network instead. The ctx deadline bounds the doomed dial.
	downCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.processCached(downCtx, cmd); err == nil {
		t.Fatal("sidecar down: expected network error, got a served reply (cache not bypassed)")
	}

	// Sidecar up: same command is a cache hit, no network involved.
	ready.Store(true)
	cmd2 := NewStringCmd(ctx, "get", "foo")
	if err := c.processCached(ctx, cmd2); err != nil {
		t.Fatalf("sidecar up: expected cache hit, got %v", err)
	}
	if got := cmd2.Val(); got != "bar" {
		t.Fatalf("cached value mismatch: got %q want %q", got, "bar")
	}
}
