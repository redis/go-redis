package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleflightGroupCoalesces(t *testing.T) {
	const n = 50
	var g singleflightGroup
	var calls atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{})
	joined := make(chan struct{}, n)

	g.onWaiter = func() { joined <- struct{}{} }

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := g.Do(context.Background(), "k", func(shared context.Context) (interface{}, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return "ok", nil
			})
			if err != nil {
				errs <- err
				return
			}
			if v != "ok" {
				errs <- errors.New("unexpected value")
			}
		}()
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("shared call did not start")
	}
	// Wait until every non-leader caller has joined the in-flight call.
	for i := 0; i < n-1; i++ {
		select {
		case <-joined:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d waiters joined, want %d", i, n-1)
		}
	}
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("shared calls = %d, want 1", got)
	}
}

func TestSingleflightGroupWaiterCancelDoesNotAbortShared(t *testing.T) {
	var g singleflightGroup
	var calls atomic.Int32
	var sharedFinished atomic.Bool
	started := make(chan struct{})
	release := make(chan struct{})

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, err := g.Do(context.Background(), "k", func(shared context.Context) (interface{}, error) {
			calls.Add(1)
			close(started)
			<-release
			if err := shared.Err(); err != nil {
				t.Errorf("shared ctx cancelled while the leader still waits: %v", err)
			}
			sharedFinished.Store(true)
			return "ok", nil
		})
		if err != nil {
			t.Errorf("leader err: %v", err)
		}
	}()

	<-started
	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() {
		_, err := g.Do(waiterCtx, "k", func(shared context.Context) (interface{}, error) {
			t.Error("waiter must not run shared fn")
			return nil, nil
		})
		waiterErr <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-waiterErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not return on cancel")
	}
	if sharedFinished.Load() {
		t.Fatal("shared finished before release")
	}
	close(release)
	<-leaderDone
	if !sharedFinished.Load() {
		t.Fatal("shared call was aborted")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("shared calls = %d, want 1", got)
	}
}

func TestSingleflightGroupLeaderCancelReturnsEarly(t *testing.T) {
	var g singleflightGroup
	started := make(chan struct{})
	sharedErr := make(chan error, 1)

	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := g.Do(leaderCtx, "k", func(shared context.Context) (interface{}, error) {
			close(started)
			<-shared.Done()
			sharedErr <- shared.Err()
			return nil, shared.Err()
		})
		leaderErr <- err
	}()

	<-started
	cancel()
	select {
	case err := <-leaderErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("leader err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not return on cancel")
	}
	// The leader was the only caller, so the shared lookup must be cancelled
	// rather than keep running with nobody waiting for it.
	select {
	case err := <-sharedErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shared err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shared lookup kept running after every caller gave up")
	}
}

func TestSingleflightGroupAbandonedCallNotReused(t *testing.T) {
	var g singleflightGroup
	started := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := g.Do(ctx, "k", func(shared context.Context) (interface{}, error) {
			close(started)
			<-release
			return "stale", nil
		})
		errCh <- err
	}()
	<-started
	cancel()
	<-errCh

	// A new caller must start a fresh lookup, not join the abandoned one.
	v, err := g.Do(context.Background(), "k", func(shared context.Context) (interface{}, error) {
		return "fresh", nil
	})
	close(release)
	if err != nil || v != "fresh" {
		t.Fatalf("Do = %v, %v; want fresh, nil", v, err)
	}
}

func TestSingleflightGroupPanicReturnsError(t *testing.T) {
	var g singleflightGroup
	_, err := g.Do(context.Background(), "k", func(shared context.Context) (interface{}, error) {
		panic("boom")
	})
	if err == nil {
		t.Fatal("want error from panicking fn")
	}
	// The key must not stay stuck after a panic.
	v, err := g.Do(context.Background(), "k", func(shared context.Context) (interface{}, error) {
		return "ok", nil
	})
	if err != nil || v != "ok" {
		t.Fatalf("Do after panic = %v, %v; want ok, nil", v, err)
	}
}

func TestSentinelOptionsMinIdleConnsZero(t *testing.T) {
	opt := &FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379"},
		PoolSize:      100,
		MinIdleConns:  20,
	}
	sopt := opt.sentinelOptions("127.0.0.1:26379")
	if sopt.MinIdleConns != 0 {
		t.Fatalf("sentinel MinIdleConns = %d, want 0", sopt.MinIdleConns)
	}
	if sopt.PoolSize != 100 {
		t.Fatalf("sentinel PoolSize = %d, want inherited 100", sopt.PoolSize)
	}
}

// TestSentinelFailoverMasterAddrCoalesces checks that a cold burst of
// MasterAddr calls shares one SENTINEL get-master-addr-by-name query (#4146).
// The fake sentinel holds its first reply until every caller has joined the
// in-flight lookup (through onWaiter), so the burst cannot race the reply the
// way a fixed sleep did.
func TestSentinelFailoverMasterAddrCoalesces(t *testing.T) {
	const (
		n          = 40
		masterAddr = "127.0.0.1:6379"
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen sentinel: %v", err)
	}
	defer ln.Close()

	var queries atomic.Int32
	firstQuery := make(chan struct{})
	release := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveHeldMasterAddr(conn, masterAddr, &queries, firstQuery, release)
		}
	}()

	sentinelAddr := ln.Addr().String()
	failover := &sentinelFailover{
		opt: &FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{sentinelAddr},
			PoolSize:      n,
			DialTimeout:   time.Second,
			ReadTimeout:   time.Second,
			WriteTimeout:  time.Second,
		},
		sentinelAddrs: []string{sentinelAddr},
	}
	defer func() { _ = failover.Close() }()

	joined := make(chan struct{}, n)
	failover.masterAddrFlight.onWaiter = func() { joined <- struct{}{} }

	var wg sync.WaitGroup
	addrs := make(chan string, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := failover.MasterAddr(context.Background())
			if err != nil {
				errs <- err
				return
			}
			addrs <- addr
		}()
	}

	select {
	case <-firstQuery:
	case <-time.After(2 * time.Second):
		t.Fatal("sentinel did not receive get-master-addr-by-name")
	}
	// Every other caller has joined the in-flight lookup, so releasing the
	// reply cannot let a late caller start a second one.
	for i := 0; i < n-1; i++ {
		select {
		case <-joined:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d callers joined the lookup, want %d", i, n-1)
		}
	}
	close(release)
	wg.Wait()
	close(addrs)
	close(errs)

	for err := range errs {
		t.Fatalf("MasterAddr: %v", err)
	}
	for addr := range addrs {
		if addr != masterAddr {
			t.Fatalf("MasterAddr = %q, want %q", addr, masterAddr)
		}
	}
	if got := queries.Load(); got != 1 {
		t.Fatalf("get-master-addr-by-name queries = %d, want 1", got)
	}
}

// serveHeldMasterAddr answers a fake sentinel connection. It holds the first
// get-master-addr-by-name reply until release, and answers every later one
// immediately.
func serveHeldMasterAddr(
	conn net.Conn,
	masterAddr string,
	queries *atomic.Int32,
	firstQuery chan struct{},
	release <-chan struct{},
) {
	defer conn.Close()
	host, port, _ := net.SplitHostPort(masterAddr)
	rd := bufio.NewReader(conn)
	for {
		args, err := readSentinelCommand(rd)
		if err != nil {
			return
		}
		var reply string
		switch {
		case len(args) == 3 && strings.EqualFold(args[0], "sentinel") &&
			strings.EqualFold(args[1], "get-master-addr-by-name"):
			if queries.Add(1) == 1 {
				close(firstQuery)
			}
			select {
			case <-release:
			case <-time.After(2 * time.Second):
				return
			}
			reply = fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(host), host, len(port), port)
		case strings.EqualFold(args[0], "sentinels"):
			// No other sentinels to discover.
			reply = "*0\r\n"
		case strings.EqualFold(args[0], "psubscribe"):
			var b strings.Builder
			for i, pattern := range args[1:] {
				fmt.Fprintf(&b, "*3\r\n$10\r\npsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(pattern), pattern, i+1)
			}
			reply = b.String()
		case strings.EqualFold(args[0], "hello"):
			reply = "-NOPROTO sorry this protocol version is not supported\r\n"
		default:
			reply = "+OK\r\n"
		}
		if reply != "" {
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		}
	}
}

// readSentinelCommand parses one RESP2 array-of-bulk-strings request.
func readSentinelCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("unexpected request line %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		sizeLine, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		sizeLine = strings.TrimSuffix(strings.TrimSuffix(sizeLine, "\n"), "\r")
		if len(sizeLine) == 0 || sizeLine[0] != '$' {
			return nil, fmt.Errorf("unexpected bulk header %q", sizeLine)
		}
		size, err := strconv.Atoi(sizeLine[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}
