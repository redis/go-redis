package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// startMockPONGServer speaks enough RESP for Ping() against a ClusterClient
// that uses ClusterSlots (no CLUSTER SLOTS command on the wire).
func startMockPONGServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					args, err := readRESPCommand(r)
					if err != nil {
						return
					}
					if len(args) == 0 {
						continue
					}
					switch strings.ToUpper(args[0]) {
					case "HELLO":
						_, _ = c.Write([]byte("-ERR unknown command 'hello'\r\n"))
					case "PING":
						_, _ = c.Write([]byte("+PONG\r\n"))
					default:
						_, _ = c.Write([]byte("+OK\r\n"))
					}
				}
			}(conn)
		}
	}()
	return ln
}

// startHungServer accepts TCP connections but never replies, so reads hit
// the socket deadline (i/o timeout) instead of connection refused.
func startHungServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
		_ = ln.Close()
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				<-done
			}(conn)
		}
	}()
	return ln
}

func newSlotSwapClusterClient(t *testing.T, currentAddr *atomic.Value, extra func(*ClusterOptions)) *ClusterClient {
	t.Helper()
	opt := &ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{
				Start: 0,
				End:   16383,
				Nodes: []ClusterNode{{Addr: currentAddr.Load().(string)}},
			}}, nil
		},
		MaxRedirects:               3,
		ClusterStateReloadInterval: time.Hour,
		DialTimeout:                200 * time.Millisecond,
		DialerRetries:              1,
		DialerRetryTimeout:         time.Millisecond,
		MinRetryBackoff:            -1,
		MaxRetryBackoff:            -1,
	}
	if extra != nil {
		extra(opt)
	}
	client := NewClusterClient(opt)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func waitForClusterPing(t *testing.T, client *ClusterClient, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = client.Ping(ctx).Err()
		if lastErr == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("ping after topology change: %v", lastErr)
}

func TestIsNodeGoneError(t *testing.T) {
	dialRefused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	dialDeadline := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	dialCanceled := &net.OpError{Op: "dial", Net: "tcp", Err: context.Canceled}
	ioTimeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "wrapped canceled", err: fmt.Errorf("op: %w", context.Canceled), want: false},
		{name: "dial canceled", err: dialCanceled, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: false},
		{name: "wrapped deadline", err: fmt.Errorf("op: %w", context.DeadlineExceeded), want: false},
		{name: "dial refused", err: dialRefused, want: true},
		{name: "dial deadline", err: dialDeadline, want: false},
		{name: "i/o timeout", err: ioTimeout, want: true},
		{name: "wrapped i/o timeout", err: fmt.Errorf("read: %w", ioTimeout), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNodeGoneError(tt.err); got != tt.want {
				t.Fatalf("isNodeGoneError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestClusterClientReloadsStateOnConnectionRefused(t *testing.T) {
	live := startMockPONGServer(t)
	defer live.Close()
	liveAddr := live.Addr().String()

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen dead: %v", err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	var currentAddr atomic.Value
	currentAddr.Store(deadAddr)

	ctx := context.Background()
	client := newSlotSwapClusterClient(t, &currentAddr, nil)

	if err := client.Ping(ctx).Err(); err == nil {
		t.Fatal("expected ping against the closed listener to fail")
	}

	currentAddr.Store(liveAddr)

	// LazyReload is async and has a 200ms cooldown, so the next Get may
	// still see the previous slot map. Wait until the refresh lands.
	waitForClusterPing(t, client, ctx)
}

func TestClusterClientReloadsStateOnNetworkTimeout(t *testing.T) {
	hung := startHungServer(t)
	live := startMockPONGServer(t)
	defer live.Close()
	liveAddr := live.Addr().String()

	var currentAddr atomic.Value
	currentAddr.Store(hung.Addr().String())

	ctx := context.Background()
	client := newSlotSwapClusterClient(t, &currentAddr, func(opt *ClusterOptions) {
		opt.ReadTimeout = 50 * time.Millisecond
		opt.WriteTimeout = 50 * time.Millisecond
	})

	if err := client.Ping(ctx).Err(); err == nil {
		t.Fatal("expected ping against the hung listener to fail")
	}

	currentAddr.Store(liveAddr)
	waitForClusterPing(t, client, ctx)
}

func TestClusterClientDoesNotReloadOnDeadlineExceeded(t *testing.T) {
	live := startMockPONGServer(t)
	defer live.Close()

	var loads atomic.Int32
	client := NewClusterClient(&ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			loads.Add(1)
			return []ClusterSlot{{
				Start: 0,
				End:   16383,
				Nodes: []ClusterNode{{Addr: live.Addr().String()}},
			}}, nil
		},
		ClusterStateReloadInterval: time.Hour,
		MinRetryBackoff:            -1,
		MaxRetryBackoff:            -1,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return nil, context.DeadlineExceeded
		},
	})
	defer client.Close()

	if err := client.Ping(context.Background()).Err(); err == nil {
		t.Fatal("expected ping to fail with deadline")
	}
	before := loads.Load()

	// LazyReload's goroutine would call ClusterSlots after the 200ms cooldown.
	time.Sleep(350 * time.Millisecond)
	if got := loads.Load(); got != before {
		t.Fatalf("caller deadline triggered topology reload: loads %d -> %d", before, got)
	}
}

func TestClusterClientDoesNotReloadOnCanceledContext(t *testing.T) {
	live := startMockPONGServer(t)
	defer live.Close()

	var loads atomic.Int32
	client := NewClusterClient(&ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			loads.Add(1)
			return []ClusterSlot{{
				Start: 0,
				End:   16383,
				Nodes: []ClusterNode{{Addr: live.Addr().String()}},
			}}, nil
		},
		ClusterStateReloadInterval: time.Hour,
		MinRetryBackoff:            -1,
		MaxRetryBackoff:            -1,
	})
	defer client.Close()

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("warmup ping: %v", err)
	}
	before := loads.Load()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_ = client.Ping(canceled).Err()

	// LazyReload's goroutine would call ClusterSlots after the 200ms cooldown.
	time.Sleep(350 * time.Millisecond)
	if got := loads.Load(); got != before {
		t.Fatalf("canceled context triggered topology reload: loads %d -> %d", before, got)
	}
}
