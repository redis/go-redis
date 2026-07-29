package redis

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestIsClientTrackingCmd pins the guard's matcher: any CLIENT TRACKING
// subcommand matches, other CLIENT subcommands (incl. TRACKINGINFO) do not.
func TestIsClientTrackingCmd(t *testing.T) {
	matching := []Cmder{
		makeCmd("client", "tracking", "on"),
		makeCmd("client", "tracking", "off"),
		makeCmd("CLIENT", "TRACKING", "on", "bcast"),
		makeCmd("Client", "Tracking"),
		makeCmd([]byte("client"), []byte("tracking"), "off"), // raw []byte args
	}
	for _, cmd := range matching {
		if !isClientTrackingCmd(cmd) {
			t.Errorf("expected %v to match CLIENT TRACKING", cmd.Args())
		}
	}
	nonMatching := []Cmder{
		makeCmd("client", "trackinginfo"),
		makeCmd("client", "info"),
		makeCmd("client", "kill", "id", "1"),
		makeCmd("get", "tracking"),
		makeCmd("client"),
	}
	for _, cmd := range nonMatching {
		if isClientTrackingCmd(cmd) {
			t.Errorf("expected %v NOT to match CLIENT TRACKING", cmd.Args())
		}
	}
}

// TestClientTrackingRejectedWithCSC: on a client with the built-in cache
// configured, CLIENT TRACKING must be rejected before it reaches a connection —
// it would flip an arbitrary pool conn's tracking state and leave it filling
// the cache with entries the server never invalidates. The guard fires without
// dialing, so no server is needed.
func TestClientTrackingRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.ClientTrackingOff(ctx).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}
	if err := c.ClientTrackingOn(ctx, nil).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}
	// The raw escape hatch is caught too: the guard matches leading args.
	// (Non-tracking CLIENT subcommands are covered by TestIsClientTrackingCmd's
	// non-matching cases — probing one here would dial for seconds.)
	if err := c.Do(ctx, "client", "tracking", "off").Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("raw Do(client tracking off) must be rejected with CSC enabled, got %v", err)
	}
}

// TestClientTrackingRejectedWithCSC_Pipeline: pipelines bypass process(), so
// generalProcessPipeline mirrors the guard — a CLIENT TRACKING frame inside a
// Pipeline or TxPipeline must be rejected on a CSC client too.
func TestClientTrackingRejectedWithCSC_Pipeline(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOff(ctx)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("Pipelined ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}

	_, err = c.TxPipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOn(ctx, nil)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("TxPipelined ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}
}

func TestCSCDisablesWhenHELLO3FallsBackToRESP2(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var getCalls atomic.Int32
	var trackingCalls atomic.Int32
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
				case "get":
					getCalls.Add(1)
					return "$-1\r\n"
				case "client":
					trackingCalls.Add(1)
					return "+OK\r\n"
				default:
					return "+OK\r\n"
				}
			})
		}
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if err := client.Get(context.Background(), "missing").Err(); err != Nil {
			t.Fatalf("GET %d: got %v, want redis.Nil", i+1, err)
		}
	}
	if client.cscActive == nil || client.cscActive.Load() {
		t.Fatal("CSC must be disabled after HELLO 3 falls back to RESP2")
	}
	if got := trackingCalls.Load(); got != 0 {
		t.Fatalf("server received %d CLIENT TRACKING commands after RESP2 fallback, want 0", got)
	}
	if got := getCalls.Load(); got != 2 {
		t.Fatalf("server received %d GETs, want 2 (RESP2 fallback must bypass CSC)", got)
	}
}

func TestSelectRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Do(ctx, "select", 1).Err(); !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("raw SELECT must be rejected with CSC enabled, got %v", err)
	}

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "select", 1)
		return nil
	})
	if !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("pipelined SELECT must be rejected with CSC enabled, got %v", err)
	}
}

func TestConnectionStateCommandsRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: every guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	tests := []struct {
		name string
		args []interface{}
		want error
	}{
		{"AUTH", []interface{}{"auth", "password"}, errAuthWithCSC},
		{"HELLO 2", []interface{}{"hello", 2}, errHelloWithCSC},
		{"RESET", []interface{}{"reset"}, errResetWithCSC},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.Do(ctx, tc.args...).Err(); !errors.Is(err, tc.want) {
				t.Fatalf("%v must be rejected with CSC enabled, got %v", tc.args, err)
			}
		})
	}

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "hello", 2)
		return nil
	})
	if !errors.Is(err, errHelloWithCSC) {
		t.Fatalf("pipelined HELLO 2 must be rejected with CSC enabled, got %v", err)
	}

	// Bare HELLO only reports connection properties and does not change
	// protocol, authentication, or tracking state.
	if err := c.cscCommandError(makeCmd("hello")); err != nil {
		t.Fatalf("bare HELLO must remain allowed, got %v", err)
	}
}

// TestOnConnectUsesCSCStateGuard verifies that initConn's exemption ends after
// the library's own CLIENT TRACKING command. OnConnect is user code and must
// not be able to mutate a tracked pool connection.
func TestOnConnectUsesCSCStateGuard(t *testing.T) {
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
			go func(netConn net.Conn) {
				defer netConn.Close()
				scanner := bufio.NewScanner(netConn)
				command := 0
				for scanner.Scan() {
					if !strings.HasPrefix(scanner.Text(), "*") {
						continue
					}
					command++
					if command == 1 {
						// HELLO 3 returns an empty RESP3 map.
						_, _ = netConn.Write([]byte("%0\r\n"))
					} else {
						_, _ = netConn.Write([]byte("+OK\r\n"))
					}
				}
			}(netConn)
		}
	}()

	c := NewClient(&Options{
		Addr:                  ln.Addr().String(),
		Protocol:              3,
		MaxRetries:            -1,
		DisableIdentity:       true,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		OnConnect: func(ctx context.Context, cn *Conn) error {
			return cn.Select(ctx, 1).Err()
		},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Ping(context.Background()).Err(); !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("OnConnect SELECT must be rejected after init's exemption ends, got %v", err)
	}
}

// TestClientTrackingAllowedWithoutCSC: without the built-in cache the guard
// predicate is off entirely (asserted directly — a live dial would prove
// nothing more and costs seconds against an unreachable address).
func TestClientTrackingAllowedWithoutCSC(t *testing.T) {
	c := NewClient(&Options{Addr: "localhost:1", Protocol: 3})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.baseClient.cscCommandError(
		NewCmd(context.Background(), "client", "tracking", "on"),
	); err != nil {
		t.Fatalf("a client without CSC rejected CLIENT TRACKING: %v", err)
	}
}
