package redis

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func TestEphemeralConnCSCMaxActiveConns(t *testing.T) {
	client := NewClient(&Options{
		Protocol: 3, PoolSize: 4, MaxActiveConns: 1, MaxConcurrentDials: 1,
		MaxRetries: -1, DisableIdentity: true,
		ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 16},
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go serveTestRESPConn(server, func(command string) string {
				if command == "hello" {
					return "%0\r\n"
				}
				return "+OK\r\n"
			})
			return client, nil
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	first, second := client.EphemeralConn(), client.EphemeralConn()
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	ctx := context.Background()
	if err := first.Select(ctx, 1).Err(); err != nil {
		t.Fatal(err)
	}
	if err := second.Select(ctx, 1).Err(); !errors.Is(err, pool.ErrPoolExhausted) {
		t.Fatalf("second session: %v, want ErrPoolExhausted", err)
	}
	if err := client.Ping(ctx).Err(); !errors.Is(err, pool.ErrPoolExhausted) {
		t.Fatalf("pooled command alongside session: %v, want ErrPoolExhausted", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Select(ctx, 1).Err(); err != nil {
		t.Fatalf("session after capacity released: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if got := client.PoolStats().TotalConns; got != 0 {
		t.Fatalf("connections after closing both sessions=%d, want 0", got)
	}
}

func TestEphemeralConnCSCSkipsMaintenanceNegotiation(t *testing.T) {
	var probes atomic.Int32
	client := NewClient(&Options{
		Protocol: 3, MaxRetries: -1, DisableIdentity: true,
		ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 16},
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeAuto},
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go serveTestRESPConn(server, func(command string) string {
				switch command {
				case "hello":
					return "%0\r\n"
				case "client":
					probes.Add(1)
					return "-ERR unsupported CLIENT command\r\n"
				case "ping":
					return "+PONG\r\n"
				}
				return "+OK\r\n"
			})
			return client, nil
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	manager := client.GetMaintNotificationsManager()
	if manager == nil {
		t.Fatal("parent maintenance manager was not installed")
	}
	conn := client.EphemeralConn()
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if probes.Load() != 0 {
		t.Error("isolated session negotiated CLIENT features")
	}
	if client.opt.MaintNotificationsConfig.Mode != maintnotifications.ModeAuto {
		t.Error("isolated session downgraded the parent's maintenance mode")
	}
	if client.GetMaintNotificationsManager() != manager {
		t.Error("isolated session changed the parent's maintenance manager")
	}
}

func TestEphemeralConnCSCIsolation(t *testing.T) {
	client := NewClient(&Options{
		Addr:                  "127.0.0.1:0",
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })
	conn := client.EphemeralConn()
	t.Cleanup(func() { _ = conn.Close() })
	borrowed := client.Conn()
	t.Cleanup(func() { _ = borrowed.Close() })
	tx := client.newTx()
	t.Cleanup(func() { _ = tx.Close(context.Background()) })

	for _, args := range [][]interface{}{
		{"select", 1},
		{"auth", "password"},
		{"hello", 2},
		{"reset"},
		{"client", "tracking", "off"},
	} {
		cmd := NewCmd(context.Background(), args...)
		if err := conn.cscCommandError(cmd); err != nil {
			t.Errorf("EphemeralConn rejected %v: %v", args, err)
		}
		for name, base := range map[string]*baseClient{
			"parent": client.baseClient, "Conn": &borrowed.baseClient, "Tx": &tx.baseClient,
		} {
			if err := base.cscCommandError(cmd); err == nil {
				t.Errorf("%s allowed %v with CSC active", name, args)
			}
		}
	}
	if conn.cscTrackingRequested() || conn.csc != nil || conn.cscPoolHook != nil {
		t.Error("EphemeralConn must not participate in the parent's cache or tracking")
	}
	if !client.cscTrackingRequested() {
		t.Error("creating an EphemeralConn disabled the parent's CSC")
	}
}

func TestEphemeralConnCSCConnection(t *testing.T) {
	for _, caching := range []bool{false, true} {
		name := "without CSC"
		if caching {
			name = "with CSC"
		}
		t.Run(name, func(t *testing.T) {
			var dials, tracking, gets atomic.Int32
			opt := &Options{
				Protocol: 3, PoolSize: 1, MaxRetries: -1, DisableIdentity: true,
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				Dialer: func(context.Context, string, string) (net.Conn, error) {
					dials.Add(1)
					client, server := net.Pipe()
					go serveTestRESPConn(server, func(command string) string {
						switch command {
						case "hello":
							return "%0\r\n"
						case "client":
							tracking.Add(1)
						case "get":
							gets.Add(1)
							return "$2\r\nv1\r\n"
						}
						return "+OK\r\n"
					})
					return client, nil
				},
			}
			cache := NewLocalCache(CacheConfig{MaxEntries: 16})
			if caching {
				opt.ClientSideCache = cache
			}
			client := NewClient(opt)
			t.Cleanup(func() { _ = client.Close() })
			ctx := context.Background()
			if err := client.Get(ctx, "key").Err(); err != nil {
				t.Fatal(err)
			}
			conn := client.EphemeralConn()
			t.Cleanup(func() { _ = conn.Close() })
			if got := dials.Load(); got != 1 {
				t.Fatalf("creating a lazy EphemeralConn dialed: %d", got)
			}
			for i := 0; i < 2; i++ {
				if err := conn.Get(ctx, "key").Err(); err != nil {
					t.Fatal(err)
				}
			}
			if got := gets.Load(); got != 3 {
				t.Fatalf("EphemeralConn reads must hit server: GETs=%d, want 3", got)
			}
			wantDials, wantTracking := int32(1), int32(0)
			if caching {
				wantDials, wantTracking = 2, 1
				if cache.Len() != 1 {
					t.Fatal("fresh EphemeralConn evicted the parent's warm cache")
				}
			}
			if dials.Load() != wantDials || tracking.Load() != wantTracking {
				t.Fatalf("dials=%d tracking=%d, want %d and %d", dials.Load(), tracking.Load(), wantDials, wantTracking)
			}
			if err := conn.Select(ctx, 1).Err(); err != nil {
				t.Fatalf("SELECT on EphemeralConn: %v", err)
			}
			for _, args := range [][]interface{}{
				{"auth", "password"}, {"hello", 2}, {"reset"}, {"client", "tracking", "off"},
			} {
				if err := conn.Do(ctx, args...).Err(); err != nil {
					t.Fatalf("session command %v on EphemeralConn: %v", args, err)
				}
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if caching {
				if err := client.Get(ctx, "key").Err(); err != nil {
					t.Fatal(err)
				}
				if gets.Load() != 3 {
					t.Fatal("parent lost its cached value after EphemeralConn.Close")
				}
			}
		})
	}
}
