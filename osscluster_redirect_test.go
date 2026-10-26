package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// With cluster-preferred-endpoint-type unknown-endpoint, MOVED and ASK carry
// no host ("MOVED 0 :6380"), and the client is expected to reuse the host of
// the node that sent the redirect. The nodes listen on 127.0.0.2 so that
// dialing ":port", which goes to the local default address, fails.
func TestClusterRedirectEmptyHost(t *testing.T) {
	if ln, err := net.Listen("tcp", "127.0.0.2:0"); err != nil {
		t.Skipf("127.0.0.2 is not available: %v", err)
	} else {
		_ = ln.Close()
	}

	for _, redirect := range []string{"MOVED", "ASK"} {
		for _, mode := range []string{"", "pipeline", "cursor"} {
			name := redirect
			if mode != "" {
				name += "/" + mode
			}
			t.Run(name, func(t *testing.T) {
				var targetGets atomic.Int32
				target := newRedirectTestServer(t, func(args []string) string {
					switch args[0] {
					case "get":
						targetGets.Add(1)
						return "$2\r\nok\r\n"
					case "ft.cursor":
						targetGets.Add(1)
						return "%0\r\n"
					case "hello":
						return "%0\r\n"
					}
					return "+OK\r\n"
				})
				_, targetPort, _ := net.SplitHostPort(target)

				var source string
				source = newRedirectTestServer(t, func(args []string) string {
					switch {
					case args[0] == "get" || args[0] == "ft.cursor":
						return "-" + redirect + " 0 :" + targetPort + "\r\n"
					case args[0] == "hello":
						return "%0\r\n"
					case args[0] == "cluster" && args[1] == "shards":
						return "-ERR unknown subcommand\r\n"
					case args[0] == "cluster" && args[1] == "slots":
						host, port, _ := net.SplitHostPort(source)
						return fmt.Sprintf("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$%d\r\n%s\r\n:%s\r\n", len(host), host, port)
					}
					return "+OK\r\n"
				})

				client := NewClusterClient(&ClusterOptions{
					Addrs:                    []string{source},
					Protocol:                 3,
					DisableIdentity:          true,
					MaxRedirects:             1,
					MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				})
				t.Cleanup(func() { _ = client.Close() })

				ctx := context.Background()
				switch mode {
				case "cursor":
					// FT.CURSOR picks its node itself rather than in process.
					if err := client.FTCursorRead(ctx, "idx", 1, 0).Err(); err != nil {
						t.Fatalf("FTCursorRead() = %v; want nil", err)
					}
				case "pipeline":
					var get *StringCmd
					_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error {
						get = pipe.Get(ctx, "key")
						return nil
					})
					if val, err := get.Result(); err != nil || val != "ok" {
						t.Fatalf("Get() = %q, %v; want %q, nil", val, err, "ok")
					}
				default:
					if val, err := client.Get(ctx, "key").Result(); err != nil || val != "ok" {
						t.Fatalf("Get() = %q, %v; want %q, nil", val, err, "ok")
					}
				}
				if n := targetGets.Load(); n != 1 {
					t.Fatalf("target served %d commands, want 1", n)
				}
			})
		}
	}
}

// newRedirectTestServer starts a RESP server on 127.0.0.2 and returns its
// address. reply receives the lowercased command arguments.
func newRedirectTestServer(t *testing.T, reply func(args []string) string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			wg.Add(1)
			go func() { defer wg.Done(); serveRedirectTestConn(conn, reply) }()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

func serveRedirectTestConn(conn net.Conn, reply func(args []string) string) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		n, err := strconv.Atoi(strings.TrimPrefix(scanner.Text(), "*"))
		if err != nil || n <= 0 {
			return
		}
		args := make([]string, 0, n+1)
		for i := 0; i < n; i++ {
			if !scanner.Scan() || !scanner.Scan() {
				return
			}
			args = append(args, strings.ToLower(scanner.Text()))
		}
		args = append(args, "") // so reply can index args[1]
		if _, err := conn.Write([]byte(reply(args))); err != nil {
			return
		}
	}
}

func TestRedirectAddr(t *testing.T) {
	opt := &ClusterOptions{}
	opt.init()
	nodes := newClusterNodes(opt)
	defer nodes.Close()

	node, err := nodes.GetOrCreate("10.0.0.5:6379")
	if err != nil {
		t.Fatal(err)
	}
	ipv6, err := nodes.GetOrCreate("[2001:db8::1]:6379")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		addr string
		node *clusterNode
		want string
	}{
		{":6380", node, "10.0.0.5:6380"},
		{":6380", ipv6, "[2001:db8::1]:6380"},
		{"10.0.0.6:6380", node, "10.0.0.6:6380"},
		{":6380", nil, ":6380"},
	}
	for _, tc := range tests {
		if got := redirectAddr(tc.addr, tc.node); got != tc.want {
			t.Errorf("redirectAddr(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

func TestIsMovedSameConnAddr(t *testing.T) {
	tests := []struct {
		moved string
		addr  string
		want  bool
	}{
		{"MOVED 1 10.0.0.5:6380", "10.0.0.5:6380", true},
		{"MOVED 1 10.0.0.6:6380", "10.0.0.5:6380", false},
		// Without a host the redirect is relative to the sending connection.
		{"MOVED 1 :6380", "10.0.0.5:6380", true},
		{"MOVED 1 :6381", "10.0.0.5:6380", false},
		{"ASK 1 :6380", "10.0.0.5:6380", false},
	}
	for _, tc := range tests {
		err := proto.ParseErrorReply([]byte("-" + tc.moved))
		if got := isMovedSameConnAddr(err, tc.addr); got != tc.want {
			t.Errorf("isMovedSameConnAddr(%q, %q) = %v, want %v", tc.moved, tc.addr, got, tc.want)
		}
	}
}
