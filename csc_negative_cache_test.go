package redis

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestProcessCached_CachesServerNilReply exercises the complete miss/fill/hit
// path. The second GET must be answered locally even though the cached command
// still returns redis.Nil to its caller.
func TestProcessCached_CachesServerNilReply(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var getCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveNegativeCacheTestConn(netConn, &getCalls)
		}
	}()

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCache: cache,
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if err := client.Get(context.Background(), "missing").Err(); err != Nil {
			t.Fatalf("GET %d: got %v, want redis.Nil", i+1, err)
		}
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("server received %d GETs, want 1 (second lookup should hit CSC)", got)
	}
	if cache.Len() != 1 {
		t.Fatalf("negative lookup was not retained in CSC, Len=%d", cache.Len())
	}
}

func serveNegativeCacheTestConn(netConn net.Conn, getCalls *atomic.Int32) {
	serveTestRESPConn(netConn, func(command string) string {
		switch command {
		case "hello":
			return "%0\r\n"
		case "get":
			getCalls.Add(1)
			return "$-1\r\n"
		default:
			return "+OK\r\n"
		}
	})
}

func serveTestRESPConn(netConn net.Conn, replyFor func(command string) string) {
	defer netConn.Close()

	scanner := bufio.NewScanner(netConn)
	for scanner.Scan() {
		header := scanner.Text()
		if !strings.HasPrefix(header, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
		if err != nil || n <= 0 {
			return
		}

		command := ""
		for i := 0; i < n; i++ {
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
				return
			}
			if i == 0 {
				command = strings.ToLower(scanner.Text())
			}
		}

		if _, err := netConn.Write([]byte(replyFor(command))); err != nil {
			return
		}
	}
}
