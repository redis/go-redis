package redis_test

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestFailoverClientMasterAddrCoalesces checks that a cold burst of dials
// shares one SENTINEL get-master-addr-by-name query (#4146 / #4222 review).
func TestFailoverClientMasterAddrCoalesces(t *testing.T) {
	var queries atomic.Int32

	master, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen master: %v", err)
	}
	defer master.Close()
	go acceptRedisPong(master)

	masterHost, masterPort, _ := net.SplitHostPort(master.Addr().String())

	sentinel, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen sentinel: %v", err)
	}
	defer sentinel.Close()
	go func() {
		for {
			conn, err := sentinel.Accept()
			if err != nil {
				return
			}
			go serveCountingFakeSentinel(conn, masterHost, masterPort, &queries)
		}
	}()

	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{sentinel.Addr().String()},
		PoolSize:      50,
		MinIdleConns:  0,
		DialTimeout:   time.Second,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
	})
	defer client.Close()

	const n = 40
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := client.Ping(ctx).Err(); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Ping: %v", err)
	}
	if got := queries.Load(); got != 1 {
		t.Fatalf("get-master-addr-by-name queries = %d, want 1", got)
	}
}

func acceptRedisPong(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go serveRedisPong(conn)
	}
}

func serveRedisPong(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		args, err := readCommand(rd)
		if err != nil {
			return
		}
		var reply string
		switch {
		case len(args) >= 1 && strings.EqualFold(args[0], "ping"):
			reply = "+PONG\r\n"
		case len(args) >= 1 && strings.EqualFold(args[0], "hello"):
			// Minimal RESP2 HELLO rejection / simple OK is fine; many
			// clients fall back. Reply with a map-shaped error-free stub.
			reply = "-NOPROTO sorry this protocol version is not supported\r\n"
		default:
			reply = "+OK\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func serveCountingFakeSentinel(conn net.Conn, masterHost, masterPort string, queries *atomic.Int32) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		args, err := readCommand(rd)
		if err != nil {
			return
		}
		var reply string
		switch {
		case len(args) == 3 && strings.EqualFold(args[0], "sentinel") &&
			strings.EqualFold(args[1], "get-master-addr-by-name"):
			queries.Add(1)
			reply = "*2\r\n$" + strconv.Itoa(len(masterHost)) + "\r\n" + masterHost + "\r\n$" +
				strconv.Itoa(len(masterPort)) + "\r\n" + masterPort + "\r\n"
		case strings.EqualFold(args[0], "psubscribe") || strings.EqualFold(args[0], "subscribe"):
			continue
		case len(args) >= 1 && strings.EqualFold(args[0], "hello"):
			reply = "-NOPROTO sorry this protocol version is not supported\r\n"
		default:
			reply = "+OK\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}
