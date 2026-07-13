package redis_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeMonitorServer implements just enough of the Redis protocol for the
// MONITOR command: it acknowledges the handshake, replies +OK to MONITOR,
// streams one monitor line and then goes silent while keeping the
// connection open, so the client hits its ReadTimeout.
func fakeMonitorServer(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			rd := bufio.NewReader(c)
			for {
				line, err := rd.ReadString('\n')
				if err != nil {
					return
				}
				if line[0] == '*' || line[0] == '$' {
					continue // RESP framing, only react to command verbs
				}
				switch {
				case strings.HasPrefix(strings.ToLower(line), "hello"):
					c.Write([]byte("-ERR unknown command 'hello'\r\n"))
				case strings.HasPrefix(strings.ToLower(line), "monitor"):
					c.Write([]byte("+OK\r\n"))
					c.Write([]byte("+1700000000.000000 [0 127.0.0.1:1] \"set\" \"foo\" \"bar\"\r\n"))
					// Go silent: the next read on the client times out.
					select {}
				default:
					c.Write([]byte("+OK\r\n"))
				}
			}
		}(conn)
	}
}

// See https://github.com/redis/go-redis/issues/3079: when the connection
// dies (e.g. ReadTimeout expires because MONITOR received no traffic), the
// monitor channel must be closed so the listener is not blocked forever,
// and the error must be available via cmd.Err().
func TestMonitorReadTimeoutClosesChannel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go fakeMonitorServer(ln)

	client := redis.NewClient(&redis.Options{
		Addr:        ln.Addr().String(),
		ReadTimeout: 500 * time.Millisecond,
	})
	defer client.Close()

	ch := make(chan string, 100)
	cmd := client.Monitor(context.Background(), ch)
	cmd.Start()
	defer cmd.Stop()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				if cmd.Err() == nil {
					t.Fatal("channel closed but cmd.Err() is nil")
				}
				return // unblocked with an error, as expected
			}
		case <-deadline:
			t.Fatal("monitor channel was not closed after read error; listener would block forever")
		}
	}
}
