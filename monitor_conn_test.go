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
// streams one monitor line and then closes the connection, simulating the
// server (or the network) dropping a monitor connection.
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
					return // deferred Close drops the connection mid-monitor
				default:
					c.Write([]byte("+OK\r\n"))
				}
			}
		}(conn)
	}
}

// See https://github.com/redis/go-redis/issues/3079: when the connection
// backing MONITOR dies, the monitor channel must be closed so a listener is
// not blocked forever, and the error must be available via cmd.Err().
func TestMonitorConnErrorClosesChannel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go fakeMonitorServer(ln)

	client := redis.NewClient(&redis.Options{
		Addr: ln.Addr().String(),
	})
	defer client.Close()

	ch := make(chan string, 100)
	cmd := client.Monitor(context.Background(), ch)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}
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
			t.Fatal("monitor channel was not closed after the connection died; listener would block forever")
		}
	}
}

// A clean Stop must not report an error and must not require any server
// traffic to take effect: closing the dedicated connection unblocks the
// reader goroutine even when the server is idle.
func TestMonitorStopWithoutTraffic(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				rd := bufio.NewReader(c)
				for {
					line, err := rd.ReadString('\n')
					if err != nil {
						c.Close()
						return
					}
					if line[0] == '*' || line[0] == '$' {
						continue
					}
					switch {
					case strings.HasPrefix(strings.ToLower(line), "hello"):
						c.Write([]byte("-ERR unknown command 'hello'\r\n"))
					case strings.HasPrefix(strings.ToLower(line), "monitor"):
						c.Write([]byte("+OK\r\n"))
						select {} // keep the connection open, send nothing
					default:
						c.Write([]byte("+OK\r\n"))
					}
				}
			}(conn)
		}
	}()

	client := redis.NewClient(&redis.Options{
		Addr: ln.Addr().String(),
	})
	defer client.Close()

	ch := make(chan string, 100)
	cmd := client.Monitor(context.Background(), ch)
	cmd.Start()
	time.Sleep(100 * time.Millisecond) // let the reader block in Peek

	done := make(chan struct{})
	go func() {
		cmd.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() blocked; reader goroutine was not unblocked")
	}
	if cmd.Err() != nil {
		t.Fatalf("clean Stop must not set an error, got: %v", cmd.Err())
	}
}
