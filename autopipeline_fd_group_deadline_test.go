package redis

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestFullDuplexGroupedReadFullDeadlinePerSocketRead covers a partly buffered
// reply reached EARLY in a read group: k1 completes less than half the
// timeout after the group armed its deadline, together with the first bytes
// of k2, and k2 needs another 0.9*timeout. Stopping the group only after half
// the timeout still read k2 under that deadline, which expired before k2
// arrived, although k2 arrived within its own ReadTimeout. A reply that needs
// socket data must start a new group with a full deadline.
func TestFullDuplexGroupedReadFullDeadlinePerSocketRead(t *testing.T) {
	const timeout = time.Second
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var once atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				slow := false
				gets := 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					switch {
					case len(args) > 0 && (args[0] == "hello" || args[0] == "HELLO"):
						_, _ = io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n")
					case len(args) == 2 && (args[0] == "get" || args[0] == "GET"):
						if gets == 0 {
							slow = once.CompareAndSwap(false, true)
						}
						gets++
						if !slow {
							_, _ = io.WriteString(c, "$2\r\n"+args[1]+"\r\n")
							continue
						}
						switch gets {
						case 1:
							// Delay k0 so k1 and k2 are in flight and read in the
							// same group as the reply before them.
							time.Sleep(100 * time.Millisecond)
							_, _ = io.WriteString(c, "$2\r\nk0\r\n")
						case 3:
							_, _ = io.WriteString(c, "$2\r\nk")
							time.Sleep(timeout * 3 / 10)
							_, _ = io.WriteString(c, "1\r\n$2\r\nk")
							time.Sleep(timeout * 9 / 10)
							_, _ = io.WriteString(c, "2\r\n")
						}
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}(c)
		}
	}()

	c := NewClient(&Options{
		Addr:             ln.Addr().String(),
		Protocol:         2,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		ReadTimeout:      timeout,
		MaxRetries:       -1, // a replay would hide the spurious timeout
	})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	cmds := []*StringCmd{ap.Get(ctx, "k0"), ap.Get(ctx, "k1"), ap.Get(ctx, "k2")}
	for i, cmd := range cmds {
		if v, err := cmd.Result(); err != nil || v != "k"+strconv.Itoa(i) {
			t.Fatalf("reply %d: v=%q err=%v (k2 read under k1's deadline?)", i, v, err)
		}
	}
	if !once.Load() {
		t.Fatal("the slow connection was never used; the test did not exercise the partial frame")
	}
}
