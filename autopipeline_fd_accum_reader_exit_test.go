package redis

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A batch taken off the queue while the writer waits in the MaxFlushDelay
// accumulation must not be written once the reader has exited. Writing it put
// a command the reader could never acknowledge on a connection the server
// still reads (a read timeout leaves the socket open), so the server ran it,
// and recovery then replayed it as unacknowledged: one SET, two executions.
// The batch is now kept unsent and recovered as never sent, so it runs once.
func TestFullDuplexAccumulationDoesNotWriteAfterReaderExit(t *testing.T) {
	const held = 70 // above the accumulation gate (64 at this window)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var conns, sets atomic.Int64
	// heldAt / setAt: when the first connection received the last held GET
	// and the SET (unix nanos, 0 = never).
	var heldAt, setAt atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			first := conns.Add(1) == 1
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				gets := 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					name := ""
					if len(args) > 0 {
						name = strings.ToLower(args[0])
					}
					switch {
					case name == "hello":
						_, _ = io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n")
					case name == "set":
						sets.Add(1)
						if first {
							setAt.Store(time.Now().UnixNano())
						}
						if !first {
							_, _ = io.WriteString(c, "+OK\r\n")
						}
					case name == "get":
						if !first {
							_, _ = io.WriteString(c, "$-1\r\n")
							continue
						}
						// Hold every GET on the first connection: the reader times
						// out while the writer is parked in its accumulation wait.
						// The connection stays open and read, so a late write
						// would still run.
						if gets++; gets == held {
							heldAt.Store(time.Now().UnixNano())
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
		ReadTimeout:      300 * time.Millisecond,
		MaxRetries:       3,
	})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		FullDuplexWindow: 4096,            // accumulation gate at its 64 floor
		MaxFlushDelay:    2 * time.Second, // far longer than the test: the reader dies mid-wait
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	gets := make([]*StringCmd, held)
	for i := range gets {
		gets[i] = ap.Get(ctx, "g")
	}
	time.Sleep(50 * time.Millisecond) // the GETs are written and held
	set := ap.Set(ctx, "x", "v", 0)
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	// The case under test is a SET the writer was HOLDING in its wait when the
	// reader timed out. A writer whose wait ended earlier (a short grace with
	// nothing else arriving) sent the SET before the timeout, and replaying a
	// sent command after a timeout is ordinary at-least-once recovery.
	if at := setAt.Load(); at != 0 && time.Duration(at-heldAt.Load()) < 200*time.Millisecond {
		t.Skip("the writer sent the SET before the reader timed out; it was not held in a wait")
	}
	if n := sets.Load(); n != 1 {
		t.Fatalf("SET executed %d times, want 1 (written after the reader exited, then replayed?)", n)
	}
	if conns.Load() < 2 {
		t.Fatal("the session never recovered on a new connection; the test did not exercise the reader exit")
	}
}
