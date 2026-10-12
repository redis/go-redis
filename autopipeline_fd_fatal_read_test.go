package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fdPoisonServer is a minimal RESP2 server for the batched-reader tests. GET
// replies with its own key, HELLO with a minimal map, anything else with +OK. The FIRST connection that
// sends a GET is poisoned: its replies are held until `hold` GETs arrived, then
// written in ONE write with reply number `bad` replaced by an unparseable
// line. Holding the replies puts the fault in the middle of a reader snapshot
// with more replies already buffered behind it. Later connections (the replay
// after the session fails) answer each command at once.
type fdPoisonServer struct {
	ln       net.Listener
	hold     int
	bad      int
	poisoned atomic.Bool
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    []net.Conn
}

func newFDPoisonServer(t *testing.T, hold, bad int) *fdPoisonServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fdPoisonServer{ln: ln, hold: hold, bad: bad}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(c)
			}()
		}
	}()
	return s
}

func (s *fdPoisonServer) addr() string { return s.ln.Addr().String() }

func (s *fdPoisonServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *fdPoisonServer) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	poison := false
	var held []byte
	gets := 0
	for {
		args, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		if len(args) == 2 && (args[0] == "get" || args[0] == "GET") {
			if gets == 0 && !s.poisoned.Load() {
				poison = s.poisoned.CompareAndSwap(false, true)
			}
			gets++
			reply := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
			if !poison {
				if _, err := io.WriteString(c, reply); err != nil {
					return
				}
				continue
			}
			if gets == s.bad {
				reply = "?not-resp\r\n" // unknown type byte: a protocol fault
			}
			held = append(held, reply...)
			if gets == s.hold {
				if _, err := c.Write(held); err != nil {
					return
				}
				held = nil
			}
			continue
		}
		if len(args) > 0 && (args[0] == "hello" || args[0] == "HELLO") {
			// RESP2 HELLO reply: a flat key/value array.
			if _, err := io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n"); err != nil {
				return
			}
			continue
		}
		if _, err := io.WriteString(c, "+OK\r\n"); err != nil {
			return
		}
	}
}

// TestFullDuplexFatalReplyStopsTheSnapshot covers a protocol fault in the
// middle of a reader snapshot. The reader must stop at the fault: the failed
// command and everything after it stay in the deque and are replayed on a new
// connection. Before the fix the reader kept reading the rest of the snapshot
// from the desynced stream, then advanced past the failed command without
// completing it, so that caller never returned.
func TestFullDuplexFatalReplyStopsTheSnapshot(t *testing.T) {
	const n, bad = 20, 10
	srv := newFDPoisonServer(t, n, bad)
	defer srv.close()

	c := NewClient(&Options{
		Addr:             srv.addr(),
		Protocol:         2,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		MaxRetries:       3,
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
	cmds := make([]*StringCmd, n)
	for i := range cmds {
		cmds[i] = ap.Get(ctx, "k"+strconv.Itoa(i))
	}

	for i, cmd := range cmds {
		done := make(chan struct{})
		var v string
		var gerr error
		go func() {
			v, gerr = cmd.Result()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("command %d never completed: the reader advanced past it without completing it", i)
		}
		// Replies before the fault are correct. The faulted command and the
		// unread tail settle with the protocol error: a protocol fault is not
		// retried, since those commands may already have run. None of them may
		// receive a value read from the desynced stream.
		if i < bad-1 {
			if want := "k" + strconv.Itoa(i); gerr != nil || v != want {
				t.Fatalf("command %d: v=%q err=%v, want %q", i, v, gerr, want)
			}
		} else if gerr == nil || v != "" {
			t.Fatalf("command %d at/after the fault: v=%q err=%v, want the protocol error and no value", i, v, gerr)
		}
	}
	if !srv.poisoned.Load() {
		t.Fatal("the poisoned connection was never used; the test did not exercise the fault")
	}
}

// TestFullDuplexGroupedReadRearmsDeadline covers a reply that is only partly
// buffered when the group reaches it. k1 and k2 share one read group. k1
// arrives slowly, using most of the read timeout, and its last write also
// carries the first bytes of k2, so k2 needs another socket read. Before the
// fix that read ran under the deadline armed for k1 and timed out, although k2
// arrived well within its own ReadTimeout.
func TestFullDuplexGroupedReadRearmsDeadline(t *testing.T) {
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
							// Delay k0 so k1 and k2 are both in flight when the
							// reader takes its next snapshot: they must share a group.
							time.Sleep(100 * time.Millisecond)
							_, _ = io.WriteString(c, "$2\r\nk0\r\n")
						case 3:
							// k1 finishes at 0.6*timeout together with the first
							// bytes of k2, which finishes at 1.2*timeout.
							_, _ = io.WriteString(c, "$2\r\nk")
							time.Sleep(timeout * 6 / 10)
							_, _ = io.WriteString(c, "1\r\n$2\r\nk")
							time.Sleep(timeout * 6 / 10)
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
