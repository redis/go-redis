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

	"github.com/redis/go-redis/v9/internal/proto"
)

// fdPanicReplyCmd is a GET whose reply decoder panics, like a RawWriteToCmd
// whose user io.Writer panics while the reply streams into it.
type fdPanicReplyCmd struct{ *StringCmd }

func (fdPanicReplyCmd) readReply(*proto.Reader) error { panic("boom: reply decoder") }

// A decoder panic in the middle of a read group must not replay the replies
// already read in that group. The first reply is delayed so SET a, GET b and
// the panicking command share one group. Before the fix the panic escaped to
// the reader's recover, which never counted the group's completed replies, so
// recovery replayed SET a and GET b on a new connection.
func TestFullDuplexGroupPanicDoesNotReplayReadReplies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var sets atomic.Int64
	var first atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				scripted := false
				var held []string
				n := 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					name := strings.ToLower(args[0])
					var reply string
					switch {
					case name == "hello":
						reply = "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n"
					case name == "set":
						sets.Add(1)
						reply = "+OK\r\n"
					case name == "get":
						reply = "$" + itoa(len(args[1])) + "\r\n" + args[1] + "\r\n"
					default:
						reply = "+OK\r\n"
					}
					if name != "get" && name != "set" {
						_, _ = io.WriteString(c, reply)
						continue
					}
					if n == 0 {
						scripted = first.CompareAndSwap(false, true)
					}
					n++
					if !scripted {
						_, _ = io.WriteString(c, reply)
						continue
					}
					if n == 1 {
						time.Sleep(100 * time.Millisecond) // let the next three queue up
						_, _ = io.WriteString(c, reply)
						continue
					}
					held = append(held, reply)
					if len(held) == 3 {
						_, _ = io.WriteString(c, strings.Join(held, "")) // one group
						held = nil
					}
				}
			}(c)
		}
	}()

	cl := NewClient(&Options{Addr: ln.Addr().String(), Protocol: 2, DisableIdentity: true,
		PipelinePoolSize: 2, MaxRetries: 1,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})
	defer cl.Close()
	ap, err := cl.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	g0 := ap.Get(ctx, "k0")
	set := ap.Set(ctx, "a", "v", 0)
	getB := ap.Get(ctx, "b")
	boom := ap.Submit(ctx, fdPanicReplyCmd{NewStringCmd(ctx, "get", "c")})

	if v, err := g0.Result(); err != nil || v != "k0" {
		t.Fatalf("g0: v=%q err=%v", v, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET a: %v", err)
	}
	if v, err := getB.Result(); err != nil || v != "b" {
		t.Fatalf("GET b: v=%q err=%v", v, err)
	}
	if err := boom.Wait(); err == nil {
		t.Fatal("the panicking command succeeded")
	}
	if n := sets.Load(); n != 1 {
		t.Fatalf("SET a executed %d times: replies already read in the group were replayed", n)
	}
}

// Once a burst has drained, the queue must not keep the burst's backing array;
// a queue that stays small keeps its buffer, so ordinary traffic does not pay a
// reallocation per wave.
func TestFDQueueReleasesBurstBuffer(t *testing.T) {
	q := newFDQueue(1 << 16)
	for i := 0; i < 10000; i++ {
		q.push(fdQueueTag(i))
	}
	for q.depth() > 0 {
		q.takeInto(nil, 200)
	}
	if c := cap(q.buf); c > fdQueueRetainCap {
		t.Fatalf("drained queue kept a %d-slot buffer, want <= %d", c, fdQueueRetainCap)
	}

	small := newFDQueue(1 << 16)
	for i := 0; i < 1000; i++ {
		small.push(fdQueueTag(i))
	}
	before := cap(small.buf)
	small.takeInto(nil, 1000)
	if cap(small.buf) != before {
		t.Fatalf("a small queue dropped its buffer (%d -> %d); it should reuse it", before, cap(small.buf))
	}
}
