package proto_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// bufferedReader returns a Reader whose buffer holds exactly data, with the
// underlying source exhausted, so HasBufferedReply sees only those bytes.
func bufferedReader(t *testing.T, data string) *proto.Reader {
	t.Helper()
	rd := proto.NewReader(bytes.NewReader([]byte(data)))
	if data != "" {
		if _, err := rd.Peek(1); err != nil { // fill the buffer
			t.Fatalf("peek: %v", err)
		}
	}
	if got := rd.Buffered(); got != len(data) {
		t.Fatalf("buffered %d bytes, want %d", got, len(data))
	}
	return rd
}

var completeReplies = []string{
	"+OK\r\n",
	"-ERR bad\r\n",
	":42\r\n",
	"_\r\n",
	",3.14\r\n",
	"#t\r\n",
	"(12345678901234567890\r\n",
	"$5\r\nhello\r\n",
	"$0\r\n\r\n",
	"$-1\r\n",
	"*-1\r\n",
	"=15\r\ntxt:Some string\r\n",
	"!10\r\nERR failed\r\n",
	"*0\r\n",
	"*2\r\n$1\r\na\r\n:1\r\n",
	"%1\r\n+k\r\n*2\r\n:1\r\n:2\r\n",
	"~2\r\n+a\r\n+b\r\n",
	// A push and an attribute in front of the reply are part of reading it.
	">2\r\n+invalidate\r\n*1\r\n$1\r\nk\r\n+OK\r\n",
	"|1\r\n+ttl\r\n:3\r\n$1\r\nv\r\n",
	// An attribute inside an aggregate prefixes the element after it; it is
	// not an element itself.
	"*2\r\n|1\r\n+a\r\n+b\r\n:1\r\n:2\r\n",
	"%1\r\n+k\r\n|1\r\n+a\r\n+b\r\n$1\r\nv\r\n",
}

// TestHasBufferedReply checks each complete reply, every strict prefix of it,
// and the reply followed by the start of another.
func TestHasBufferedReply(t *testing.T) {
	for _, reply := range completeReplies {
		if !bufferedReader(t, reply).HasBufferedReply() {
			t.Errorf("%q: complete reply reported as partial", reply)
		}
		if !bufferedReader(t, reply+"$3\r\nab").HasBufferedReply() {
			t.Errorf("%q: complete reply followed by a partial one reported as partial", reply)
		}
		for n := 0; n < len(reply); n++ {
			if bufferedReader(t, reply[:n]).HasBufferedReply() {
				t.Errorf("%q: prefix %q reported as complete", reply, reply[:n])
			}
		}
	}
}

func TestHasBufferedReplyRejects(t *testing.T) {
	for _, data := range []string{
		">1\r\n+push\r\n",                // a push alone is not a reply
		"$3\r\nabcd\r\n",                 // length does not match the terminator
		"$-2\r\n",                        // invalid length
		"*?\r\n:1\r\n.\r\n",              // streamed aggregate
		"*9223372036854775807\r\n:1\r\n", // count the buffer cannot hold
		"x\r\n",                          // unknown type
		"+OK\n",                          // bare LF
	} {
		if bufferedReader(t, data).HasBufferedReply() {
			t.Errorf("%q: reported as a complete reply", data)
		}
	}
}

// HasBufferedReply must not consume: the reply reads normally afterwards.
func TestHasBufferedReplyConsumesNothing(t *testing.T) {
	rd := proto.NewReader(bufio.NewReader(bytes.NewReader([]byte("$5\r\nhello\r\n"))))
	if _, err := rd.Peek(1); err != nil {
		t.Fatalf("peek: %v", err)
	}
	if !rd.HasBufferedReply() {
		t.Fatal("complete reply reported as partial")
	}
	if s, err := rd.ReadString(); err != nil || s != "hello" {
		t.Fatalf("ReadString = %q, %v; want hello", s, err)
	}
}

func BenchmarkHasBufferedReply(b *testing.B) {
	data := bytes.Repeat([]byte("$5\r\nhello\r\n"), 64)
	rd := proto.NewReader(bytes.NewReader(data))
	if _, err := rd.Peek(1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !rd.HasBufferedReply() {
			b.Fatal("partial")
		}
	}
}
