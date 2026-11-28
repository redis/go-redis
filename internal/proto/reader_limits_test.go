package proto_test

import (
	"io"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// rejectedHeaders are reply headers whose length replyLen cannot decode: a
// negative bulk length below the nil marker, and length fields that overflow
// int so util.Atoi rejects them. readLine has already consumed the header when
// replyLen returns the error, so the rest of the frame cannot be located and
// must not be decoded as the next reply.
func rejectedHeaders() []struct{ name, header string } {
	return []struct{ name, header string }{
		{"negative bulk length", "$-2\r\n"},
		{"bulk length overflowing int", "$9223372036854775808\r\n"},
		{"push length overflowing int", ">9223372036854775808\r\n"},
		{"map length overflowing int", "%9223372036854775808\r\n"},
	}
}

// TestReader_RejectedLengthIsTerminalUntilReset covers the stream-integrity
// half of the guard: the rejected header has already been consumed, so the rest
// of the frame cannot be located. Any later read would decode the frame body as
// the next reply — which is how a pipelined/pending-push read could return a
// bogus success. The error therefore has to be fatal for the Reader, and only
// Reset may clear it.
func TestReader_RejectedLengthIsTerminalUntilReset(t *testing.T) {
	for _, tc := range rejectedHeaders() {
		t.Run(tc.name, func(t *testing.T) {
			// The bytes after the header stand for the unread part of the
			// rejected frame. They must never be parsed as the next reply.
			body := "+body\r\n"
			r := proto.NewReader(strings.NewReader(tc.header + body))
			fatalErr := func() error { _, err := r.ReadReply(); return err }()
			if fatalErr == nil {
				t.Fatalf("ReadReply on %q = nil error, want an error", tc.header)
			}
			if got, want := r.Buffered(), len(body); got != want {
				t.Fatalf("buffered after rejected header = %d, want %d (frame body consumed)", got, want)
			}

			for _, check := range []struct {
				name string
				read func() error
			}{
				{"ReadReply", func() error { _, err := r.ReadReply(); return err }},
				{"ReadString", func() error { _, err := r.ReadString(); return err }},
				{"ReadStringInto", func() error { _, err := r.ReadStringInto(make([]byte, 64)); return err }},
				{"ReadInt", func() error { _, err := r.ReadInt(); return err }},
				{"ReadArrayLen", func() error { _, err := r.ReadArrayLen(); return err }},
				{"ReadMapLen", func() error { _, err := r.ReadMapLen(); return err }},
				{"PeekReplyType", func() error { _, err := r.PeekReplyType(); return err }},
				{"Peek", func() error { _, err := r.Peek(1); return err }},
				{"PeekPushNotificationName", func() error { _, err := r.PeekPushNotificationName(); return err }},
				{"DiscardNext", r.DiscardNext},
				{"Discard", func() error { return r.Discard([]byte("+item")) }},
				{"ReadRawReply", func() error { _, err := r.ReadRawReply(); return err }},
				{"ReadRawReplyWriteTo", func() error { _, err := r.ReadRawReplyWriteTo(io.Discard); return err }},
			} {
				if err := check.read(); err != fatalErr {
					t.Errorf("%s after rejected header = %v; want the fatal error %v", check.name, err, fatalErr)
				}
			}
			if r.Buffered() != len(body) {
				t.Errorf("buffered after fatal reads = %d, want %d; a read consumed frame bytes", r.Buffered(), len(body))
			}
			if r.HasBufferedReply() {
				t.Error("HasBufferedReply after rejected header = true, want false")
			}

			r.Reset(strings.NewReader("+reset\r\n+next\r\n"))
			for _, want := range []string{"reset", "next"} {
				if got, err := r.ReadReply(); err != nil || got != want {
					t.Fatalf("ReadReply after Reset = %v, %v; want %q", got, err, want)
				}
			}
		})
	}
}

// TestReader_CommandReplyAfterFailedPushDrain mirrors what a client does when a
// pending RESP3 push notification cannot be drained: it reports the drain error
// and then reads the command reply on the same Reader. The rejected header makes
// the drain fail mid-frame, so the command reply read must return that same
// error instead of treating the push's remaining bytes as the reply.
func TestReader_CommandReplyAfterFailedPushDrain(t *testing.T) {
	push := ">2\r\n+message\r\n$-2\r\n"
	r := proto.NewReader(strings.NewReader(push + "+PONG\r\n"))
	_, drainErr := r.ReadReply()
	if drainErr == nil {
		t.Fatal("push drain: ReadReply = nil error, want the rejected length error")
	}
	reply, err := r.ReadString()
	if err != drainErr {
		t.Fatalf("command reply read = %q, %v; want the drain error %v (frame body decoded as a reply)", reply, err, drainErr)
	}
}
