package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A RESP2 flat reply (a single array of alternating member/score or key/value
// elements) must have an even length. An odd length means the parser would read
// n/2 pairs and leave the trailing element on the wire, desyncing the pooled
// connection. These parsers must reject it, matching the guard the sibling flat
// parsers (VLINKS, VSIM) already have.
func TestFlatReplyOddLengthRejected(t *testing.T) {
	t.Run("ZSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n"))
		cmd := NewZSliceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil {
			t.Fatalf("odd-length flat reply accepted; readReply returned nil (val=%+v)", cmd.val)
		}
	})
	t.Run("KeyValueSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n$1\r\nk\r\n$1\r\nv\r\n$1\r\nx\r\n"))
		cmd := NewKeyValueSliceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil {
			t.Fatalf("odd-length flat reply accepted; readReply returned nil (val=%+v)", cmd.val)
		}
	})
	t.Run("ZSliceWithKeyCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*2\r\n$3\r\nzst\r\n*3\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n"))
		cmd := NewZSliceWithKeyCmd(context.Background())
		if err := cmd.readReply(rd); err == nil {
			t.Fatalf("odd-length flat reply accepted; readReply returned nil (val=%+v)", cmd.val)
		}
	})
}

// Even-length flat replies and the nested-array form must still parse cleanly.
func TestFlatReplyValidStillParses(t *testing.T) {
	t.Run("flat", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*4\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$3\r\n2.5\r\n"))
		cmd := NewZSliceCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("valid flat reply: %v", err)
		}
		if len(cmd.val) != 2 || cmd.val[0].Member != "a" || cmd.val[1].Member != "b" {
			t.Fatalf("unexpected val: %+v", cmd.val)
		}
	})
	t.Run("nested", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*2\r\n*2\r\n$1\r\na\r\n$3\r\n1.5\r\n*2\r\n$1\r\nb\r\n$3\r\n2.5\r\n"))
		cmd := NewZSliceCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("valid nested reply: %v", err)
		}
		if len(cmd.val) != 2 {
			t.Fatalf("unexpected val: %+v", cmd.val)
		}
	})
}
