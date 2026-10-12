package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A per-entry array whose declared length is zero must be rejected before the
// parser uses itemLen-1 as a slice size. Without the guard, itemLen-1 == -1
// panics the make (len/cap out of range) and the mandatory first field is read
// out of the empty entry, consuming a frame from the next reply and desyncing
// the pooled connection.
func TestZeroLengthEntryRejected(t *testing.T) {
	t.Run("TSTimestampValueSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*1\r\n*0\r\n:123\r\n"))
		cmd := newTSTimestampValueSliceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil {
			t.Fatalf("zero-length sample accepted; readReply returned nil (val=%+v)", cmd.val)
		}
	})
	t.Run("MapStringSliceInterfaceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*1\r\n*0\r\n$1\r\nk\r\n"))
		cmd := NewMapStringSliceInterfaceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil {
			t.Fatalf("zero-length entry accepted; readReply returned nil (val=%+v)", cmd.val)
		}
	})
}

// Valid replies must still parse after the guard.
func TestEntryLenValidStillParses(t *testing.T) {
	t.Run("TSTimestampValueSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*1\r\n*2\r\n:123\r\n$3\r\n1.5\r\n"))
		cmd := newTSTimestampValueSliceCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("valid sample: %v", err)
		}
		if len(cmd.val) != 1 || cmd.val[0].Timestamp != 123 || cmd.val[0].Value != 1.5 {
			t.Fatalf("unexpected val: %+v", cmd.val)
		}
	})
	t.Run("MapStringSliceInterfaceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*1\r\n*2\r\n$1\r\nk\r\n$3\r\n1.5\r\n"))
		cmd := NewMapStringSliceInterfaceCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("valid entry: %v", err)
		}
		if got := cmd.val["k"]; len(got) != 1 {
			t.Fatalf("unexpected val: %+v", cmd.val)
		}
	})
}
