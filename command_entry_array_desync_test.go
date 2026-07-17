package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// These parsers read a fixed set of fields from each entry array but did not
// reconcile that with the element count the server declares. A server that
// declares a longer array leaves the trailing frames on the wire; readReply
// returns nil and the desynced connection goes back to the pool, so the next
// command reads the leftover frames as its own reply. Each case appends a
// sentinel reply and checks it is still readable after readReply.

func TestEntryArrayDrainsExtraElements(t *testing.T) {
	t.Run("LatencyCmd", func(t *testing.T) {
		// entry declares 5 elements: the 4 fields plus one extra
		rd := proto.NewReader(strings.NewReader(
			"*1\r\n*5\r\n$5\r\nevent\r\n:1000\r\n:5\r\n:9\r\n:7\r\n+PONG\r\n"))
		cmd := NewLatencyCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		assertSentinel(t, rd)
	})

	t.Run("SlowLogCmd", func(t *testing.T) {
		// entry declares 7 elements: the 6 known fields plus one extra
		rd := proto.NewReader(strings.NewReader(
			"*1\r\n*7\r\n:1\r\n:1000\r\n:50\r\n*1\r\n$3\r\nGET\r\n$5\r\n1.2.3\r\n$4\r\nname\r\n:99\r\n+PONG\r\n"))
		cmd := NewSlowLogCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		assertSentinel(t, rd)
	})

	t.Run("HotKeysCmd", func(t *testing.T) {
		// outer array wraps two elements; only the first is parsed
		rd := proto.NewReader(strings.NewReader(
			"*2\r\n%1\r\n$15\r\ntracking-active\r\n:1\r\n%0\r\n+PONG\r\n"))
		cmd := NewHotKeysCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		assertSentinel(t, rd)
	})
}

// LATENCY GET entries hold 4 fields; a 3-element entry must be rejected rather
// than over-read into the next reply.
func TestLatencyCmdShortEntryRejected(t *testing.T) {
	// The trailing :9 belongs to the next reply; the unpatched parser reads it
	// as the 4th field and returns nil, desyncing the connection.
	rd := proto.NewReader(strings.NewReader(
		"*1\r\n*3\r\n$5\r\nevent\r\n:1000\r\n:5\r\n:9\r\n"))
	cmd := NewLatencyCmd(context.Background())
	if err := cmd.readReply(rd); err == nil {
		t.Fatalf("short latency entry accepted; readReply returned nil (val=%+v)", cmd.val)
	}
}

// GeoSearchLocationCmd must validate each entry array against the requested
// WITH flags, like GeoLocationCmd, instead of discarding the length.
func TestGeoSearchLocationEntryLengthValidated(t *testing.T) {
	opt := &GeoSearchLocationQuery{WithDist: true} // withLen == 2

	t.Run("oversized rejected", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader(
			"*1\r\n*3\r\n$1\r\na\r\n$3\r\n1.5\r\n:9\r\n"))
		cmd := NewGeoSearchLocationCmd(context.Background(), opt)
		if err := cmd.readReply(rd); err == nil {
			t.Fatalf("oversized geo entry accepted; readReply returned nil (val=%+v)", cmd.val)
		}
	})

	t.Run("valid parses", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader(
			"*1\r\n*2\r\n$1\r\na\r\n$3\r\n1.5\r\n"))
		cmd := NewGeoSearchLocationCmd(context.Background(), opt)
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("valid geo entry: %v", err)
		}
		if len(cmd.val) != 1 || cmd.val[0].Name != "a" || cmd.val[0].Dist != 1.5 {
			t.Fatalf("unexpected val: %+v", cmd.val)
		}
	})
}

func assertSentinel(t *testing.T, rd *proto.Reader) {
	t.Helper()
	s, err := rd.ReadString()
	if err != nil {
		t.Fatalf("stream desynced, sentinel unreadable: %v", err)
	}
	if s != "PONG" {
		t.Fatalf("stream desynced, got %q want PONG", s)
	}
}
