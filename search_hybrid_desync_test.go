package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// FT.HYBRID replies with a RESP3 map on a RESP3 connection, like the other
// FT.* commands. FTHybridCmd.readReply must consume the whole map: reading it
// with ReadSlice treats the map header's declared length as an ELEMENT count,
// so only half the key/value frames are consumed and the rest are left on the
// connection. readReply then returns nil (success), the connection is pooled
// desynced, and the next command reads a stray map frame as its own reply.
func TestFTHybridRESP3MapNoDesync(t *testing.T) {
	// A 4-pair RESP3 map, followed by the next command's reply on the wire.
	const reply = "%4\r\n" +
		"$13\r\ntotal_results\r\n:5\r\n" +
		"$7\r\nresults\r\n*0\r\n" +
		"$6\r\nformat\r\n$6\r\nSTRING\r\n" +
		"$7\r\nwarning\r\n*0\r\n"
	const next = ":99\r\n"

	rd := proto.NewReader(strings.NewReader(reply + next))
	cmd := newFTHybridCmd(context.Background(), nil)
	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if cmd.val.TotalResults != 5 {
		t.Fatalf("TotalResults = %d, want 5", cmd.val.TotalResults)
	}

	// The whole map must have been consumed: the next reply on the wire is the
	// following command's integer 99, not a leftover map frame.
	got, err := rd.ReadReply()
	if err != nil {
		t.Fatalf("reading next reply: %v", err)
	}
	if n, ok := got.(int64); !ok || n != 99 {
		t.Fatalf("connection desynced: next reply = %#v, want int64(99)", got)
	}
}

// The RESP2 flat-array form must still parse and stay in sync.
func TestFTHybridRESP2ArrayStillParses(t *testing.T) {
	const reply = "*4\r\n" +
		"$13\r\ntotal_results\r\n:7\r\n" +
		"$7\r\nresults\r\n*0\r\n"
	const next = ":42\r\n"

	rd := proto.NewReader(strings.NewReader(reply + next))
	cmd := newFTHybridCmd(context.Background(), nil)
	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if cmd.val.TotalResults != 7 {
		t.Fatalf("TotalResults = %d, want 7", cmd.val.TotalResults)
	}
	got, err := rd.ReadReply()
	if err != nil {
		t.Fatalf("reading next reply: %v", err)
	}
	if n, ok := got.(int64); !ok || n != 42 {
		t.Fatalf("connection desynced: next reply = %#v, want int64(42)", got)
	}
}
