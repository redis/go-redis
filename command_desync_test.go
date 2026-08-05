package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A reply type that MapStringSliceInterfaceCmd does not expect must not be left
// unread. Before the fix the switch had no default and returned nil, so the
// peeked frame stayed buffered and the next command on the pooled connection
// read it as its own reply.
func TestMapStringSliceInterfaceCmdRejectsUnexpectedReplyType(t *testing.T) {
	for _, reply := range []string{
		"_\r\n",        // RESP3 null
		"$-1\r\n",      // RESP2 nil bulk string
		"~1\r\n:1\r\n", // RESP3 set
		":7\r\n",       // integer
	} {
		cmd := NewMapStringSliceInterfaceCmd(context.Background())
		rd := proto.NewReader(strings.NewReader(reply + "+NEXT\r\n"))
		if err := cmd.readReply(rd); err == nil {
			t.Fatalf("reply %q: readReply() = nil, want error", reply)
		}
	}
}

// An unknown field inside the FUNCTION STATS engine map must have its value
// drained so the reader stays aligned with the following reply.
func TestFunctionStatsCmdIgnoresUnknownEngineField(t *testing.T) {
	reply := "%2\r\n" +
		"$14\r\nrunning_script\r\n_\r\n" +
		"$7\r\nengines\r\n" +
		"%1\r\n" +
		"$3\r\nLUA\r\n" +
		"%2\r\n" +
		"$15\r\nlibraries_count\r\n:3\r\n" +
		"$13\r\nnew-lua-field\r\n$5\r\nhello\r\n" // unknown field, string value

	cmd := NewFunctionStatsCmd(context.Background())
	rd := proto.NewReader(strings.NewReader(reply + "+NEXT\r\n"))
	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply() returned unexpected error: %v", err)
	}
	if engines := cmd.Val().Engines; len(engines) != 1 || engines[0].LibrariesCount != 3 {
		t.Fatalf("unexpected engines: %+v", engines)
	}
	if next, err := rd.ReadString(); err != nil || next != "NEXT" {
		t.Fatalf("stream desynced: next=%q err=%v", next, err)
	}
}

// An unknown field inside the LCS IDX reply map must have its value drained so
// the reader stays aligned with the following reply.
func TestLCSCmdIgnoresUnknownIdxField(t *testing.T) {
	reply := "%2\r\n" +
		"$13\r\nnew-idx-field\r\n$3\r\nfoo\r\n" + // unknown field, string value
		"$3\r\nlen\r\n:6\r\n"

	cmd := &LCSCmd{readType: 3} // force the IDX parsing path
	cmd.baseCmd = baseCmd{ctx: context.Background()}
	rd := proto.NewReader(strings.NewReader(reply + "+NEXT\r\n"))
	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply() returned unexpected error: %v", err)
	}
	if cmd.Val().Len != 6 {
		t.Fatalf("unexpected LCS len: %d", cmd.Val().Len)
	}
	if next, err := rd.ReadString(); err != nil || next != "NEXT" {
		t.Fatalf("stream desynced: next=%q err=%v", next, err)
	}
}
