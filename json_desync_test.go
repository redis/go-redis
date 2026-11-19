package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A malformed array reply must surface the read error so the pooled connection
// is dropped by isBadConn. Before the fix JSONSliceCmd.readReply returned nil
// when rd.ReadReply() failed, so the failed read was reported as success and
// the connection went back to the pool in an indeterminate state, letting the
// next command read leftover bytes as its own reply.
func TestJSONSliceCmdReturnsReadError(t *testing.T) {
	// RESP2 array whose element is a malformed integer; ReadReply errors.
	reply := "*1\r\n:notanint\r\n"
	cmd := NewJSONSliceCmd(context.Background())
	rd := proto.NewReader(strings.NewReader(reply + "+NEXT\r\n"))
	if err := cmd.readReply(rd); err == nil {
		t.Fatalf("readReply() = nil, want error")
	}
}

// A legacy path (one that does not start with "$") gets a single value
// instead of an array with one value per match.
func TestIntPointerSliceCmdLegacyPathReply(t *testing.T) {
	tests := []struct {
		name  string
		reply string
		want  int64
	}{
		{name: "integer", reply: ":3\r\n", want: 3},
		{name: "toggle_true", reply: "$4\r\ntrue\r\n", want: 1},
		{name: "toggle_false", reply: "$5\r\nfalse\r\n", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewIntPointerSliceCmd(context.Background())
			rd := proto.NewReader(strings.NewReader(tc.reply + "+NEXT\r\n"))
			if err := cmd.readReply(rd); err != nil {
				t.Fatalf("readReply() error = %v", err)
			}
			if len(cmd.val) != 1 || cmd.val[0] == nil || *cmd.val[0] != tc.want {
				t.Fatalf("val = %v, want one element equal to %d", cmd.val, tc.want)
			}
			if next, err := rd.ReadString(); err != nil || next != "NEXT" {
				t.Fatalf("next reply = %q, %v; reader is out of sync", next, err)
			}
		})
	}

	t.Run("null", func(t *testing.T) {
		for _, reply := range []string{"$-1\r\n", "_\r\n"} {
			cmd := NewIntPointerSliceCmd(context.Background())
			rd := proto.NewReader(strings.NewReader(reply))
			if err := cmd.readReply(rd); err != Nil {
				t.Fatalf("readReply(%q) error = %v, want Nil", reply, err)
			}
		}
	})

	t.Run("unexpected_string", func(t *testing.T) {
		cmd := NewIntPointerSliceCmd(context.Background())
		rd := proto.NewReader(strings.NewReader("$3\r\nfoo\r\n"))
		if err := cmd.readReply(rd); err == nil {
			t.Fatal("readReply() = nil, want error")
		}
	})
}

// With RESP2, JSON.TYPE with a legacy path replies with a single string.
func TestJSONSliceCmdLegacyPathReply(t *testing.T) {
	cmd := NewJSONSliceCmd(context.Background())
	rd := proto.NewReader(strings.NewReader("$5\r\narray\r\n+NEXT\r\n"))
	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply() error = %v", err)
	}
	if len(cmd.val) != 1 || cmd.val[0] != "array" {
		t.Fatalf("val = %v, want [array]", cmd.val)
	}
	if next, err := rd.ReadString(); err != nil || next != "NEXT" {
		t.Fatalf("next reply = %q, %v; reader is out of sync", next, err)
	}
}
