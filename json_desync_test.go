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

// With RESP3, JSON.NUMINCRBY replies with an array that holds null for each
// match that is not a number. A null element must not fail the command.
func TestJSONCmdArrayWithNullElement(t *testing.T) {
	cmd := newJSONCmd(context.Background())
	rd := proto.NewReader(strings.NewReader("*4\r\n_\r\n_\r\n:7\r\n,3.5\r\n+NEXT\r\n"))
	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply() error = %v", err)
	}
	if got, want := cmd.Val(), "[null,null,7,3.5]"; got != want {
		t.Fatalf("Val() = %q, want %q", got, want)
	}
	if next, err := rd.ReadString(); err != nil || next != "NEXT" {
		t.Fatalf("next reply = %q, %v; reader is out of sync", next, err)
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

// The JSON array commands reply to a legacy path with a single value instead
// of an array with one value per match.
func TestSliceCmdLegacyPathReply(t *testing.T) {
	ctx := context.Background()

	intCmd := NewIntSliceCmd(ctx)
	rd := proto.NewReader(strings.NewReader(":3\r\n+NEXT\r\n"))
	if err := intCmd.readReply(rd); err != nil {
		t.Fatalf("IntSliceCmd.readReply() error = %v", err)
	}
	if len(intCmd.val) != 1 || intCmd.val[0] != 3 {
		t.Fatalf("IntSliceCmd val = %v, want [3]", intCmd.val)
	}
	if next, err := rd.ReadString(); err != nil || next != "NEXT" {
		t.Fatalf("next reply = %q, %v; reader is out of sync", next, err)
	}

	strCmd := NewStringSliceCmd(ctx)
	rd = proto.NewReader(strings.NewReader("$1\r\n4\r\n+NEXT\r\n"))
	if err := strCmd.readReply(rd); err != nil {
		t.Fatalf("StringSliceCmd.readReply() error = %v", err)
	}
	if len(strCmd.val) != 1 || strCmd.val[0] != "4" {
		t.Fatalf("StringSliceCmd val = %v, want [4]", strCmd.val)
	}
	if next, err := rd.ReadString(); err != nil || next != "NEXT" {
		t.Fatalf("next reply = %q, %v; reader is out of sync", next, err)
	}

	// JSON.ARRPOP on an empty array replies with null.
	for _, reply := range []string{"$-1\r\n", "_\r\n"} {
		strCmd = NewStringSliceCmd(ctx)
		if err := strCmd.readReply(proto.NewReader(strings.NewReader(reply))); err != Nil {
			t.Fatalf("StringSliceCmd.readReply(%q) error = %v, want Nil", reply, err)
		}
	}
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
