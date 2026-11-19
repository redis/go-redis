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
