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
