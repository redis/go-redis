package proto_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestReadChunked(t *testing.T) {
	payload := strings.Repeat("id=1 addr=127.0.0.1:1234 name=x\n", 100)

	encodings := map[string]string{
		"bulk":     fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload),
		"verbatim": fmt.Sprintf("=%d\r\ntxt:%s\r\n", len(payload)+4, payload),
	}

	for name, wire := range encodings {
		t.Run(name, func(t *testing.T) {
			// A small chunk buffer forces many fn calls and boundary straddling.
			rd := proto.NewReader(strings.NewReader(wire + "+OK\r\n"))
			var got bytes.Buffer
			n, err := rd.ReadChunked(make([]byte, 64), func(chunk []byte) error {
				got.Write(chunk)
				return nil
			})
			if err != nil {
				t.Fatalf("ReadChunked: %v", err)
			}
			if n != len(payload) {
				t.Fatalf("delivered %d bytes, want %d", n, len(payload))
			}
			if got.String() != payload {
				t.Fatalf("payload mismatch")
			}
			// The stream must be aligned on the next reply.
			if s, err := rd.ReadString(); err != nil || s != "OK" {
				t.Fatalf("follow-up reply: %q, %v (stream misaligned)", s, err)
			}
		})
	}
}

func TestReadChunkedFnErrorDrains(t *testing.T) {
	payload := strings.Repeat("x", 1000)
	wire := fmt.Sprintf("$%d\r\n%s\r\n+OK\r\n", len(payload), payload)
	rd := proto.NewReader(strings.NewReader(wire))

	sentinel := errors.New("stop")
	calls := 0
	_, err := rd.ReadChunked(make([]byte, 64), func(chunk []byte) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times after error, want 1", calls)
	}
	// The remainder must have been drained: the next reply parses cleanly.
	if s, err := rd.ReadString(); err != nil || s != "OK" {
		t.Fatalf("follow-up reply: %q, %v (stream misaligned after fn error)", s, err)
	}
}

func TestReadChunkedErrorReply(t *testing.T) {
	rd := proto.NewReader(strings.NewReader("-ERR unknown command\r\n"))
	_, err := rd.ReadChunked(make([]byte, 64), func(chunk []byte) error {
		t.Fatal("fn must not be called for an error reply")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("err = %v, want server error", err)
	}
}
