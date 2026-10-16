package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func clientListWire(t testing.TB, verbatim bool, entries int) string {
	t.Helper()
	var b strings.Builder
	for i := 0; i < entries; i++ {
		fmt.Fprintf(&b, "id=%d addr=127.0.0.1:%d name=client-%d resp=3\n", i, 10000+i, i)
	}
	payload := b.String()
	if verbatim {
		return fmt.Sprintf("=%d\r\ntxt:%s\r\n", len(payload)+4, payload)
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
}

func TestStreamingCmdClientListParsed(t *testing.T) {
	// Enough entries to span several read chunks, so entries straddle
	// chunk boundaries.
	const entries = 2000

	for _, verbatim := range []bool{false, true} {
		name := "bulk"
		if verbatim {
			name = "verbatim"
		}
		t.Run(name, func(t *testing.T) {
			rd := proto.NewReader(strings.NewReader(clientListWire(t, verbatim, entries) + "+OK\r\n"))

			var got []int64
			cmd := NewStreamingCmd(context.Background(), "\n", StreamingParser(
				func(line []byte) (*ClientInfo, error) {
					return parseClientInfo(strings.TrimSpace(string(line)))
				},
				func(info *ClientInfo) error {
					got = append(got, info.ID)
					return nil
				},
			), 0, "client", "list")

			if err := cmd.readReply(rd); err != nil {
				t.Fatalf("readReply: %v", err)
			}
			if cmd.entriesCount != entries || len(got) != entries {
				t.Fatalf("streamed %d segments (callback saw %d), want %d", cmd.entriesCount, len(got), entries)
			}
			for i, id := range got {
				if id != int64(i) {
					t.Fatalf("entry %d has id %d (boundary split corrupted a line)", i, id)
				}
			}
			// The reader must be aligned on the next reply.
			if s, err := rd.ReadString(); err != nil || s != "OK" {
				t.Fatalf("follow-up reply: %q, %v (stream misaligned)", s, err)
			}
		})
	}
}

func TestStreamingCmdMultiByteDelimiter(t *testing.T) {
	// Segments joined by a 2-byte delimiter, sized so the payload spans
	// several chunks and the delimiter itself straddles chunk boundaries.
	const entries = 5000
	segs := make([]string, entries)
	for i := range segs {
		segs[i] = fmt.Sprintf("segment-%d", i)
	}
	payload := strings.Join(segs, "||")
	wire := fmt.Sprintf("$%d\r\n%s\r\n+OK\r\n", len(payload), payload)
	rd := proto.NewReader(strings.NewReader(wire))

	var got []string
	cmd := NewStreamingCmd(context.Background(), "||", func(segment []byte) error {
		got = append(got, string(segment))
		return nil
	}, 0, "dummy")

	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if len(got) != entries {
		t.Fatalf("streamed %d segments, want %d", len(got), entries)
	}
	for i, s := range got {
		if s != segs[i] {
			t.Fatalf("segment %d = %q, want %q", i, s, segs[i])
		}
	}
	if s, err := rd.ReadString(); err != nil || s != "OK" {
		t.Fatalf("follow-up reply: %q, %v (stream misaligned)", s, err)
	}
}

func TestStreamingCmdNoDelimiter(t *testing.T) {
	payload := strings.Repeat("x", 40_000) // > 2 chunks
	wire := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
	rd := proto.NewReader(strings.NewReader(wire))

	var got strings.Builder
	cmd := NewStreamingCmd(context.Background(), "", func(chunk []byte) error {
		got.Write(chunk)
		return nil
	}, 0, "dummy")

	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if got.String() != payload {
		t.Fatalf("payload mismatch: got %d bytes, want %d", got.Len(), len(payload))
	}
	if cmd.entriesCount < 2 {
		t.Fatalf("expected multiple raw chunks, got %d", cmd.entriesCount)
	}
}

func TestStreamingCmdCallbackError(t *testing.T) {
	rd := proto.NewReader(strings.NewReader(clientListWire(t, false, 100) + "+OK\r\n"))

	sentinel := errors.New("stop")
	seen := 0
	cmd := NewStreamingCmd(context.Background(), "\n", func(segment []byte) error {
		seen++
		if seen == 3 {
			return sentinel
		}
		return nil
	}, 0, "client", "list")

	if err := cmd.readReply(rd); !errors.Is(err, sentinel) {
		t.Fatalf("readReply err = %v, want sentinel", err)
	}
	if seen != 3 {
		t.Fatalf("callback ran %d times after error, want 3", seen)
	}
	// The rest of the reply must have been drained.
	if s, err := rd.ReadString(); err != nil || s != "OK" {
		t.Fatalf("follow-up reply: %q, %v (stream misaligned after callback error)", s, err)
	}
	// entriesCount must not include the entry whose callback failed.
	if cmd.entriesCount != 2 {
		t.Fatalf("entriesCount = %d, want 2 (the failed 3rd entry must not be counted)", cmd.entriesCount)
	}
}

// A StreamingCmd callback error is a reply-level outcome, not a transport
// failure: ReadChunked always drains the rest of the reply before readReply
// returns, so the connection is not desynchronized. isRedisError (consulted
// by isBadConn, pipeline-abort, and full-duplex fdReplyIsFatal gates
// throughout the client) must treat it as connection-safe, or a callback
// erroring on one entry would get the socket removed from the pool and, in
// full-duplex mode, abort unrelated in-flight commands sharing it.
func TestStreamingCmdCallbackErrorIsConnSafe(t *testing.T) {
	wire := clientListWire(t, false, 3) + "+OK\r\n"
	rd := proto.NewReader(strings.NewReader(wire))

	sentinel := errors.New("business logic stop")
	cmd := NewStreamingCmd(context.Background(), "\n", func(segment []byte) error {
		return sentinel
	}, 0, "client", "list")

	err := cmd.readReply(rd)
	if !errors.Is(err, sentinel) {
		t.Fatalf("readReply err = %v, want wrapping sentinel", err)
	}
	if !isRedisError(err) {
		t.Fatalf("isRedisError(err) = false, want true (must not be treated as a transport failure)")
	}
	if isBadConn(err, false, "127.0.0.1:6379") {
		t.Fatalf("isBadConn(err) = true, want false (ReadChunked already drained the reply)")
	}
	// The stream must still be aligned on the next reply.
	if s, e := rd.ReadString(); e != nil || s != "OK" {
		t.Fatalf("follow-up reply: %q, %v (stream misaligned)", s, e)
	}
}
