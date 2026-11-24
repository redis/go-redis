package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// readerCmdable runs commands by reading their reply from rd, with no server.
func readerCmdable(rd *proto.Reader) cmdable {
	return func(ctx context.Context, cmd Cmder) error {
		err := cmd.readReply(rd)
		cmd.SetErr(err)
		return err
	}
}

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

func expectNextOK(t *testing.T, rd *proto.Reader) {
	t.Helper()
	if s, err := rd.ReadString(); err != nil || s != "OK" {
		t.Fatalf("follow-up reply: %q, %v (stream misaligned)", s, err)
	}
}

func collectSegments(delim string, wire string, wrap func(*strings.Reader) *proto.Reader) ([]string, *StreamingCmd, *proto.Reader, error) {
	rd := wrap(strings.NewReader(wire))
	var got []string
	cmd := newStreamingCmd(context.Background(), delim, func(seg []byte) error {
		got = append(got, string(seg))
		return nil
	}, "dummy")
	err := cmd.readReply(rd)
	return got, cmd, rd, err
}

func plainReader(r *strings.Reader) *proto.Reader   { return proto.NewReader(r) }
func oneByteReader(r *strings.Reader) *proto.Reader { return proto.NewReader(iotest.OneByteReader(r)) }

func TestClientListStreaming(t *testing.T) {
	// Enough entries to span several reads, so entries straddle chunks.
	const entries = 2000

	for _, verbatim := range []bool{false, true} {
		name := "bulk"
		if verbatim {
			name = "verbatim"
		}
		t.Run(name, func(t *testing.T) {
			rd := proto.NewReader(strings.NewReader(clientListWire(t, verbatim, entries) + "+OK\r\n"))

			var got []int64
			cmd := readerCmdable(rd).ClientListStreaming(context.Background(), func(info *ClientInfo) error {
				got = append(got, info.ID)
				return nil
			})
			n, err := cmd.Result()
			if err != nil {
				t.Fatalf("ClientListStreaming: %v", err)
			}
			if n != entries || len(got) != entries {
				t.Fatalf("Val=%d (callback saw %d), want %d", n, len(got), entries)
			}
			for i, id := range got {
				if id != int64(i) {
					t.Fatalf("entry %d has id %d (boundary split corrupted a line)", i, id)
				}
			}
			expectNextOK(t, rd)
		})
	}
}

func TestStreamingCmdDelimiters(t *testing.T) {
	// The delimiter straddles read boundaries when reads are one byte long.
	const entries = 3000
	segs := make([]string, entries)
	for i := range segs {
		segs[i] = fmt.Sprintf("segment-%d", i)
	}

	for _, delim := range []string{"\n", "||", "<->"} {
		for rname, wrap := range map[string]func(*strings.Reader) *proto.Reader{
			"plain": plainReader, "one-byte": oneByteReader,
		} {
			t.Run(fmt.Sprintf("%q/%s", delim, rname), func(t *testing.T) {
				payload := strings.Join(segs, delim)
				wire := fmt.Sprintf("$%d\r\n%s\r\n+OK\r\n", len(payload), payload)
				got, cmd, rd, err := collectSegments(delim, wire, wrap)
				if err != nil {
					t.Fatalf("readReply: %v", err)
				}
				if len(got) != entries || cmd.entriesCount != entries {
					t.Fatalf("got %d segments (Val=%d), want %d", len(got), cmd.entriesCount, entries)
				}
				for i, s := range got {
					if s != segs[i] {
						t.Fatalf("segment %d = %q, want %q", i, s, segs[i])
					}
				}
				expectNextOK(t, rd)
			})
		}
	}
}

func TestStreamingCmdEdgeReplies(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want []string
	}{
		{"empty bulk", "$0\r\n\r\n", nil},
		{"empty verbatim", "=4\r\ntxt:\r\n", nil},
		{"trailing delimiter", "$4\r\na\nb\n\r\n", []string{"a", "b"}},
		{"no trailing delimiter", "$3\r\na\nb\r\n", []string{"a", "b"}},
		{"empty segments", "$3\r\n\n\n\n\r\n", []string{"", "", ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, rd, err := collectSegments("\n", tt.wire+"+OK\r\n", plainReader)
			if err != nil {
				t.Fatalf("readReply: %v", err)
			}
			if strings.Join(got, "|") != strings.Join(tt.want, "|") || len(got) != len(tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
			expectNextOK(t, rd)
		})
	}
}

func TestStreamingCmdNilReply(t *testing.T) {
	_, _, rd, err := collectSegments("\n", "$-1\r\n+OK\r\n", plainReader)
	if !errors.Is(err, Nil) {
		t.Fatalf("err = %v, want Nil", err)
	}
	expectNextOK(t, rd)
}

func TestStreamingCmdServerError(t *testing.T) {
	_, _, rd, err := collectSegments("\n", "-ERR boom\r\n+OK\r\n", plainReader)
	if err == nil || !isRedisError(err) {
		t.Fatalf("err = %v, want a Redis error", err)
	}
	expectNextOK(t, rd)
}

func TestStreamingCmdCallbackError(t *testing.T) {
	rd := proto.NewReader(strings.NewReader(clientListWire(t, false, 100) + "+OK\r\n"))

	sentinel := errors.New("stop")
	seen := 0
	cmd := newStreamingCmd(context.Background(), "\n", func(segment []byte) error {
		seen++
		if seen == 3 {
			return sentinel
		}
		return nil
	}, "client", "list")

	err := cmd.readReply(rd)
	if !errors.Is(err, sentinel) {
		t.Fatalf("readReply err = %v, want sentinel", err)
	}
	var cbErr *StreamingCallbackError
	if !errors.As(err, &cbErr) {
		t.Fatalf("err %T is not *StreamingCallbackError", err)
	}
	if seen != 3 {
		t.Fatalf("callback ran %d times, want 3 (no calls after the error)", seen)
	}
	expectNextOK(t, rd)
	if cmd.entriesCount != 2 {
		t.Fatalf("entriesCount = %d, want 2 (the failed entry is not counted)", cmd.entriesCount)
	}
}

// A callback failure is a reply-level outcome: the reply was drained, so the
// connection must stay in the pool and a pipeline must keep reading — even
// when the callback returns a context error.
func TestStreamingCmdCallbackErrorIsConnSafe(t *testing.T) {
	for name, cbErr := range map[string]error{
		"plain":            errors.New("business logic stop"),
		"context canceled": context.Canceled,
		"deadline":         context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			rd := proto.NewReader(strings.NewReader(clientListWire(t, false, 3) + "+OK\r\n"))
			cmd := newStreamingCmd(context.Background(), "\n", func([]byte) error { return cbErr }, "client", "list")

			err := cmd.readReply(rd)
			if !errors.Is(err, cbErr) {
				t.Fatalf("readReply err = %v, want to wrap %v", err, cbErr)
			}
			if !isRedisError(err) {
				t.Fatal("isRedisError = false, want true (pipelines and full-duplex keep the conn)")
			}
			if isBadConn(err, false, "127.0.0.1:6379") {
				t.Fatal("isBadConn = true, want false (the reply was drained)")
			}
			expectNextOK(t, rd)
		})
	}
}

func TestStreamingCmdMalformedReplyDropsConn(t *testing.T) {
	for name, wire := range map[string]string{
		"bad verbatim tag": "=8\r\ntxtXa\nb\n\r\n",
		"short verbatim":   "=2\r\nab\r\n",
		"array":            "*1\r\n$1\r\nx\r\n",
		"eof in payload":   "$10\r\nab\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := collectSegments("\n", wire, plainReader)
			if err == nil {
				t.Fatal("readReply succeeded, want error")
			}
			if !isBadConn(err, false, "127.0.0.1:6379") {
				t.Fatalf("isBadConn(%v) = false, want true (stream is not aligned)", err)
			}
		})
	}
}

func TestStreamingCmdSegmentTooLong(t *testing.T) {
	payload := strings.Repeat("x", maxStreamingSegment+1)
	wire := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
	_, _, _, err := collectSegments("\n", wire, plainReader)
	if !errors.Is(err, ErrStreamingSegmentTooLong) {
		t.Fatalf("err = %v, want ErrStreamingSegmentTooLong", err)
	}
}

func TestStreamingCmdRequiresCallbackAndDelimiter(t *testing.T) {
	rd := proto.NewReader(strings.NewReader("$1\r\nx\r\n"))
	if err := newStreamingCmd(context.Background(), "\n", nil, "x").readReply(rd); err == nil {
		t.Fatal("nil callback: want error, not a panic")
	}
	noop := func([]byte) error { return nil }
	if err := newStreamingCmd(context.Background(), "", noop, "x").readReply(rd); err == nil {
		t.Fatal("empty delimiter: want error")
	}
}

// A long segment read in tiny chunks must cost O(n). Rescanning or recopying
// the whole pending segment on each chunk is O(n^2): about 3e11 bytes moved
// here, which takes far longer than the budget.
func TestStreamingCmdLongSegmentIsLinear(t *testing.T) {
	const size = 768 << 10 // below the segment cap
	payload := strings.Repeat("x", size) + "\n" + "tail"
	wire := fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)

	done := make(chan error, 1)
	var got []string
	go func() {
		var err error
		got, _, _, err = collectSegments("\n", wire, oneByteReader)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil || len(got) != 2 || len(got[0]) != size || got[1] != "tail" {
			t.Fatalf("got %d segments, err=%v", len(got), err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("long segment took over 20s: scan or copy is not linear")
	}
}

func TestClientListStreamingNilCallback(t *testing.T) {
	rd := proto.NewReader(strings.NewReader(clientListWire(t, false, 3) + "+OK\r\n"))
	cmd := readerCmdable(rd).ClientListStreaming(context.Background(), nil)
	if cmd.Err() == nil {
		t.Fatal("nil callback: want error, not a panic")
	}
	// Nothing was sent, so nothing was read.
	if s, err := rd.ReadString(); err != nil || !strings.Contains(s, "id=0") {
		t.Fatalf("reply was consumed: %q, %v", s, err)
	}
}

func TestStreamingCmdRejectsClusterFanout(t *testing.T) {
	cmd := newStreamingCmd(context.Background(), "\n", func([]byte) error { return nil }, "client", "list")
	if clusterFanoutCommandError(cmd) == nil {
		t.Fatal("clusterFanoutCommandError = nil, want a fan-out rejection")
	}
}

// Errors that happen after the reply was read in full must not drive retries,
// redirects or failover, whatever they wrap.
func TestStreamingReplyLevelErrorsAreTerminal(t *testing.T) {
	moved := proto.RedisError("MOVED 3999 127.0.0.1:6381")
	for name, err := range map[string]error{
		"callback wraps EOF":      &StreamingCallbackError{Err: io.EOF},
		"callback wraps MOVED":    &StreamingCallbackError{Err: moved},
		"callback wraps READONLY": &StreamingCallbackError{Err: proto.RedisError("READONLY You can't write against a read only replica.")},
		"callback wraps timeout":  &StreamingCallbackError{Err: context.DeadlineExceeded},
		"segment too long":        ErrStreamingSegmentTooLong,
	} {
		t.Run(name, func(t *testing.T) {
			if shouldRetry(err, true) {
				t.Error("shouldRetry = true")
			}
			if isBadConn(err, true, "127.0.0.1:6379") {
				t.Error("isBadConn = true")
			}
			if !isRedisError(err) {
				t.Error("isRedisError = false (pipelines would drop the conn)")
			}
			if got := classifyOutcome(err, true); got != outcomeSuccess {
				t.Errorf("classifyOutcome = %v, want outcomeSuccess", got)
			}
			c := &ClusterClient{}
			if r := c.checkMovedErr(context.Background(), nil, nil, err, ""); r.node != nil || r.cmds != nil {
				t.Errorf("checkMovedErr redirected: %+v", r)
			}
		})
	}
	if (&StreamingCallbackError{}).Error() == "" {
		t.Error("zero StreamingCallbackError has an empty message")
	}
}

func TestStreamingCmdCloneAndFanout(t *testing.T) {
	cmd := newStreamingCmd(context.Background(), "\n", func([]byte) error { return nil }, "client", "list")
	clone, ok := cmd.Clone().(*StreamingCmd)
	if !ok || clone == cmd || string(clone.delimiter) != "\n" || clone.fn == nil {
		t.Fatalf("Clone = %#v", cmd.Clone())
	}
	if !isStreamingCmd(cmd) {
		t.Fatal("isStreamingCmd = false")
	}
	if !cmd.NoRetry() {
		t.Fatal("NoRetry = false")
	}
}
