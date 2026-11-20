package proto_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/redis/go-redis/v9/internal/proto"
)

func readChunked(rd *proto.Reader) (string, int, error) {
	var got bytes.Buffer
	calls := 0
	err := rd.ReadChunked(func(chunk []byte) error {
		calls++
		got.Write(chunk)
		return nil
	})
	return got.String(), calls, err
}

func expectOK(t *testing.T, rd *proto.Reader) {
	t.Helper()
	if s, err := rd.ReadString(); err != nil || s != "OK" {
		t.Fatalf("follow-up reply: %q, %v (stream misaligned)", s, err)
	}
}

func TestReadChunked(t *testing.T) {
	payload := strings.Repeat("id=1 addr=127.0.0.1:1234 name=x\n", 5000)

	encodings := map[string]string{
		"bulk":     fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload),
		"verbatim": fmt.Sprintf("=%d\r\ntxt:%s\r\n", len(payload)+4, payload),
	}
	readers := map[string]func(io.Reader) io.Reader{
		"plain":    func(r io.Reader) io.Reader { return r },
		"one-byte": iotest.OneByteReader,
	}

	for name, wire := range encodings {
		for rname, wrap := range readers {
			t.Run(name+"/"+rname, func(t *testing.T) {
				rd := proto.NewReader(wrap(strings.NewReader(wire + "+OK\r\n")))
				got, calls, err := readChunked(rd)
				if err != nil {
					t.Fatalf("ReadChunked: %v", err)
				}
				if got != payload {
					t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
				}
				if calls < 2 {
					t.Fatalf("fn called %d times, want several chunks", calls)
				}
				expectOK(t, rd)
			})
		}
	}
}

func TestReadChunkedEmpty(t *testing.T) {
	for name, wire := range map[string]string{
		"bulk":     "$0\r\n\r\n+OK\r\n",
		"verbatim": "=4\r\ntxt:\r\n+OK\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			rd := proto.NewReader(strings.NewReader(wire))
			got, calls, err := readChunked(rd)
			if err != nil || got != "" || calls != 0 {
				t.Fatalf("got %q calls=%d err=%v, want empty, no calls, nil", got, calls, err)
			}
			expectOK(t, rd)
		})
	}
}

func TestReadChunkedFnErrorDrains(t *testing.T) {
	payload := strings.Repeat("x", 100_000)
	wire := fmt.Sprintf("$%d\r\n%s\r\n+OK\r\n", len(payload), payload)
	rd := proto.NewReader(strings.NewReader(wire))

	sentinel := errors.New("stop")
	calls := 0
	err := rd.ReadChunked(func(chunk []byte) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times after error, want 1", calls)
	}
	expectOK(t, rd)
}

func TestReadChunkedRejects(t *testing.T) {
	tests := map[string]string{
		"array":              "*1\r\n$1\r\nx\r\n",
		"verbatim too short": "=2\r\nab\r\n",
		"bad tag sep":        "=8\r\ntxtXdata\r\n",
		"eof in payload":     "$10\r\nabc",
		"eof in tag":         "=10\r\ntx",
		"eof before crlf":    "$3\r\nabc",
	}
	for name, wire := range tests {
		t.Run(name, func(t *testing.T) {
			rd := proto.NewReader(strings.NewReader(wire))
			err := rd.ReadChunked(func(chunk []byte) error { return nil })
			if err == nil {
				t.Fatal("ReadChunked succeeded, want error")
			}
			var redisErr proto.RedisError
			if errors.As(err, &redisErr) {
				t.Fatalf("err %v is a RedisError; malformed replies must drop the conn", err)
			}
		})
	}
}

func TestReadChunkedNil(t *testing.T) {
	for name, wire := range map[string]string{"bulk": "$-1\r\n", "resp3": "_\r\n"} {
		t.Run(name, func(t *testing.T) {
			rd := proto.NewReader(strings.NewReader(wire + "+OK\r\n"))
			err := rd.ReadChunked(func(chunk []byte) error { return nil })
			if !errors.Is(err, proto.Nil) {
				t.Fatalf("err = %v, want Nil", err)
			}
			expectOK(t, rd)
		})
	}
}

func TestReadChunkedErrorReply(t *testing.T) {
	rd := proto.NewReader(strings.NewReader("-ERR unknown command\r\n"))
	err := rd.ReadChunked(func(chunk []byte) error {
		t.Fatal("fn must not be called for an error reply")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("err = %v, want server error", err)
	}
}
