package redis

import (
	"context"
	"io"
	"strings"
	"testing"
)

// hgetallReader returns a custom reader that parses an HGETALL reply into
// dst using only caller-owned buffers, the way a zero-allocation consumer
// would. ReadMapLen accepts both the RESP3 map reply and the RESP2 flat
// array reply.
func hgetallReader(dst map[string]string) func(*Reader) error {
	return func(rd *Reader) error {
		n, err := rd.ReadMapLen()
		if err != nil {
			return err
		}
		buf := make([]byte, 64)
		for i := 0; i < n; i++ {
			keyLen, err := rd.ReadStringInto(buf)
			if err != nil {
				return err
			}
			key := string(buf[:keyLen])
			valLen, err := rd.ReadStringInto(buf)
			if err != nil {
				return err
			}
			dst[key] = string(buf[:valLen])
		}
		return nil
	}
}

func TestCustomCmdReadReply(t *testing.T) {
	replies := map[string]string{
		"resp2 flat array": "*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
		"resp3 map":        "%2\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
	}
	want := map[string]string{"f1": "v1", "f2": "v2"}

	for name, reply := range replies {
		t.Run(name, func(t *testing.T) {
			rd := NewReader(strings.NewReader(reply))
			got := make(map[string]string)
			cmd := NewCustomCmd(context.Background(), hgetallReader(got), "hgetall", "key")

			if err := cmd.readReply(rd); err != nil {
				t.Fatalf("readReply: %v", err)
			}
			if len(got) != len(want) {
				t.Fatalf("got %d fields, want %d", len(got), len(want))
			}
			for k, v := range want {
				if got[k] != v {
					t.Errorf("field %q: got %q, want %q", k, got[k], v)
				}
			}
			// The reader must be fully consumed, or the connection would be
			// left desynchronised for the next command.
			if _, err := rd.ReadLine(); err != io.EOF {
				t.Errorf("reply not fully consumed: ReadLine returned %v, want io.EOF", err)
			}
		})
	}
}

func TestCustomCmdCloneDrainsReply(t *testing.T) {
	reply := "*2\r\n$2\r\nf1\r\n$2\r\nv1\r\n"
	rd := NewReader(strings.NewReader(reply))

	got := make(map[string]string)
	cmd := NewCustomCmd(context.Background(), hgetallReader(got), "hgetall", "key")
	clone := cmd.Clone()

	if err := clone.readReply(rd); err == nil {
		t.Fatal("cloned CustomCmd readReply should error")
	}
	if len(got) != 0 {
		t.Errorf("cloned CustomCmd must not invoke the reader function, parsed %d fields", len(got))
	}
	// The clone must still have drained the reply to keep the connection
	// aligned.
	if _, err := rd.ReadLine(); err != io.EOF {
		t.Errorf("reply not drained: ReadLine returned %v, want io.EOF", err)
	}
}

func TestCustomCmdNoRetry(t *testing.T) {
	cmd := NewCustomCmd(context.Background(), func(*Reader) error { return nil }, "hgetall", "key")
	if !cmd.NoRetry() {
		t.Error("CustomCmd must not be retried: the reader function writes into caller-owned state")
	}
}
