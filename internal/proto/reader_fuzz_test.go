package proto

import (
	"bytes"
	"strings"
	"testing"
)

func FuzzReadReply(f *testing.F) {
	// Seed corpus with valid RESP3 replies plus unhashable-key maps so the
	// fuzzer exercises every readMap write site (value ok / Nil / error).
	for _, s := range []string{
		"+OK\r\n", ":123\r\n", "-ERR x\r\n",
		"$5\r\nhello\r\n", "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"_\r\n", "#t\r\n", ",3.14\r\n",
		"%1\r\n+k\r\n+v\r\n", "~2\r\n+a\r\n+b\r\n",
		">2\r\n+pub\r\n+msg\r\n",
		"%1\r\n*0\r\n+v\r\n",     // array key, value ok
		"%1\r\n*0\r\n_\r\n",      // array key, Nil value
		"%1\r\n*0\r\n-ERR x\r\n", // array key, RedisError value
		"%1\r\n%0\r\n+v\r\n",     // map key, value ok
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		_, _ = r.ReadReply()
	})
}

// TestReadMapRejectsUnhashableKey verifies that an aggregate (array or map)
// used as a RESP3 map key is rejected with a parse error instead of causing a
// runtime "hash of unhashable type" panic. The key is written into the result
// map at three sites in readMap — on a successful value read, on a Nil value,
// and on a RedisError value — so each is covered here.
func TestReadMapRejectsUnhashableKey(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"array key, value ok", "%1\r\n*0\r\n+v\r\n"},
		{"array key, Nil value", "%1\r\n*0\r\n_\r\n"},
		{"array key, RedisError value", "%1\r\n*0\r\n-ERR boom\r\n"},
		{"map key, value ok", "%1\r\n%0\r\n+v\r\n"},
		{"map key, Nil value", "%1\r\n%0\r\n_\r\n"},
		{"map key, RedisError value", "%1\r\n%0\r\n-ERR boom\r\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReader(bytes.NewReader([]byte(tc.data)))
			_, err := r.ReadReply()
			if err == nil {
				t.Fatal("expected error for unhashable map key, got nil")
			}
			if !strings.Contains(err.Error(), "scalar type") {
				t.Fatalf("expected 'scalar type' in error, got: %v", err)
			}
		})
	}
}
