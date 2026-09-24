package redis

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// applyCachedFast decodes the simple reply shapes without allocating a
// proto.Reader. It is only safe if it agrees with the generic path on every
// input, including the ones it declines: a fast path that silently decodes
// something differently from the reader would corrupt a cached read.
//
// This drives both paths over the same payloads and compares value and error.
func TestApplyCachedFastMatchesGenericReader(t *testing.T) {
	ctx := context.Background()
	big := strings.Repeat("x", 600)

	cases := []struct {
		name string
		raw  string
	}{
		{"empty bulk", "$0\r\n\r\n"},
		{"short bulk", "$5\r\nhello\r\n"},
		{"bulk with CR inside", "$4\r\na\rb\r\n"},
		{"bulk with LF inside", "$4\r\na\nb\r\n"},
		{"bulk with CRLF inside", "$6\r\na\r\nbc\r\n"},
		{"binary bulk", "$3\r\n\x00\x01\x02\r\n"},
		{"large bulk", fmt.Sprintf("$%d\r\n%s\r\n", len(big), big)},
		{"resp2 null bulk", "$-1\r\n"},
		{"resp3 null", "_\r\n"},
		{"status ok", "+OK\r\n"},
		{"status with space", "+string\r\n"},
		{"empty status", "+\r\n"},
		// Shapes the fast path must DECLINE rather than misread.
		{"integer", ":42\r\n"},
		{"array", "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{"resp3 double", ",3.14\r\n"},
		{"resp3 bool", "#t\r\n"},
		{"verbatim", "=9\r\ntxt:hello\r\n"},
		{"error", "-ERR nope\r\n"},
		// Malformed / truncated: must not panic, must not disagree.
		{"truncated bulk", "$5\r\nhel\r\n"},
		{"bulk length lies long", "$99\r\nhello\r\n"},
		{"no terminator", "$5\r\nhello"},
		{"bad length", "$abc\r\nhello\r\n"},
		{"just crlf", "\r\n"},
		{"empty", ""},
	}

	for _, tc := range cases {
		for _, kind := range []string{"string", "status"} {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				raw := []byte(tc.raw)

				mk := func() Cmder {
					if kind == "string" {
						return NewStringCmd(ctx, "get", "k")
					}
					return NewStatusCmd(ctx, "type", "k")
				}

				// Generic path, verbatim from applyCachedReply's fallback.
				gen := mk()
				genErr := gen.readReply(proto.NewReaderSize(bytes.NewReader(raw), len(raw)+1))

				// Fast path via applyCachedReply (falls back internally when it
				// declines, so this is what production actually runs).
				fast := mk()
				fastErr := applyCachedReply(fast, raw)

				sameErr := (genErr == nil) == (fastErr == nil)
				if sameErr && genErr != nil {
					// Both non-nil: Nil must match Nil exactly; other errors
					// only have to agree on being errors, since the generic
					// reader's messages are its own.
					sameErr = (genErr == Nil) == (fastErr == Nil)
				}
				if !sameErr {
					t.Fatalf("error mismatch: generic=%v fast=%v", genErr, fastErr)
				}

				var genVal, fastVal string
				switch c := gen.(type) {
				case *StringCmd:
					genVal = c.Val()
					fastVal = fast.(*StringCmd).Val()
				case *StatusCmd:
					genVal = c.Val()
					fastVal = fast.(*StatusCmd).Val()
				}
				if genVal != fastVal {
					t.Fatalf("value mismatch: generic=%q fast=%q", genVal, fastVal)
				}
			})
		}
	}
}

// The fast path must not alias the cache entry's bytes into the command: the
// slice it is handed belongs to the cache (see LocalCache.getShared), so a
// decoded value that shared its backing array would change under the caller
// the next time that entry was refetched.
func TestApplyCachedFastCopiesTheValue(t *testing.T) {
	ctx := context.Background()
	raw := []byte("$5\r\nhello\r\n")

	cmd := NewStringCmd(ctx, "get", "k")
	if err := applyCachedReply(cmd, raw); err != nil {
		t.Fatalf("applyCachedReply: %v", err)
	}
	if got := cmd.Val(); got != "hello" {
		t.Fatalf("value = %q, want hello", got)
	}

	// Scribble over the source the way a refetch writing into a reused buffer
	// would. The decoded string must be unaffected.
	for i := range raw {
		raw[i] = 'Z'
	}
	if got := cmd.Val(); got != "hello" {
		t.Fatalf("decoded value aliases the source: became %q after overwrite", got)
	}
}

// getShared hands out the entry's own slice. Prove it really is shared (that
// is the point -- no copy) and that Get still copies, so the public contract
// is unchanged for third-party callers.
func TestGetSharedAliasesAndGetCopies(t *testing.T) {
	ctx := context.Background()
	lc := NewLocalCache(CacheConfig{MaxEntries: 8})
	tok, fetch := lc.Reserve("ck", []string{"rk"})
	if tok == 0 || !fetch {
		t.Fatal("Reserve did not hand out a token")
	}
	if !lc.fulfill("ck", tok, 0, []byte("payload")) {
		t.Fatal("fulfill failed")
	}

	a, ok := lc.getShared(ctx, "ck")
	if !ok {
		t.Fatal("getShared missed")
	}
	b, ok := lc.getShared(ctx, "ck")
	if !ok {
		t.Fatal("second getShared missed")
	}
	if &a[0] != &b[0] {
		t.Fatal("getShared copied; it must alias the entry")
	}

	c, ok := lc.Get(ctx, "ck")
	if !ok {
		t.Fatal("Get missed")
	}
	if &c[0] == &a[0] {
		t.Fatal("Get aliased the entry; the public contract must keep copying")
	}
	if string(c) != "payload" {
		t.Fatalf("Get returned %q", c)
	}
}
