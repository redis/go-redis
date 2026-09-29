package redis

import (
	"context"
	"testing"
)

// The same key must pick the same engine whatever Go type carries it. A key
// given as []byte or *string used to look keyless and round-robin, so a
// same-key write and read could land on two engines with no order between
// them. An empty-string key used to collide with the keyless sentinel.
func TestFDForRoutesKeyFormsTogether(t *testing.T) {
	ctx := context.Background()
	cl := NewClient(&Options{Addr: "127.0.0.1:6379"})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("no local redis: %v", err)
	}
	ap, err := cl.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:           true,
		NumShards:            8,
		MaxConcurrentBatches: 1,
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	defer ap.Close()
	if len(ap.fds) != 8 {
		t.Fatalf("want 8 engines, got %d", len(ap.fds))
	}

	for i := 0; i < 200; i++ {
		k := "fdkey:" + itoa(i)
		s := k
		want := ap.fdFor(NewCmd(ctx, "get", k))
		for _, arg := range []interface{}{[]byte(k), &s} {
			if got := ap.fdFor(NewCmd(ctx, "get", arg)); got != want {
				t.Fatalf("key %q as %T picked a different engine than as string", k, arg)
			}
		}
	}

	// A raw FCALL routes by its declared key, not by the function name, so it
	// shares an engine with a plain read of that key.
	for i := 0; i < 200; i++ {
		k := "fdfcall:" + itoa(i)
		if ap.fdFor(NewCmd(ctx, "fcall", "myfn", "1", k)) != ap.fdFor(NewCmd(ctx, "get", k)) {
			t.Fatalf("raw FCALL on %q picked a different engine than GET %q", k, k)
		}
	}

	first := ap.fdFor(NewCmd(ctx, "get", ""))
	for i := 0; i < 16; i++ {
		if ap.fdFor(NewCmd(ctx, "set", "", "v")) != first {
			t.Fatal("empty-string key round-robined instead of routing by hash")
		}
	}
}
