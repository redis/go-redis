package redis_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// countingKeyArg is a key argument the cache cannot render in wire form, so
// the command is uncacheable. It counts MarshalBinary calls.
type countingKeyArg struct {
	key   string
	calls int
}

func (a *countingKeyArg) MarshalBinary() ([]byte, error) {
	a.calls++
	return []byte(a.key), nil
}

// TestCSCUncacheableKeyMarshaledOnce pins the key-type gate order in
// processCached: a command whose key the cache cannot render must fall back
// before the cache key is built. Building the cache key encodes every
// argument, so a BinaryMarshaler key would be marshaled once for the cache
// key and again for the request.
func TestCSCUncacheableKeyMarshaledOnce(t *testing.T) {
	if err := probeRedis(cscNativeAddr()); err != nil {
		t.Skipf("redis not available at %s: %v", cscNativeAddr(), err)
	}
	cache := redis.NewLocalCache(redis.CacheConfig{MaxEntries: 32})
	c := redis.NewClient(&redis.Options{
		Addr:            cscNativeAddr(),
		Protocol:        3,
		ClientSideCache: cache,
		PoolSize:        1,
	})
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", cscNativeAddr(), err)
	}
	plain := redis.NewClient(&redis.Options{Addr: cscNativeAddr()})
	t.Cleanup(func() { _ = plain.Close() })
	unavailable, err := probeClientTracking(ctx, plain)
	if unavailable {
		t.Skipf("CLIENT TRACKING is unavailable: %v", err)
	}
	if err != nil {
		t.Fatalf("probe CLIENT TRACKING: %v", err)
	}

	key := "csc-keygate:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := plain.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	t.Cleanup(func() { _ = plain.Del(context.Background(), key).Err() })

	arg := &countingKeyArg{key: key}
	cmd := redis.NewStringCmd(ctx, "get", arg)
	if err := c.Process(ctx, cmd); err != nil {
		t.Fatalf("GET: %v", err)
	}
	if got := cmd.Val(); got != "v" {
		t.Fatalf("GET = %q, want v", got)
	}
	if arg.calls != 1 {
		t.Fatalf("MarshalBinary called %d times, want 1 (the request only)", arg.calls)
	}
	if n := cache.Len(); n != 0 {
		t.Fatalf("cache.Len = %d, want 0: an unrenderable key must not be cached", n)
	}
}
