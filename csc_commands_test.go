package redis

import (
	"context"
	"testing"
)

// helper to create a Cmd with the given args.
func makeCmd(args ...interface{}) Cmder {
	return NewCmd(context.Background(), args...)
}

// --- isCacheable -----------------------------------------------------------

func TestIsCacheable_AllowedCommands(t *testing.T) {
	allowed := []string{
		"GET", "MGET", "HGET", "HMGET", "HGETALL",
		"HKEYS", "HVALS", "HLEN", "HEXISTS", "HSTRLEN",
		"LINDEX", "LLEN", "LPOS", "LRANGE",
		"SCARD", "SISMEMBER", "SMEMBERS", "SMISMEMBER",
		"SDIFF", "SINTER", "SINTERCARD", "SUNION",
		"ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZMSCORE",
		"ZRANGE", "ZRANGEBYLEX", "ZRANGEBYSCORE",
		"ZRANK", "ZREVRANGE", "ZREVRANGEBYLEX",
		"ZREVRANGEBYSCORE", "ZREVRANK", "ZSCORE",
		"ZDIFF", "ZINTER", "ZUNION",
		"STRLEN", "GETBIT", "GETRANGE", "SUBSTR",
		"BITCOUNT", "BITFIELD_RO", "BITPOS",
		"EXISTS", "TYPE", "SORT_RO", "LCS",
		"GEODIST", "GEOHASH", "GEOPOS", "GEOSEARCH",
		"GEORADIUSBYMEMBER_RO", "GEORADIUS_RO",
		"XLEN", "XPENDING", "XRANGE", "XREAD", "XREVRANGE",
		"JSON.GET", "JSON.MGET", "JSON.ARRINDEX", "JSON.ARRLEN",
		"JSON.OBJKEYS", "JSON.OBJLEN", "JSON.RESP",
		"JSON.STRLEN", "JSON.TYPE",
		"TS.GET", "TS.INFO", "TS.RANGE", "TS.REVRANGE",
	}
	for _, name := range allowed {
		// Use lower-case name as first arg (matching how go-redis sends commands)
		cmd := makeCmd(name, "mykey")
		if !isCacheable(cmd) {
			t.Errorf("expected %q to be cacheable", name)
		}
	}
}

func TestIsCacheable_CaseInsensitive(t *testing.T) {
	for _, name := range []string{"get", "Get", "GET", "gEt"} {
		cmd := makeCmd(name, "k")
		if !isCacheable(cmd) {
			t.Errorf("expected %q to be cacheable (case-insensitive)", name)
		}
	}
}

func TestIsCacheable_WriteCommandsRejected(t *testing.T) {
	writes := []string{"SET", "DEL", "HSET", "LPUSH", "SADD", "ZADD", "EXPIRE", "FLUSHDB"}
	for _, name := range writes {
		cmd := makeCmd(name, "k")
		if isCacheable(cmd) {
			t.Errorf("expected %q to NOT be cacheable", name)
		}
	}
}

func TestIsCacheable_KeylessCommandRejected(t *testing.T) {
	// PING has no keys; even if someone added it to the allow-list it
	// should be rejected because cmdFirstKeyPos returns 0.
	cmd := makeCmd("ping")
	if isCacheable(cmd) {
		t.Error("expected keyless command PING to NOT be cacheable")
	}
}

func TestIsCacheable_EmptyArgs(t *testing.T) {
	cmd := makeCmd()
	if isCacheable(cmd) {
		t.Error("expected empty command to NOT be cacheable")
	}
}

// --- buildCacheKey ---------------------------------------------------------

func TestBuildCacheKey_SimpleGet(t *testing.T) {
	cmd := makeCmd("GET", "foo")
	key, ok := buildCacheKey(cmd)
	if !ok || key == "" {
		t.Fatal("expected non-empty cache key")
	}
	// Same command must produce identical keys.
	if key2, _ := buildCacheKey(makeCmd("GET", "foo")); key != key2 {
		t.Errorf("identical commands produced different keys: %q vs %q", key, key2)
	}
}

func TestBuildCacheKey_DifferentArgsDiffer(t *testing.T) {
	k1, _ := buildCacheKey(makeCmd("GET", "foo"))
	k2, _ := buildCacheKey(makeCmd("GET", "bar"))
	if k1 == k2 {
		t.Error("different keys must produce different cache keys")
	}
}

func TestBuildCacheKey_CollisionSafety(t *testing.T) {
	// "a|b" as one arg vs "a" and "b" as two args must differ.
	k1, _ := buildCacheKey(makeCmd("GET", "a|b"))
	k2, _ := buildCacheKey(makeCmd("GET", "a", "b"))
	if k1 == k2 {
		t.Error("length-prefixing should prevent separator collision")
	}
}

func TestBuildCacheKey_BinaryData(t *testing.T) {
	cmd := makeCmd("GET", []byte{0x00, 0x01, 0xff})
	key, ok := buildCacheKey(cmd)
	if !ok || key == "" {
		t.Fatal("expected non-empty cache key for binary argument")
	}
}

func TestBuildCacheKey_MultiKey(t *testing.T) {
	k1, _ := buildCacheKey(makeCmd("MGET", "a", "b"))
	k2, _ := buildCacheKey(makeCmd("MGET", "a", "b", "c"))
	if k1 == k2 {
		t.Error("different arg counts must produce different cache keys")
	}
}

func TestBuildCacheKey_EmptyArgs(t *testing.T) {
	cmd := makeCmd()
	if key, ok := buildCacheKey(cmd); ok || key != "" {
		t.Errorf("expected empty cache key for no-args command, got %q (ok=%v)", key, ok)
	}
}

// --- extractRedisKeys ------------------------------------------------------

func TestExtractRedisKeys_SingleKey(t *testing.T) {
	cmd := makeCmd("GET", "mykey")
	keys := extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "mykey" {
		t.Errorf("expected [mykey], got %v", keys)
	}
}

func TestExtractRedisKeys_SingleKeyWithExtraArgs(t *testing.T) {
	// LRANGE has one key followed by start/stop — only the key should be extracted.
	cmd := makeCmd("LRANGE", "mylist", "0", "10")
	keys := extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "mylist" {
		t.Errorf("LRANGE: expected [mylist], got %v", keys)
	}

	// HGET has one key followed by a field name.
	cmd = makeCmd("HGET", "myhash", "field1")
	keys = extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "myhash" {
		t.Errorf("HGET: expected [myhash], got %v", keys)
	}

	// ZCOUNT has one key followed by min/max.
	cmd = makeCmd("ZCOUNT", "myset", "-inf", "+inf")
	keys = extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "myset" {
		t.Errorf("ZCOUNT: expected [myset], got %v", keys)
	}

	// GETRANGE has one key followed by start/end offsets.
	cmd = makeCmd("GETRANGE", "mystr", "0", "5")
	keys = extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "mystr" {
		t.Errorf("GETRANGE: expected [mystr], got %v", keys)
	}
}

func TestExtractRedisKeys_MultiKey(t *testing.T) {
	cmd := makeCmd("MGET", "a", "b", "c")
	keys := extractRedisKeys(cmd)
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(keys), keys)
	}
	want := []string{"a", "b", "c"}
	for i, k := range keys {
		if k != want[i] {
			t.Errorf("key[%d] = %q, want %q", i, k, want[i])
		}
	}
}

func TestExtractRedisKeys_MultiKeyExists(t *testing.T) {
	cmd := makeCmd("EXISTS", "k1", "k2", "k3")
	keys := extractRedisKeys(cmd)
	if len(keys) != 3 {
		t.Fatalf("EXISTS: expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestExtractRedisKeys_NumKeysPattern(t *testing.T) {
	// ZDIFF numkeys key [key ...]
	cmd := makeCmd("ZDIFF", 2, "zs1", "zs2")
	cmd.(*Cmd).SetFirstKeyPos(2)
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "zs1" || keys[1] != "zs2" {
		t.Errorf("ZDIFF: expected [zs1 zs2], got %v", keys)
	}

	// SINTERCARD numkeys key [key ...] LIMIT limit
	cmd = makeCmd("SINTERCARD", 2, "s1", "s2", "LIMIT", 10)
	keys = extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "s1" || keys[1] != "s2" {
		t.Errorf("SINTERCARD: expected [s1 s2], got %v", keys)
	}
}

func TestExtractRedisKeys_LCS(t *testing.T) {
	cmd := makeCmd("LCS", "key1", "key2")
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "key1" || keys[1] != "key2" {
		t.Errorf("LCS: expected [key1 key2], got %v", keys)
	}
}

func TestExtractRedisKeys_JSONMGet(t *testing.T) {
	// JSON.MGET key [key ...] path
	cmd := makeCmd("JSON.MGET", "j1", "j2", "$.name")
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "j1" || keys[1] != "j2" {
		t.Errorf("JSON.MGET: expected [j1 j2], got %v", keys)
	}
}

func TestExtractRedisKeys_XREAD(t *testing.T) {
	// XREAD COUNT 10 STREAMS stream1 stream2 0 0
	cmd := makeCmd("XREAD", "COUNT", 10, "STREAMS", "s1", "s2", "0", "0")
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "s1" || keys[1] != "s2" {
		t.Errorf("XREAD: expected [s1 s2], got %v", keys)
	}
}

func TestExtractRedisKeys_KeylessCommand(t *testing.T) {
	cmd := makeCmd("ping")
	keys := extractRedisKeys(cmd)
	if keys != nil {
		t.Errorf("expected nil for keyless command, got %v", keys)
	}
}
