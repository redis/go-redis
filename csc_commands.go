package redis

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9/internal/proto"
)

// defaultCacheableCommands is the allow-list of read-only, deterministic
// commands whose responses may be stored in the client-side cache. Keys are
// lowercase to match baseCmd.Name() on the hot path.
var defaultCacheableCommands = map[string]struct{}{
	// String commands
	"get": {}, "mget": {}, "getbit": {}, "getrange": {},
	"strlen": {}, "substr": {},
	// Hash commands
	"hget": {}, "hgetall": {}, "hmget": {},
	"hkeys": {}, "hvals": {}, "hlen": {},
	"hexists": {}, "hstrlen": {},
	// List commands
	"lindex": {}, "llen": {}, "lpos": {}, "lrange": {},
	// Set commands
	"scard": {}, "sismember": {}, "smembers": {}, "smismember": {},
	"sdiff": {}, "sinter": {}, "sintercard": {}, "sunion": {},
	// Sorted-set commands
	"zcard": {}, "zcount": {}, "zlexcount": {}, "zmscore": {},
	"zrange": {}, "zrangebylex": {}, "zrangebyscore": {},
	"zrank": {}, "zrevrange": {}, "zrevrangebylex": {},
	"zrevrangebyscore": {}, "zrevrank": {}, "zscore": {},
	"zdiff": {}, "zinter": {}, "zunion": {},
	// Bit commands
	"bitcount": {}, "bitfield_ro": {}, "bitpos": {},
	// Key/generic commands
	"exists": {}, "type": {}, "sort_ro": {}, "lcs": {},
	// Geo commands
	"geodist": {}, "geohash": {}, "geopos": {}, "geosearch": {},
	"georadiusbymember_ro": {}, "georadius_ro": {},
	// Stream commands. XREAD is deliberately excluded: it supports BLOCK, and
	// its $/+ IDs are state-relative, so identical args are not deterministic.
	"xlen": {}, "xpending": {}, "xrange": {}, "xrevrange": {},
	// JSON (RedisJSON) commands
	"json.get": {}, "json.mget": {}, "json.arrindex": {}, "json.arrlen": {},
	"json.objkeys": {}, "json.objlen": {}, "json.resp": {},
	"json.strlen": {}, "json.type": {},
	// TimeSeries commands
	"ts.get": {}, "ts.info": {}, "ts.range": {}, "ts.revrange": {},
}

// isCacheable reports whether cmd is eligible for client-side caching: its
// name is on the allow-list and it operates on at least one key.
func isCacheable(cmd Cmder) bool {
	if _, ok := defaultCacheableCommands[cmd.Name()]; !ok {
		return false
	}
	// SORT_RO ... BY/GET reads pattern keys that extractRedisKeys can't
	// enumerate, so its invalidations would be dropped and the result go stale.
	// Plain SORT_RO is fine.
	if cmd.Name() == "sort_ro" && sortROHasByGet(cmd.Args()) {
		return false
	}
	return cmdFirstKeyPos(cmd) != 0
}

// sortROHasByGet reports whether a SORT_RO invocation uses BY or GET
// (case-insensitive), scanning past the command name and key.
func sortROHasByGet(args []interface{}) bool {
	if len(args) < 3 {
		return false
	}
	for _, a := range args[2:] {
		s, ok := a.(string)
		if !ok {
			continue
		}
		if strings.EqualFold(s, "by") || strings.EqualFold(s, "get") {
			return true
		}
	}
	return false
}

// buildCacheKey returns the RESP-encoded form of the command's argument list,
// used as a collision-free canonical cache key. ok is false when the writer
// cannot marshal the arguments, in which case the caller must skip caching
// rather than bucket the command under an empty key.
func buildCacheKey(cmd Cmder) (string, bool) {
	args := cmd.Args()
	if len(args) == 0 {
		return "", false
	}
	var buf bytes.Buffer
	if err := proto.NewWriter(&buf).WriteArgs(args); err != nil {
		return "", false
	}
	return buf.String(), true
}

// extractRedisKeys returns the Redis key arguments from cmd. The result
// populates CacheEntry.RedisKeys so the cache can map incoming invalidations
// back to affected entries.
func extractRedisKeys(cmd Cmder) []string {
	firstKey := cmdFirstKeyPos(cmd)
	if firstKey == 0 {
		return nil
	}

	argsLen := len(cmd.Args())
	if firstKey >= argsLen {
		return nil
	}

	switch cmd.Name() {
	// All remaining args from firstKeyPos are keys.
	case "mget", "exists", "sdiff", "sinter", "sunion":
		keys := make([]string, 0, argsLen-firstKey)
		for i := firstKey; i < argsLen; i++ {
			keys = append(keys, cmd.stringArg(i))
		}
		return keys

	// Numkeys pattern: numkeys at args[1], keys from args[2].
	case "sintercard", "zdiff", "zinter", "zunion":
		if argsLen < 3 {
			return nil
		}
		numKeys, err := strconv.Atoi(cmd.stringArg(1))
		if err != nil || numKeys <= 0 {
			return nil
		}
		keys := make([]string, 0, numKeys)
		for i := 2; i < 2+numKeys && i < argsLen; i++ {
			keys = append(keys, cmd.stringArg(i))
		}
		return keys

	// LCS: exactly two consecutive keys starting at firstKeyPos.
	case "lcs":
		if firstKey+1 >= argsLen {
			return nil
		}
		return []string{cmd.stringArg(firstKey), cmd.stringArg(firstKey + 1)}

	// JSON.MGET: keys from firstKeyPos to second-to-last (last arg is the
	// JSON path, not a key).
	case "json.mget":
		lastKey := argsLen - 2
		if lastKey < firstKey {
			return nil
		}
		keys := make([]string, 0, lastKey-firstKey+1)
		for i := firstKey; i <= lastKey; i++ {
			keys = append(keys, cmd.stringArg(i))
		}
		return keys
	}

	// Single key at firstKeyPos (GET, HGET, LRANGE, ...).
	return []string{cmd.stringArg(firstKey)}
}
