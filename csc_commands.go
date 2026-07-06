package redis

import (
	"fmt"
	"strconv"
	"strings"
)

// defaultCacheableCommands is the allow-list of read-only, deterministic
// commands whose responses may be stored in the client-side cache.
// Only commands in this set are eligible for caching (opt-in at the client level).
// This matches the redis-py DEFAULT_ALLOW_LIST.
//
//nolint:unused // Will be used by the CSC integration in _process hot path.
var defaultCacheableCommands = map[string]struct{}{
	// String commands
	"GET": {}, "MGET": {}, "GETBIT": {}, "GETRANGE": {},
	"STRLEN": {}, "SUBSTR": {},
	// Hash commands
	"HGET": {}, "HGETALL": {}, "HMGET": {},
	"HKEYS": {}, "HVALS": {}, "HLEN": {},
	"HEXISTS": {}, "HSTRLEN": {},
	// List commands
	"LINDEX": {}, "LLEN": {}, "LPOS": {}, "LRANGE": {},
	// Set commands
	"SCARD": {}, "SISMEMBER": {}, "SMEMBERS": {}, "SMISMEMBER": {},
	"SDIFF": {}, "SINTER": {}, "SINTERCARD": {}, "SUNION": {},
	// Sorted-set commands
	"ZCARD": {}, "ZCOUNT": {}, "ZLEXCOUNT": {}, "ZMSCORE": {},
	"ZRANGE": {}, "ZRANGEBYLEX": {}, "ZRANGEBYSCORE": {},
	"ZRANK": {}, "ZREVRANGE": {}, "ZREVRANGEBYLEX": {},
	"ZREVRANGEBYSCORE": {}, "ZREVRANK": {}, "ZSCORE": {},
	"ZDIFF": {}, "ZINTER": {}, "ZUNION": {},
	// Bit commands
	"BITCOUNT": {}, "BITFIELD_RO": {}, "BITPOS": {},
	// Key/generic commands
	"EXISTS": {}, "TYPE": {}, "SORT_RO": {}, "LCS": {},
	// Geo commands
	"GEODIST": {}, "GEOHASH": {}, "GEOPOS": {}, "GEOSEARCH": {},
	"GEORADIUSBYMEMBER_RO": {}, "GEORADIUS_RO": {},
	// Stream commands
	"XLEN": {}, "XPENDING": {}, "XRANGE": {}, "XREAD": {}, "XREVRANGE": {},
	// JSON (RedisJSON) commands
	"JSON.GET": {}, "JSON.MGET": {}, "JSON.ARRINDEX": {}, "JSON.ARRLEN": {},
	"JSON.OBJKEYS": {}, "JSON.OBJLEN": {}, "JSON.RESP": {},
	"JSON.STRLEN": {}, "JSON.TYPE": {},
	// TimeSeries commands
	"TS.GET": {}, "TS.INFO": {}, "TS.RANGE": {}, "TS.REVRANGE": {},
}

// cscSeparator is used to join command arguments into a flat cache key.
// The pipe character is chosen because it is unlikely to appear in Redis
// keys under normal usage, and length-prefixing each segment prevents
// collisions even when arguments contain the separator character.
//
//nolint:unused // Will be used by the CSC integration in _process hot path.
const cscSeparator = "|"

// isCacheable reports whether cmd is eligible for client-side caching.
// A command is cacheable when:
//  1. Its uppercased name appears in the defaultCacheableCommands allow-list.
//  2. It operates on at least one key (firstKeyPos > 0).
//
//nolint:unused // Will be used by the CSC integration in _process hot path.
func isCacheable(cmd Cmder) bool {
	name := strings.ToUpper(cmd.Name())
	if _, ok := defaultCacheableCommands[name]; !ok {
		return false
	}
	// Ensure the command actually has keys.
	if cmdFirstKeyPos(cmd) == 0 {
		return false
	}
	return true
}

// buildCacheKey constructs a unique, collision-free string that identifies a
// command together with all of its arguments.
// Format: "len:COMMAND|len:arg1|len:arg2|..."
// Each segment is length-prefixed ("len:value") so that arguments containing
// the separator character or binary data cannot collide with a different
// argument list.  For example, the two calls
//
//	buildCacheKey(["GET", "a|b"])   -> "3:GET|3:a|b"
//	buildCacheKey(["GET", "a", "b"]) -> "3:GET|1:a|1:b"
//
// produce distinct keys.
//
//nolint:unused // Will be used by the CSC integration in _process hot path.
func buildCacheKey(cmd Cmder) string {
	args := cmd.Args()
	if len(args) == 0 {
		return ""
	}

	var b strings.Builder
	// Pre-allocate a reasonable capacity to reduce allocations.
	b.Grow(64)

	for i, arg := range args {
		if i > 0 {
			b.WriteString(cscSeparator)
		}
		s := argToString(arg)
		// Length-prefix each segment for collision safety.
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteByte(':')
		b.WriteString(s)
	}
	return b.String()
}

// extractRedisKeys returns the Redis key arguments from cmd by using the
// command's key-position metadata and per-command knowledge of the argument
// layout.
//
// This is used to populate the CacheEntry.RedisKeys slice so that the
// localCache.byRedisKey index can map server-side invalidation messages
// (which reference Redis keys) back to the affected cache entries.
//
//nolint:unused // Will be used by the CSC integration in _process hot path.
func extractRedisKeys(cmd Cmder) []string {
	firstKey := cmdFirstKeyPos(cmd)
	if firstKey == 0 {
		return nil
	}

	args := cmd.Args()
	if firstKey >= len(args) {
		return nil
	}

	name := strings.ToUpper(cmd.Name())

	switch name {
	// All remaining args from firstKeyPos are keys.
	case "MGET", "EXISTS", "SDIFF", "SINTER", "SUNION":
		keys := make([]string, 0, len(args)-firstKey)
		for i := firstKey; i < len(args); i++ {
			keys = append(keys, argToString(args[i]))
		}
		return keys

	// Numkeys pattern: numkeys at args[1], keys from args[2].
	case "SINTERCARD", "ZDIFF", "ZINTER", "ZUNION":
		if len(args) < 3 {
			return nil
		}
		numKeys, err := strconv.Atoi(argToString(args[1]))
		if err != nil || numKeys <= 0 {
			return nil
		}
		keys := make([]string, 0, numKeys)
		for i := 2; i < 2+numKeys && i < len(args); i++ {
			keys = append(keys, argToString(args[i]))
		}
		return keys

	// LCS: exactly two consecutive keys starting at firstKeyPos.
	case "LCS":
		keys := make([]string, 0, 2)
		for i := firstKey; i < firstKey+2 && i < len(args); i++ {
			keys = append(keys, argToString(args[i]))
		}
		return keys

	// JSON.MGET: keys from firstKeyPos to second-to-last (last arg is the
	// JSON path, not a key).
	case "JSON.MGET":
		lastKey := len(args) - 2
		if lastKey < firstKey {
			lastKey = firstKey
		}
		keys := make([]string, 0, lastKey-firstKey+1)
		for i := firstKey; i <= lastKey; i++ {
			keys = append(keys, argToString(args[i]))
		}
		return keys

	// XREAD: keys appear after the STREAMS keyword; the second half of the
	// remaining args are stream IDs, not keys.
	case "XREAD":
		streamsIdx := -1
		for i, arg := range args {
			if strings.ToUpper(argToString(arg)) == "STREAMS" {
				streamsIdx = i
				break
			}
		}
		if streamsIdx < 0 || streamsIdx >= len(args)-1 {
			return nil
		}
		remaining := len(args) - streamsIdx - 1
		numStreams := remaining / 2
		if numStreams <= 0 {
			return nil
		}
		keys := make([]string, numStreams)
		for i := 0; i < numStreams; i++ {
			keys[i] = argToString(args[streamsIdx+1+i])
		}
		return keys
	}

	// Default: single key at firstKeyPos.
	// This is correct for the majority of cacheable commands (GET, HGET,
	// LRANGE, ZCARD, ZCOUNT, BITCOUNT, etc.) which have exactly one key
	// followed by non-key arguments (field names, indices, scores, etc.).
	return []string{argToString(args[firstKey])}
}

// argToString converts a command argument to its string representation.
//
//nolint:unused // Will be used by the CSC integration in _process hot path.
func argToString(arg interface{}) string {
	switch v := arg.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}
