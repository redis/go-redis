package redis

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/proto"
)

// A command may be cached only when its COMMAND metadata shows it is read-only,
// takes keys, is deterministic, does not run scripts, and is not marked
// dont_cache. Unknown commands are never cached. Metadata comes from the
// generated cscCommandTable with cscCommandOverrides overlaid at init.

//go:generate go run ./internal/csccmdgen -addr localhost:6379 -out csc_command_table.go

// cscCmdBits holds the command flags and tips the eligibility check uses,
// named after the COMMAND metadata they come from.
type cscCmdBits uint8

const (
	cscFlagReadonly cscCmdBits = 1 << iota
	cscFlagScriptRunner
	cscFlagBlocking
	cscTipNondeterministicOutput
	cscTipDontCache
	// cscHasKeySpec: the command has at least one complete key spec.
	// Incomplete or not_key specs don't count — they can't prove which keys
	// the command reads.
	cscHasKeySpec
)

// cscKeyExtract says how extractRedisKeys finds a command's key arguments.
// None means the keys can't be listed reliably, so the command is never
// cached even if it looks eligible.
type cscKeyExtract uint8

const (
	cscKeyExtractNone cscKeyExtract = iota
	// Keys at firstKey..lastKey every step args; negative lastKey counts
	// from the end.
	cscKeyExtractRange
	// The arg at numkeysAt holds the key count; keys start at firstKey
	// (ZDIFF-style "numkeys key ...").
	cscKeyExtractKeynum
)

// cscCommandMeta is the metadata for one command, keyed by its lowercase
// name ("get", "json.mget", "memory|usage").
type cscCommandMeta struct {
	bits    cscCmdBits
	extract cscKeyExtract

	firstKey  int16
	lastKey   int16
	step      int16
	numkeysAt int16
}

// cscCommandOverrides excludes commands whose server metadata or server-side
// invalidation delivery is known to be broken. Entries must use leaf names
// ("parent|child" for subcommands) and carry only negative bits, so an
// override can never make a command cacheable (TestCSCOverrides enforces
// this). Remove an entry only after a tracking probe shows the server
// invalidates correctly (tracked read -> external write -> expect a push).
var cscCommandOverrides = map[string]cscCommandMeta{
	// Read-only on paper, but its purpose is to bump key LRU/LFU state.
	"touch": {bits: cscTipDontCache},

	// Random output; the server doesn't tag it yet (its siblings are tagged).
	"vrandmember": {bits: cscTipNondeterministicOutput},

	// Flagged readonly but actually rewrites the filter; caching its OK reply
	// would suppress repeated compactions.
	"cf.compact": {bits: cscTipDontCache},

	// The module declares one key but the command reads N, and the server
	// tracks only that first key: writes to the other keys never send an
	// invalidation.
	"json.mget": {bits: cscTipDontCache},

	// The server registers no tracking for any of their series: writes never
	// send an invalidation.
	"ts.nrange":    {bits: cscTipDontCache},
	"ts.nrevrange": {bits: cscTipDontCache},

	// Replies include consumer-group state, but group changes (XGROUP
	// CREATE/SETID) never invalidate the stream key.
	"xinfo|stream": {bits: cscTipDontCache},
	"xinfo|groups": {bits: cscTipDontCache},
}

// cscNegativeBits are the bits that each rule out caching. Overrides may
// carry only these.
const cscNegativeBits = cscFlagScriptRunner | cscFlagBlocking |
	cscTipNondeterministicOutput | cscTipDontCache

// cscResolvedCommandTable is the generated table with overrides applied,
// built once at init and read lock-free afterwards.
var cscResolvedCommandTable = func() map[string]cscCommandMeta {
	merged := make(map[string]cscCommandMeta, len(cscCommandTable)+len(cscCommandOverrides))
	for name, meta := range cscCommandTable {
		merged[name] = meta
	}
	for name, override := range cscCommandOverrides {
		merged[name] = override
	}
	// Drop bare parents that have subcommand entries (e.g. "ft.config" next
	// to "ft.config|get"): container commands resolve by "parent|child", and
	// a leftover bare entry would shadow the subcommand's metadata.
	for name := range merged {
		if i := strings.IndexByte(name, '|'); i > 0 {
			delete(merged, name[:i])
		}
	}
	return merged
}()

// cscContainerParents lists the parents of "parent|child" entries, so lookups
// know when to retry with the subcommand name.
var cscContainerParents = func() map[string]struct{} {
	parents := make(map[string]struct{})
	for name := range cscResolvedCommandTable {
		if i := strings.IndexByte(name, '|'); i > 0 {
			parents[name[:i]] = struct{}{}
		}
	}
	return parents
}()

// cscCommandMetaFor resolves cmd's metadata; ok is false for unknown
// commands, which are never cached.
func cscCommandMetaFor(cmd Cmder) (cscCommandMeta, bool) {
	args := cmd.Args()
	if len(args) == 0 {
		return cscCommandMeta{}, false
	}
	// The name decides which metadata applies, so it must be wire-faithful.
	if !cscWireFaithfulText(args[0]) {
		return cscCommandMeta{}, false
	}
	name := cmd.Name()
	if meta, ok := cscResolvedCommandTable[name]; ok {
		return meta, true
	}
	if _, ok := cscContainerParents[name]; ok {
		// Same for the subcommand token.
		if len(args) > 1 && cscWireFaithfulText(args[1]) {
			if sub := cmd.stringArg(1); sub != "" {
				meta, ok := cscResolvedCommandTable[name+"|"+internal.ToLower(sub)]
				return meta, ok
			}
		}
	}
	return cscCommandMeta{}, false
}

// cscWireFaithfulText reports whether v always reads the same via stringArg
// as it is sent by proto.Writer. Other types (e.g. a BinaryMarshaler whose
// String differs) could classify one command while the wire runs another.
func cscWireFaithfulText(v interface{}) bool {
	switch v.(type) {
	case string, []byte, *string:
		return true
	default:
		return false
	}
}

// cscIsClientSideCacheable is the HLD eligibility rule: readonly, provably
// keyed, and none of the negative signals.
func cscIsClientSideCacheable(meta cscCommandMeta) bool {
	return meta.bits&cscNegativeBits == 0 &&
		meta.bits&cscFlagReadonly != 0 &&
		cscHasKeyArgument(meta)
}

// cscHasKeyArgument: a complete key spec, or legacy firstKey/step metadata.
// lastKey is not consulted; it is -1 for variadic key lists.
func cscHasKeyArgument(meta cscCommandMeta) bool {
	return meta.bits&cscHasKeySpec != 0 || (meta.firstKey > 0 && meta.step > 0)
}

// isCacheable reports whether cmd's COMMAND metadata allows caching. This is
// per command; whether a specific call can be cached is decided by
// extractRedisKeys, and processCached consults both.
func isCacheable(cmd Cmder) bool {
	// Streaming commands (RawWriteToCmd) must not be buffered for the cache.
	if cmd.NoRetry() {
		return false
	}
	meta, ok := cscCommandMetaFor(cmd)
	if !ok || !cscIsClientSideCacheable(meta) {
		return false
	}
	// SORT_RO with BY/GET reads pattern keys we can't list, so its
	// invalidations would be missed. Plain SORT_RO is fine.
	if cmd.Name() == "sort_ro" && sortROHasByGet(cmd) {
		return false
	}
	return true
}

// sortROHasByGet reports whether a SORT_RO call uses BY or GET. Argument
// types whose wire form can differ from stringArg disqualify the call
// outright rather than risk missing the keyword.
func sortROHasByGet(cmd Cmder) bool {
	args := cmd.Args()
	for i := 2; i < len(args); i++ {
		switch args[i].(type) {
		case string, *string, []byte,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64, bool:
			if s := cmd.stringArg(i); strings.EqualFold(s, "by") || strings.EqualFold(s, "get") {
				return true
			}
		default:
			return true
		}
	}
	return false
}

// isClientTrackingCmd reports whether cmd is CLIENT TRACKING (any mode).
func isClientTrackingCmd(cmd Cmder) bool {
	return cmd.Name() == "client" && strings.EqualFold(cmd.stringArg(1), "tracking")
}

// isSelectCmd: SELECT would desync the connection's DB from the cache
// namespace, which is fixed at Options.DB.
func isSelectCmd(cmd Cmder) bool {
	return cmd.Name() == "select"
}

// isAuthCmd: AUTH would desync the connection's identity from the cache
// namespace, which is fixed at Options.Username.
func isAuthCmd(cmd Cmder) bool {
	return cmd.Name() == "auth"
}

// isProtocolChangingHelloCmd: HELLO with arguments can switch a tracked
// connection out of RESP3. A bare HELLO is safe.
func isProtocolChangingHelloCmd(cmd Cmder) bool {
	return cmd.Name() == "hello" && len(cmd.Args()) > 1
}

// isResetCmd: RESET disables tracking and switches to RESP2.
func isResetCmd(cmd Cmder) bool {
	return cmd.Name() == "reset"
}

// isSubscribeCmd: raw subscriptions would turn a pooled connection into a
// Pub/Sub connection the CSC drainer cannot own.
func isSubscribeCmd(cmd Cmder) bool {
	switch cmd.Name() {
	case "subscribe", "psubscribe", "ssubscribe":
		return true
	default:
		return false
	}
}

// buildCacheKey returns the RESP encoding of the full argument list as a
// collision-free cache key; ok is false when the args can't be marshaled.
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

// keyArg renders the argument at pos exactly as it goes on the wire, so
// invalidation lookups match the server's key names. Types whose rendering
// can differ (pointers, floats, marshalers, ...) return ok=false and the
// caller skips caching — a mismatched key would be served stale forever.
func keyArg(cmd Cmder, pos int) (string, bool) {
	args := cmd.Args()
	if pos < 0 || pos >= len(args) {
		return "", false
	}
	switch args[pos].(type) {
	case string, []byte,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return cmd.stringArg(pos), true
	}
	return "", false
}

// extractRedisKeys lists the key arguments the cache must watch for
// invalidations. It returns nil — and the command is served uncached — when
// the keys can't be listed for this call; a partial list is never returned.
func extractRedisKeys(cmd Cmder) []string {
	meta, ok := cscCommandMetaFor(cmd)
	if !ok {
		return nil
	}
	argsLen := len(cmd.Args())

	switch meta.extract {
	case cscKeyExtractNone:
		return nil

	case cscKeyExtractRange:
		first, step := int(meta.firstKey), int(meta.step)
		last := int(meta.lastKey)
		if last < 0 {
			last = argsLen + last
		}
		if first <= 0 || step <= 0 || last < first || last >= argsLen {
			return nil
		}
		return cscCollectKeys(cmd, first, step, (last-first)/step+1)

	case cscKeyExtractKeynum:
		nkPos := int(meta.numkeysAt)
		if nkPos <= 0 || nkPos >= argsLen {
			return nil
		}
		// numkeys decides which positions are keys, so read it wire-faithfully.
		raw, ok := keyArg(cmd, nkPos)
		if !ok {
			return nil
		}
		numKeys, err := strconv.Atoi(raw)
		if err != nil || numKeys <= 0 {
			return nil
		}
		first, step := int(meta.firstKey), int(meta.step)
		if first <= 0 || step <= 0 {
			return nil
		}
		// The count must fit the argument list; the division form avoids
		// overflow for any step.
		if numKeys > argsLen || (argsLen-1-first)/step < numKeys-1 {
			return nil
		}
		return cscCollectKeys(cmd, first, step, numKeys)
	}
	return nil
}

// cscCollectKeys renders n keys starting at first, step apart, returning nil
// (never a partial list) if any key can't be rendered wire-faithfully.
func cscCollectKeys(cmd Cmder, first, step, n int) []string {
	keys := make([]string, 0, n)
	for i, k := first, 0; k < n; i, k = i+step, k+1 {
		key, ok := keyArg(cmd, i)
		if !ok {
			return nil
		}
		keys = append(keys, key)
	}
	return keys
}
