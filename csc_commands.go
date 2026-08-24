package redis

import (
	"bytes"
	"math"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/proto"
)

// CSC eligibility: a command may be cached only when its COMMAND metadata
// shows it is read-only, keyed, deterministic, non-blocking, not a script
// runner, and not marked dont_cache; unknown commands are never cached.
// Records resolve into a commandMetadataView (see command_metadata.go), and
// every invocation reads exactly one view.

//go:generate go run ./internal/cmdmetagen -addr localhost:6379 -out command_info_snapshot.go

// cscCmdBits mirrors the COMMAND flags/tips the eligibility check uses.
type cscCmdBits uint8

const (
	cscFlagReadonly cscCmdBits = 1 << iota
	cscFlagScriptRunner
	cscFlagBlocking
	cscTipNondeterministicOutput
	cscTipDontCache
	// Set only for COMPLETE key specs; incomplete/not_key specs can't prove
	// which keys the command reads.
	cscHasKeySpec
)

// cscKeyExtract: how key arguments are located. None = keys can't be listed
// reliably, so the command is never cached even if eligible.
type cscKeyExtract uint8

const (
	cscKeyExtractNone cscKeyExtract = iota
	// firstKey..lastKey every step args; negative lastKey counts from the end.
	cscKeyExtractRange
	// The arg at numkeysAt holds the key count; keys start at firstKey.
	cscKeyExtractKeynum
)

type cscCommandMeta struct {
	bits    cscCmdBits
	extract cscKeyExtract

	firstKey  int16
	lastKey   int16
	step      int16
	numkeysAt int16
}

// cscMetadataCorrections excludes commands whose server metadata or
// invalidation delivery is known broken (each verified live). Corrections
// are extra tips ADDED to the resolved record — snapshot or live — so they
// survive a live upgrade and never discard the record's other metadata; being
// tips-only they can never make a command cacheable. Remove an entry only
// after a tracking probe shows the server invalidates correctly.
var cscMetadataCorrections = map[string][]string{
	// Read-only on paper, but its purpose is to bump key LRU/LFU state.
	"touch": {"dont_cache"},

	// Random output; the server doesn't tag it yet (its siblings are tagged).
	"vrandmember": {"nondeterministic_output"},

	// Flagged readonly but actually rewrites the filter; caching its OK reply
	// would suppress repeated compactions.
	"cf.compact": {"dont_cache"},

	// The module declares one key but the command reads N, and the server
	// tracks only that first key: writes to the other keys never send an
	// invalidation.
	"json.mget": {"dont_cache"},

	// The server registers no tracking for any of their series: writes never
	// send an invalidation.
	"ts.nrange":    {"dont_cache"},
	"ts.nrevrange": {"dont_cache"},

	// Replies include consumer-group state, but group changes (XGROUP
	// CREATE/SETID) never invalidate the stream key.
	"xinfo|stream": {"dont_cache"},
	"xinfo|groups": {"dont_cache"},

	// Usage changes on mutations that never signal the key (e.g. XGROUP CREATE).
	"memory|usage": {"dont_cache"},
}

// cscNegativeBits are the bits that each rule out caching.
const cscNegativeBits = cscFlagScriptRunner | cscFlagBlocking |
	cscTipNondeterministicOutput | cscTipDontCache

// cscAckExtraKeySpecs: commands whose extra unknown key specs are handled
// elsewhere (sort_ro's is the BY/GET patterns, rejected per invocation).
var cscAckExtraKeySpecs = map[string]bool{
	"sort_ro": true,
}

// cscSpecComplete: incomplete specs can't say which keys are read, and
// not_key marks a channel/pattern — neither proves key positions.
func cscSpecComplete(ks KeySpec) bool {
	if (ks.BeginSearch != "index" && ks.BeginSearch != "keyword") ||
		(ks.FindKeys != "range" && ks.FindKeys != "keynum") {
		return false
	}
	for _, f := range ks.Flags {
		if f == "incomplete" || f == "not_key" {
			return false
		}
	}
	return true
}

// cscDeriveMeta compresses a record into the hot-path form. Extraction is
// emitted only when key positions are unambiguous (at most one complete spec:
// a partial key list would mean dropped invalidations and stale entries).
func cscDeriveMeta(info *CommandInfo) cscCommandMeta {
	var meta cscCommandMeta
	// The readonly FLAG is the normative source; the ReadOnly convenience
	// bool is deliberately ignored.
	for _, f := range info.Flags {
		switch f {
		case "readonly":
			meta.bits |= cscFlagReadonly
		case "script_runner":
			meta.bits |= cscFlagScriptRunner
		case "blocking":
			meta.bits |= cscFlagBlocking
		}
	}
	for _, t := range info.Tips {
		switch t {
		case "nondeterministic_output":
			meta.bits |= cscTipNondeterministicOutput
		case "dont_cache":
			meta.bits |= cscTipDontCache
		}
	}

	var complete, other int
	var keynum KeySpec
	for _, ks := range info.KeySpecs {
		if cscSpecComplete(ks) {
			complete++
			if ks.FindKeys == "keynum" {
				keynum = ks
			}
		} else {
			other++
		}
	}
	if complete > 0 {
		meta.bits |= cscHasKeySpec
	}

	meta.firstKey = int16(info.FirstKeyPos)
	meta.lastKey = int16(info.LastKeyPos)
	meta.step = int16(info.StepCount)

	canExtract := (other == 0 && complete <= 1) || cscAckExtraKeySpecs[internal.ToLower(info.Name)]
	switch {
	case canExtract && info.FirstKeyPos > 0 && info.StepCount > 0:
		meta.extract = cscKeyExtractRange
	case canExtract && complete == 1 && keynum.FindKeys == "keynum" && keynum.BeginSearch == "index":
		// Validate every component before adding or narrowing. Checking only
		// the sums would let hostile positive/negative offsets cancel into a
		// plausible position and pass the per-call bounds checks.
		if keynum.Index < 1 || keynum.Index > math.MaxInt16 ||
			keynum.KeyNumIdx < 0 || keynum.KeyNumIdx > math.MaxInt16 ||
			keynum.FirstKey < 1 || keynum.FirstKey > math.MaxInt16 ||
			keynum.KeyStep < 1 || keynum.KeyStep > math.MaxInt16 {
			break // malformed positions: no extraction, never cached
		}
		numkeysAt := keynum.Index + keynum.KeyNumIdx
		firstKey := keynum.Index + keynum.FirstKey
		if numkeysAt < 1 || numkeysAt > math.MaxInt16 ||
			firstKey < 1 || firstKey > math.MaxInt16 {
			break // malformed positions: no extraction, never cached
		}
		meta.extract = cscKeyExtractKeynum
		meta.numkeysAt = int16(numkeysAt)
		meta.firstKey = int16(firstKey)
		meta.step = int16(keynum.KeyStep)
	}
	return meta
}

// cscLookupMeta resolves cmd's metadata in view; ok is false for unknown
// commands. Name and subcommand tokens must be wire-faithful (see
// cscWireFaithfulText).
func cscLookupMeta(view *commandMetadataView, cmd Cmder) (cscCommandMeta, bool) {
	args := cmd.Args()
	if len(args) == 0 || !cscWireFaithfulText(args[0]) {
		return cscCommandMeta{}, false
	}
	name := cmd.Name()
	if meta, ok := view.cscTable[name]; ok {
		return meta, true
	}
	if _, ok := view.cscParents[name]; ok {
		if len(args) > 1 && cscWireFaithfulText(args[1]) {
			if sub := cmd.stringArg(1); sub != "" {
				meta, ok := view.cscTable[name+"|"+internal.ToLower(sub)]
				return meta, ok
			}
		}
	}
	return cscCommandMeta{}, false
}

// cscWireFaithfulText: policy tokens must read the same via stringArg as they
// go on the wire, or one command could be classified while another runs.
func cscWireFaithfulText(v interface{}) bool {
	switch v.(type) {
	case string, []byte, *string:
		return true
	default:
		return false
	}
}

// cscIsClientSideCacheable is the HLD's six-rule eligibility check.
func cscIsClientSideCacheable(meta cscCommandMeta) bool {
	return meta.bits&cscNegativeBits == 0 &&
		meta.bits&cscFlagReadonly != 0 &&
		cscHasKeyArgument(meta)
}

// lastKey is never consulted: it is -1 for variadic key lists.
func cscHasKeyArgument(meta cscCommandMeta) bool {
	return meta.bits&cscHasKeySpec != 0 || (meta.firstKey > 0 && meta.step > 0)
}

// cscEligibleMeta is the command-level decision: it resolves cmd's metadata
// in view and applies the eligibility check. Whether a specific call can be
// cached is decided later by extractRedisKeys.
func cscEligibleMeta(view *commandMetadataView, cmd Cmder) (cscCommandMeta, bool) {
	// Streaming replies (RawWriteToCmd) must not be buffered for the cache.
	if cmd.NoRetry() {
		return cscCommandMeta{}, false
	}
	meta, ok := cscLookupMeta(view, cmd)
	if !ok || !cscIsClientSideCacheable(meta) {
		return cscCommandMeta{}, false
	}
	return meta, true
}

// sortROHasByGet reports whether a SORT_RO call uses BY or GET; arg types
// whose wire form can differ from stringArg disqualify outright.
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

// buildCacheKey: the RESP encoding of the args is the collision-free key.
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

// keyArg renders the arg at pos exactly as it goes on the wire; a mismatched
// key would never match its invalidation and be served stale forever, so
// types whose rendering can differ return ok=false (caller skips caching).
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

// cscExtractRedisKeys lists the keys the cache must watch for a call to a
// command already resolved to meta. nil = serve this call uncached; a
// PARTIAL list is never returned.
func cscExtractRedisKeys(meta cscCommandMeta, cmd Cmder) []string {
	// SORT_RO with BY/GET reads pattern keys this call cannot list.
	if cmd.Name() == "sort_ro" && sortROHasByGet(cmd) {
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
		// numkeys decides which positions are keys: read it wire-faithfully.
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
		// The count must fit the args; division avoids overflow for any step.
		if numKeys > argsLen || (argsLen-1-first)/step < numKeys-1 {
			return nil
		}
		return cscCollectKeys(cmd, first, step, numKeys)
	}
	return nil
}

// cscCollectKeys returns nil — never a partial list — if any key fails keyArg.
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
