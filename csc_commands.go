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

// cscInvocationGuard identifies argument checks that COMMAND cannot express.
type cscInvocationGuard uint8

const (
	cscInvocationGuardNone cscInvocationGuard = iota
	cscInvocationGuardSortRONoByGet
)

type cscCommandMeta struct {
	bits    cscCmdBits
	extract cscKeyExtract
	guard   cscInvocationGuard

	firstKey  int16
	lastKey   int16
	step      int16
	numkeysAt int16
}

// commandMetadataCorrection patches a verified server metadata gap before CSC
// and Cluster routing derive their decisions.
type commandMetadataCorrection struct {
	tips        []string
	removeFlags []string

	keySpecs    []KeySpec
	firstKeyPos int8
	lastKeyPos  int8
	stepCount   int8
}

// commandMetadataCorrections contains verified server metadata fixes shared by
// CSC and routing. Remove entries only after probing the server fix.
var commandMetadataCorrections = map[string]commandMetadataCorrection{
	// Read-only on paper, but its purpose is to bump key LRU/LFU state.
	"touch": {tips: []string{"dont_cache"}},

	// Random output; the server doesn't tag it yet (its siblings are tagged).
	"vrandmember": {tips: []string{"nondeterministic_output"}},

	// Marked readonly but rewrites the filter; disable caching and replica routing.
	"cf.compact": {tips: []string{"dont_cache"}, removeFlags: []string{"readonly"}},

	// The server reports and tracks only the first of N keys. Keep caching
	// disabled and correct the routing range (N keys followed by a path).
	"json.mget": {
		tips: []string{"dont_cache"},
		keySpecs: []KeySpec{{
			Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1,
			FindKeys: "range", LastKey: -2, KeyStep: 1,
		}},
		firstKeyPos: 1, lastKeyPos: -2, stepCount: 1,
	},

	// The server registers no tracking for any of their series: writes never
	// send an invalidation.
	"ts.nrange":    {tips: []string{"dont_cache"}},
	"ts.nrevrange": {tips: []string{"dont_cache"}},

	// Replies include consumer-group state, but group changes (XGROUP
	// CREATE/SETID) never invalidate the stream key.
	"xinfo|stream": {tips: []string{"dont_cache"}},
	"xinfo|groups": {tips: []string{"dont_cache"}},

	// Usage changes on mutations that never signal the key (e.g. XGROUP CREATE).
	"memory|usage": {tips: []string{"dont_cache"}},
}

// cscNegativeBits are the bits that each rule out caching.
const cscNegativeBits = cscFlagScriptRunner | cscFlagBlocking |
	cscTipNondeterministicOutput | cscTipDontCache

// cscExtractionOverride handles invocation-level extraction rules that COMMAND
// cannot express.
type cscExtractionOverride struct {
	extraKeySpecs cscExtraKeySpecs
	guard         cscInvocationGuard
}

type cscExtraKeySpecs uint8

const (
	cscExtraKeySpecsNone cscExtraKeySpecs = iota
	cscExtraKeySpecsSortROPattern
)

var cscExtractionOverrides = map[string]cscExtractionOverride{
	// BY/GET keys are pattern-derived; legacy positions are safe only without them.
	"sort_ro": {
		extraKeySpecs: cscExtraKeySpecsSortROPattern,
		guard:         cscInvocationGuardSortRONoByGet,
	},
}

func (kind cscExtraKeySpecs) permits(info *CommandInfo, complete, other int) bool {
	switch kind {
	case cscExtraKeySpecsSortROPattern:
		if len(info.KeySpecs) != 2 || complete != 1 || other != 1 {
			return false
		}
		for _, spec := range info.KeySpecs {
			if cscSpecComplete(spec) {
				continue
			}
			if spec.BeginSearch != "unknown" || spec.FindKeys != "unknown" {
				return false
			}
			if len(spec.Flags) != 2 {
				return false
			}
			var readOnly, access bool
			for _, flag := range spec.Flags {
				switch flag {
				case "RO":
					readOnly = true
				case "access":
					access = true
				default:
					return false
				}
			}
			if !readOnly || !access {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// cscSpecComplete reports whether a spec identifies exact Redis keys.
func cscSpecComplete(ks KeySpec) bool {
	if (ks.BeginSearch != "index" && ks.BeginSearch != "keyword") ||
		(ks.FindKeys != "range" && ks.FindKeys != "keynum") {
		return false
	}
	return classifyCommandMetadataKeySpecFlags(ks.Flags).cscComplete
}

// cscDeriveMeta compresses a record into the hot-path form. Extraction is
// emitted only when key positions are unambiguous (at most one complete spec:
// a partial key list would mean dropped invalidations and stale entries).
func cscDeriveMeta(info *CommandInfo) cscCommandMeta {
	var meta cscCommandMeta
	override := cscExtractionOverrides[internal.ToLower(info.Name)]
	meta.guard = override.guard

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
	var completeSpec KeySpec
	for _, ks := range info.KeySpecs {
		if cscSpecComplete(ks) {
			complete++
			completeSpec = ks
		} else {
			other++
		}
	}
	if complete > 0 {
		meta.bits |= cscHasKeySpec
	}

	// Modern key specs are authoritative; never fall back when any are present.
	if len(info.KeySpecs) == 0 {
		meta.firstKey = int16(info.FirstKeyPos)
		meta.lastKey = int16(info.LastKeyPos)
		meta.step = int16(info.StepCount)
	}

	canExtract := (other == 0 && complete <= 1) || override.extraKeySpecs.permits(info, complete, other)
	if !canExtract {
		return meta
	}

	if complete == 1 && completeSpec.BeginSearch == "index" {
		switch completeSpec.FindKeys {
		case "range":
			if completeSpec.Index < 1 || completeSpec.Index > math.MaxInt16 ||
				completeSpec.KeyStep < 1 || completeSpec.KeyStep > math.MaxInt16 ||
				completeSpec.Limit != 0 || completeSpec.LastKey < math.MinInt16 {
				return meta
			}
			lastKey := completeSpec.LastKey
			if lastKey >= 0 {
				absoluteLast := int64(completeSpec.Index) + int64(lastKey)
				if absoluteLast > math.MaxInt16 {
					return meta
				}
				lastKey = int(absoluteLast)
			}
			meta.extract = cscKeyExtractRange
			meta.firstKey = int16(completeSpec.Index)
			meta.lastKey = int16(lastKey)
			meta.step = int16(completeSpec.KeyStep)
		case "keynum":
			// Validate operands before addition to prevent offset cancellation.
			if completeSpec.Index < 1 || completeSpec.Index > math.MaxInt16 ||
				completeSpec.KeyNumIdx < 0 || completeSpec.KeyNumIdx > math.MaxInt16 ||
				completeSpec.FirstKey < 1 || completeSpec.FirstKey > math.MaxInt16 ||
				completeSpec.KeyStep < 1 || completeSpec.KeyStep > math.MaxInt16 {
				return meta // malformed positions: no extraction, never cached
			}
			numkeysAt := int64(completeSpec.Index) + int64(completeSpec.KeyNumIdx)
			firstKey := int64(completeSpec.Index) + int64(completeSpec.FirstKey)
			if numkeysAt < 1 || numkeysAt > math.MaxInt16 ||
				firstKey < 1 || firstKey > math.MaxInt16 {
				return meta // malformed positions: no extraction, never cached
			}
			meta.extract = cscKeyExtractKeynum
			meta.numkeysAt = int16(numkeysAt)
			meta.firstKey = int16(firstKey)
			meta.step = int16(completeSpec.KeyStep)
		}
		return meta
	}

	if len(info.KeySpecs) == 0 && info.FirstKeyPos > 0 && info.StepCount > 0 {
		meta.extract = cscKeyExtractRange
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
	if _, ok := view.cscParents[name]; ok {
		if len(args) > 1 {
			if !cscWireFaithfulText(args[1]) {
				return cscCommandMeta{}, false
			}
			sub := cmd.stringArg(1)
			if sub == "" {
				return cscCommandMeta{}, false
			}
			meta, ok := view.cscTable[name+"|"+internal.ToLower(sub)]
			return meta, ok
		}
	}
	meta, ok := view.cscTable[name]
	return meta, ok
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

// sortROHasByGet fails closed when an argument's wire text is unknown.
func sortROHasByGet(cmd Cmder) bool {
	args := cmd.Args()
	for i := 2; i < len(args); i++ {
		switch arg := args[i].(type) {
		case string:
			if strings.EqualFold(arg, "by") || strings.EqualFold(arg, "get") {
				return true
			}
		case []byte:
			if bytes.EqualFold(arg, []byte("by")) || bytes.EqualFold(arg, []byte("get")) {
				return true
			}
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64, bool:
		default:
			return true
		}
	}
	return false
}

// cscCommandToken returns tokens with known wire representations without
// invoking BinaryMarshaler, which may be stateful.
func cscCommandToken(cmd Cmder, pos int) (string, bool) {
	args := cmd.Args()
	if pos < 0 || pos >= len(args) {
		return "", false
	}
	switch value := args[pos].(type) {
	case string:
		return value, true
	case *string:
		if value == nil {
			return "", true
		}
		return *value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

// isClientTrackingCmd reports whether cmd is provably CLIENT TRACKING.
func isClientTrackingCmd(cmd Cmder) bool {
	name, nameOK := cscCommandToken(cmd, 0)
	subcommand, subcommandOK := cscCommandToken(cmd, 1)
	return nameOK && subcommandOK && strings.EqualFold(name, "client") &&
		strings.EqualFold(subcommand, "tracking")
}

// isSelectCmd: SELECT would desync the connection's DB from the cache
// namespace, which is fixed at Options.DB.
func isSelectCmd(cmd Cmder) bool {
	name, ok := cscCommandToken(cmd, 0)
	return ok && strings.EqualFold(name, "select")
}

// isAuthCmd: AUTH would desync the connection's identity from the cache
// namespace, which is fixed at Options.Username.
func isAuthCmd(cmd Cmder) bool {
	name, ok := cscCommandToken(cmd, 0)
	return ok && strings.EqualFold(name, "auth")
}

// isProtocolChangingHelloCmd: HELLO with arguments can switch a tracked
// connection out of RESP3. A bare HELLO is safe.
func isProtocolChangingHelloCmd(cmd Cmder) bool {
	name, ok := cscCommandToken(cmd, 0)
	return ok && strings.EqualFold(name, "hello") && len(cmd.Args()) > 1
}

// isResetCmd: RESET disables tracking and switches to RESP2.
func isResetCmd(cmd Cmder) bool {
	name, ok := cscCommandToken(cmd, 0)
	return ok && strings.EqualFold(name, "reset")
}

// isSubscribeCmd: raw subscriptions would turn a pooled connection into a
// Pub/Sub connection the CSC drainer cannot own.
func isSubscribeCmd(cmd Cmder) bool {
	name, ok := cscCommandToken(cmd, 0)
	if !ok {
		return false
	}
	switch strings.ToLower(name) {
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
	// Stateful MarshalBinary calls could make the cache key differ from the command.
	if !commandArgsRepeatable(cmd) {
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
	switch meta.guard {
	case cscInvocationGuardNone:
	case cscInvocationGuardSortRONoByGet:
		// BY/GET keys cannot be enumerated.
		if sortROHasByGet(cmd) {
			return nil
		}
	default:
		// Unknown guards cannot prove a complete key list.
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
