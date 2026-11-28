package redis

import (
	"bytes"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"

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

// cscInvocationGuard identifies an invocation-level argument check that
// COMMAND cannot express. Zero means no guard; non-zero values are 1-based
// indexes into cscExtractionOverrides so cscCommandMeta stays compact and
// comparable.
type cscInvocationGuard uint8

const cscInvocationGuardNone cscInvocationGuard = 0

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

// cscExtraKeySpec describes a non-complete key spec that an extraction
// override accounts for via its invocation guard.
type cscExtraKeySpec struct {
	beginSearch string
	findKeys    string
	flags       []string
}

func (want cscExtraKeySpec) matches(spec KeySpec) bool {
	if spec.BeginSearch != want.beginSearch || spec.FindKeys != want.findKeys {
		return false
	}
	if len(spec.Flags) != len(want.flags) {
		return false
	}
	for i, flag := range spec.Flags {
		if slices.Contains(spec.Flags[:i], flag) || !slices.Contains(want.flags, flag) {
			return false
		}
	}
	return true
}

// cscInvocationGuardRule describes an invocation-level argument check that
// COMMAND cannot express. Starting at firstArg, any argument matching one of
// forbidTokens (case-insensitively) or having an uninspected wire type
// prevents caching.
type cscInvocationGuardRule struct {
	firstArg     int
	forbidTokens []string
}

func (r cscInvocationGuardRule) active() bool {
	return r.firstArg > 0 && len(r.forbidTokens) > 0
}

// allows reports whether cmd satisfies the guard rule, failing closed when an
// argument's wire text is unknown or the rule is unconfigured.
func (r cscInvocationGuardRule) allows(cmd Cmder) bool {
	if !r.active() {
		return false
	}
	args := cmd.Args()
	for i := r.firstArg; i < len(args); i++ {
		switch arg := args[i].(type) {
		case string:
			for _, tok := range r.forbidTokens {
				if strings.EqualFold(arg, tok) {
					return false
				}
			}
		case []byte:
			for _, tok := range r.forbidTokens {
				if bytes.EqualFold(arg, []byte(tok)) {
					return false
				}
			}
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64, bool:
		default:
			return false
		}
	}
	return true
}

func (g cscInvocationGuard) allows(cmd Cmder) bool {
	if g == cscInvocationGuardNone {
		return true
	}
	idx := int(g) - 1
	if idx < 0 || idx >= len(cscExtractionOverrides) {
		// Unknown guards cannot prove a complete key list.
		return false
	}
	return cscExtractionOverrides[idx].guard.allows(cmd)
}

// cscExtractionOverride handles invocation-level extraction rules that COMMAND
// cannot express.
type cscExtractionOverride struct {
	command       string
	extraKeySpecs []cscExtraKeySpec
	guard         cscInvocationGuardRule
}

// cscExtractionOverrides lists commands whose extra non-complete key specs are
// acknowledged by an invocation guard. Entry i maps to cscInvocationGuard(i+1)
// when its guard is active.
var cscExtractionOverrides = []cscExtractionOverride{
	// BY/GET keys are pattern-derived; the primary range spec is safe only without them.
	{
		command: "sort_ro",
		extraKeySpecs: []cscExtraKeySpec{{
			beginSearch: "unknown",
			findKeys:    "unknown",
			flags:       []string{"RO", "access"},
		}},
		guard: cscInvocationGuardRule{
			firstArg:     2,
			forbidTokens: []string{"by", "get"},
		},
	},
}

func cscLookupExtractionOverride(name string) (*cscExtractionOverride, cscInvocationGuard) {
	lower := internal.ToLower(name)
	for i := range cscExtractionOverrides {
		if cscExtractionOverrides[i].command == lower {
			override := &cscExtractionOverrides[i]
			if !override.guard.active() {
				return override, cscInvocationGuardNone
			}
			if i >= math.MaxUint8 {
				panic("redis: too many CSC extraction overrides")
			}
			return override, cscInvocationGuard(i + 1)
		}
	}
	return nil, cscInvocationGuardNone
}

func (o *cscExtractionOverride) permits(info *CommandInfo, complete, other int) bool {
	if o == nil || info == nil || len(o.extraKeySpecs) == 0 {
		return false
	}
	if complete != 1 || other != len(o.extraKeySpecs) || len(info.KeySpecs) != complete+other {
		return false
	}
	var stackBuf [8]bool
	var matched []bool
	if len(o.extraKeySpecs) <= len(stackBuf) {
		matched = stackBuf[:len(o.extraKeySpecs)]
	} else {
		matched = make([]bool, len(o.extraKeySpecs))
	}
	matchedCount := 0
	for _, spec := range info.KeySpecs {
		if cscSpecComplete(spec) {
			continue
		}
		found := false
		for i, want := range o.extraKeySpecs {
			if !matched[i] && want.matches(spec) {
				matched[i] = true
				matchedCount++
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return matchedCount == len(o.extraKeySpecs)
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
	override, guard := cscLookupExtractionOverride(info.Name)
	meta.guard = guard

	// The readonly FLAG is the normative source; the ReadOnly convenience
	// bool is deliberately ignored.
	var moduleCmd, movableKeys bool
	for _, f := range info.Flags {
		switch f {
		case "readonly":
			meta.bits |= cscFlagReadonly
		case "script_runner":
			meta.bits |= cscFlagScriptRunner
		case "blocking":
			meta.bits |= cscFlagBlocking
		case "module":
			moduleCmd = true
		case "movablekeys":
			movableKeys = true
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

	hasOverride := override.permits(info, complete, other)
	canExtract := (other == 0 && complete <= 1) || hasOverride
	if !canExtract {
		return meta
	}

	// Invalidation follows the positions getKeysFromCommand() reports: the
	// module getkeys API, then getkeys_proc, then the legacy range; never a key
	// spec. A module command that declares movable keys may register no getkeys
	// callback and is then tracked by its legacy positions alone, usually none.
	// Its spec positions cannot be checked against what the server tracks, so
	// without an override vouching for the shape it must not be cached.
	if moduleCmd && movableKeys && !hasOverride {
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
	if _, ok := view.subcommandParents[name]; ok {
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

func isSubscribeName(name string) bool {
	for i := range len(name) {
		if name[i] >= 0x80 {
			// Preserve ToLower's Unicode behavior, which differs from EqualFold.
			switch strings.ToLower(name) {
			case "subscribe", "psubscribe", "ssubscribe":
				return true
			default:
				return false
			}
		}
	}
	return strings.EqualFold(name, "subscribe") ||
		strings.EqualFold(name, "psubscribe") ||
		strings.EqualFold(name, "ssubscribe")
}

// cacheKeyScratch is the reusable buffer+writer pair buildCacheKeyNS encodes
// into. Building a cache key was the single largest allocator on the cached
// read path -- 32% of all bytes allocated -- because every call heap-allocated
// a bytes.Buffer, a proto.Writer (which itself allocates two 64-byte scratch
// slices), grew the buffer, and then copied it out with String(). Only the
// final String() is inherent: the key is retained, the scratch is not.
type cacheKeyScratch struct {
	buf bytes.Buffer
	wr  *proto.Writer
}

var cacheKeyScratchPool = sync.Pool{
	New: func() any {
		s := &cacheKeyScratch{}
		s.wr = proto.NewWriter(&s.buf)
		return s
	},
}

// buildCacheKeyNS renders the namespace parts and RESP arguments in one
// allocation (the returned string).
//
// Fusing the namespace in matters as much as pooling: the caller used to take
// this function's result and then do prefix+key, a second full copy of every
// cache key on every cached read. Writing the prefix into the buffer first
// makes the single String() produce the namespaced key directly, and the
// suffix after the namespace is still exactly the wire encoding, which the
// refresher relies on when it slices the namespace off to re-issue the command.
func buildCacheKeyNS(cmd Cmder, namespace ...string) (string, bool) {
	args := cmd.Args()
	if len(args) == 0 || !commandArgsRepeatable(cmd) {
		return "", false
	}
	s := cacheKeyScratchPool.Get().(*cacheKeyScratch)
	defer func() {
		// Drop an oversized buffer instead of pooling it, so one huge command
		// does not pin that capacity for the process lifetime.
		if s.buf.Cap() > cacheKeyScratchMaxCap {
			return
		}
		s.buf.Reset()
		cacheKeyScratchPool.Put(s)
	}()
	s.buf.Reset()
	for _, part := range namespace {
		s.buf.WriteString(part)
	}
	// The pooled writer targets s.buf for its whole life; Reset here only
	// guards against a zero-value scratch reaching this path.
	if s.wr == nil {
		s.wr = proto.NewWriter(&s.buf)
	}
	if err := s.wr.WriteArgs(args); err != nil {
		return "", false
	}
	return s.buf.String(), true
}

// cacheKeyScratchMaxCap bounds the buffer capacity kept in the pool.
const cacheKeyScratchMaxCap = 64 << 10

// isWireKeyType reports whether a key argument of v's type renders, through
// stringArg, exactly as proto.Writer sends it to the server, so invalidation
// lookups match the key names in the server's "invalidate" pushes. Only
// byte-identical types are accepted (fmt.Sprint of any integer matches the
// writer's base-10 strconv output); for anything else — pointers, bools,
// times, durations, floats, BinaryMarshaler values — the rendering can
// diverge, the invalidation would never match, and the entry would be served
// stale forever, so the caller skips caching (see processCached).
func isWireKeyType(v any) bool {
	switch v.(type) {
	case string, []byte,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return true
	}
	return false
}

// cscRenderEntryKey includes the metadata fingerprint in the entry namespace.
func cscRenderEntryKey(keyPrefix, fingerprint string, cmd Cmder) (string, bool) {
	return buildCacheKeyNS(cmd, keyPrefix, fingerprint, cscNamespaceSep)
}

func cscEntryKeyPrefixLen(keyPrefix, fingerprint string) int {
	return len(keyPrefix) + len(fingerprint) + len(cscNamespaceSep)
}

// keyArg renders the arg at pos exactly as it goes on the wire; a mismatched
// key would never match its invalidation and be served stale forever, so
// types whose rendering can differ return ok=false (caller skips caching).
func keyArg(cmd Cmder, pos int) (string, bool) {
	if !keyArgOK(cmd, pos) {
		return "", false
	}
	return cmd.stringArg(pos), true
}

// keyArgOK validates a key without allocating its wire representation.
func keyArgOK(cmd Cmder, pos int) bool {
	args := cmd.Args()
	return pos >= 0 && pos < len(args) && isWireKeyType(args[pos])
}

// cscIntArg reads a count using the same types and integer range as
// keyArg followed by strconv.Atoi, without rendering numeric arguments.
func cscIntArg(cmd Cmder, pos int) (int, bool) {
	args := cmd.Args()
	if pos < 0 || pos >= len(args) {
		return 0, false
	}
	var text string
	switch value := args[pos].(type) {
	case string:
		text = value
	case []byte:
		text = string(value)
	case int:
		return value, true
	case int8:
		return int(value), true
	case int16:
		return int(value), true
	case int32:
		return int(value), true
	case int64:
		return int(value), value >= math.MinInt && value <= math.MaxInt
	case uint:
		return int(value), value <= uint(math.MaxInt)
	case uint8:
		return int(value), true
	case uint16:
		return int(value), true
	case uint32:
		return int(value), uint64(value) <= uint64(math.MaxInt)
	case uint64:
		return int(value), value <= uint64(math.MaxInt)
	default:
		return 0, false
	}
	value, err := strconv.Atoi(text)
	return value, err == nil
}

// cscCanExtractRedisKeys applies the same invocation checks without allocating
// the key slice that AutoPipeline's solo-dispatch gate does not need.
func cscCanExtractRedisKeys(meta cscCommandMeta, cmd Cmder) bool {
	first, step, count := cscRedisKeyLayout(meta, cmd)
	for i := range count {
		if !keyArgOK(cmd, first+i*step) {
			return false
		}
	}
	return count > 0
}

// cscRedisKeyLayout returns first, step, and count; count zero means uncached.
func cscRedisKeyLayout(meta cscCommandMeta, cmd Cmder) (int, int, int) {
	if !meta.guard.allows(cmd) {
		return 0, 0, 0
	}
	argsLen := len(cmd.Args())

	switch meta.extract {
	case cscKeyExtractNone:
		return 0, 0, 0

	case cscKeyExtractRange:
		first, step := int(meta.firstKey), int(meta.step)
		last := int(meta.lastKey)
		if last < 0 {
			last = argsLen + last
		}
		if first <= 0 || step <= 0 || last < first || last >= argsLen {
			return 0, 0, 0
		}
		return first, step, (last-first)/step + 1

	case cscKeyExtractKeynum:
		nkPos := int(meta.numkeysAt)
		if nkPos <= 0 || nkPos >= argsLen {
			return 0, 0, 0
		}
		// numkeys decides which positions are keys: read it wire-faithfully.
		numKeys, ok := cscIntArg(cmd, nkPos)
		if !ok || numKeys <= 0 {
			return 0, 0, 0
		}
		first, step := int(meta.firstKey), int(meta.step)
		if first <= 0 || first >= argsLen || step <= 0 {
			return 0, 0, 0
		}
		// The count must fit the args; division avoids overflow for any step.
		if numKeys > argsLen || (argsLen-1-first)/step < numKeys-1 {
			return 0, 0, 0
		}
		return first, step, numKeys
	}
	return 0, 0, 0
}
