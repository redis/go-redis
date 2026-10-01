package redis

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// helper to create a Cmd with the given args.
func makeCmd(args ...interface{}) Cmder {
	return NewCmd(context.Background(), args...)
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
	return ok && isSubscribeName(name)
}

// cscExtractRedisKeys lists the keys the cache must watch for a call to a
// command already resolved to meta. nil = serve this call uncached; a
// PARTIAL list is never returned.
func cscExtractRedisKeys(meta cscCommandMeta, cmd Cmder) []string {
	first, step, count := cscRedisKeyLayout(meta, cmd)
	if count == 0 {
		return nil
	}
	return cscCollectKeys(cmd, first, step, count)
}

// Test conveniences over the default view.
func isCacheable(cmd Cmder) bool {
	return isCacheableInView(defaultCommandMetadataView(), cmd)
}

func isCacheableInView(view *commandMetadataView, cmd Cmder) bool {
	_, ok := cscEligibleMeta(view, cmd)
	return ok
}

func extractRedisKeys(cmd Cmder) []string {
	return extractRedisKeysInView(defaultCommandMetadataView(), cmd)
}

func extractRedisKeysInView(view *commandMetadataView, cmd Cmder) []string {
	meta, ok := cscLookupMeta(view, cmd)
	if !ok {
		return nil
	}
	return cscExtractRedisKeys(meta, cmd)
}

func cscCommandMetaFor(cmd Cmder) (cscCommandMeta, bool) {
	return cscLookupMeta(defaultCommandMetadataView(), cmd)
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
		"XLEN", "XRANGE", "XREVRANGE",
		// JSON.MGET is absent: the server tracks only its first key.
		"JSON.GET", "JSON.ARRINDEX", "JSON.ARRLEN",
		"JSON.OBJKEYS", "JSON.OBJLEN", "JSON.RESP",
		"JSON.STRLEN", "JSON.TYPE",
		// TS.INFO is absent: the module marks it dont_cache.
		"TS.GET", "TS.RANGE", "TS.REVRANGE",
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

func TestIsCacheable_XReadRejected(t *testing.T) {
	// XREAD supports BLOCK and state-relative $/+ IDs, so it must not be cached.
	cmd := makeCmd("XREAD", "COUNT", "5", "STREAMS", "s", "0")
	if isCacheable(cmd) {
		t.Error("expected XREAD to NOT be cacheable")
	}
}

// Keys must be extracted exactly as they go on the wire; other types make
// extraction fail so the command is served uncached.
func TestExtractRedisKeys_WireFaithfulTypesOnly(t *testing.T) {
	key := "real-key"
	cases := []struct {
		name string
		cmd  Cmder
		want []string
	}{
		{"string key", makeCmd("get", "k"), []string{"k"}},
		{"[]byte key", makeCmd("get", []byte("k")), []string{"k"}},
		{"int key", makeCmd("get", 123), []string{"123"}},
		{"uint64 key", makeCmd("get", uint64(7)), []string{"7"}},
		{"pointer key", makeCmd("get", &key), nil},
		{"bool key", makeCmd("get", true), nil},
		{"float key", makeCmd("get", 1.5), nil},
		// Multi-key commands: one divergent key poisons the whole extraction.
		{"mget with pointer key", makeCmd("mget", "a", &key, "b"), nil},
		{"mget with string keys", makeCmd("mget", "a", "b"), []string{"a", "b"}},
	}
	for _, tc := range cases {
		got := extractRedisKeys(tc.cmd)
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
				break
			}
		}
	}
}

func TestIsCacheable_XPendingRejected(t *testing.T) {
	// XPENDING's extended form returns wall-clock-relative idle times and its
	// IDLE filter is time-dependent, so it must not be cached.
	for _, cmd := range []Cmder{
		makeCmd("XPENDING", "s", "grp"),
		makeCmd("XPENDING", "s", "grp", "IDLE", "9000", "-", "+", "10"),
	} {
		if isCacheable(cmd) {
			t.Errorf("expected %v to NOT be cacheable", cmd.Args())
		}
	}
}

func TestIsCacheable_KeylessCommandRejected(t *testing.T) {
	// Keyless commands are not cached even when read-only.
	for _, cmd := range []Cmder{
		makeCmd("ping"),
		makeCmd("KEYS", "pattern*"),
		makeCmd("scan", "0"),
	} {
		if isCacheable(cmd) {
			t.Errorf("expected keyless command %v to NOT be cacheable", cmd.Args())
		}
	}
}

// Isolate each HLD rule so another exclusion cannot hide a broken rule.
func TestCSCMetadataEligibilityRules(t *testing.T) {
	for _, tc := range []struct {
		name        string
		flags, tips []string
		keyed, want bool
	}{
		{"eligible", []string{"readonly"}, nil, true, true},
		{"ACL read is insufficient", nil, nil, true, false},
		{"keyless", []string{"readonly"}, nil, false, false},
		{"blocking", []string{"readonly", "blocking"}, nil, true, false},
		{"script runner", []string{"readonly", "script_runner"}, nil, true, false},
		{"nondeterministic", []string{"readonly"}, []string{"nondeterministic_output"}, true, false},
		{"negative override", []string{"readonly"}, []string{"dont_cache"}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := testReadOnlyCommandInfo("module.read")
			info.Flags, info.Tips = tc.flags, tc.tips
			info.ACLFlags, info.ReadOnly = []string{"@read"}, true
			if !tc.keyed {
				info.KeySpecs = nil
				info.FirstKeyPos, info.LastKeyPos, info.StepCount = 0, 0, 0
			}
			// Live and static override records must apply the same rules.
			records := map[string]*CommandInfo{info.Name: info}
			for _, view := range []*commandMetadataView{
				buildCommandMetadataView(records, nil), buildCommandMetadataView(nil, records),
			} {
				if got := isCacheableInView(view, makeCmd(info.Name, "key")); got != tc.want {
					t.Fatalf("eligible=%v, want %v", got, tc.want)
				}
			}
		})
	}
}

// Pins the HLD's named cases and the probe-verified overrides.
func TestIsCacheable_HLDNormativeCases(t *testing.T) {
	notCacheable := []struct {
		reason string
		cmd    Cmder
	}{
		{"FR.14 KEYS is keyless", makeCmd("keys", "*")},
		{"FR.15 XPENDING has nondeterministic_output", makeCmd("xpending", "s", "grp")},
		{"FR.16 EVAL_RO has script_runner", makeCmd("eval_ro", "return 1", 1, "k")},
		{"FR.16 EVALSHA_RO has script_runner", makeCmd("evalsha_ro", "sha", 1, "k")},
		{"FR.16 FCALL_RO has script_runner", makeCmd("fcall_ro", "fn", 1, "k")},
		{"FR.17 TOUCH is overridden dont_cache", makeCmd("touch", "k1", "k2")},
		{"contradictory metadata override: CF.COMPACT really writes", makeCmd("cf.compact", "filter")},
		{"server tracks only the first key: JSON.MGET", makeCmd("json.mget", "j1", "j2", "$")},
		{"server tracks no keys at all: TS.NRANGE", makeCmd("ts.nrange", 2, "s1", "s2", "-", "+")},
		{"server tracks no keys at all: TS.NREVRANGE", makeCmd("ts.nrevrange", 2, "s1", "s2", "-", "+")},
		{"group mutations don't invalidate: XINFO STREAM", makeCmd("xinfo", "stream", "s")},
		{"group mutations don't invalidate: XINFO GROUPS", makeCmd("xinfo", "groups", "s")},
		{"usage changes without invalidation: MEMORY USAGE", makeCmd("memory", "usage", "s")},
		{"FR.18 XREADGROUP is not readonly", makeCmd("xreadgroup", "GROUP", "g", "c", "STREAMS", "s", ">")},
		{"FR.9 unknown commands fail closed", makeCmd("some.unknown", "k")},
		{"dont_cache tip (module): TS.INFO", makeCmd("ts.info", "series")},
		{"dont_cache tip (module): FT.SEARCH", makeCmd("ft.search", "idx", "q")},
		{"dont_cache tip (module): BF.INFO", makeCmd("bf.info", "filter")},
		{"nondeterministic_output: TTL", makeCmd("ttl", "k")},
		{"nondeterministic_output: DUMP", makeCmd("dump", "k")},
		{"nondeterministic_output: HRANDFIELD", makeCmd("hrandfield", "h")},
		{"nondeterministic_output: SRANDMEMBER", makeCmd("srandmember", "s")},
		{"nondeterministic_output override: VRANDMEMBER", makeCmd("vrandmember", "vs")},
		{"FR.20 XREAD has the blocking flag (excluded even without BLOCK)", makeCmd("xread", "STREAMS", "s", "0")},
	}
	for _, tc := range notCacheable {
		if isCacheable(tc.cmd) {
			t.Errorf("%s: expected %v to NOT be cacheable", tc.reason, tc.cmd.Args())
		}
	}
}

// Commands the old hand-list missed but whose metadata proves cacheable.
func TestIsCacheable_MetadataEligible(t *testing.T) {
	cacheable := []Cmder{
		makeCmd("pfcount", "hll1", "hll2"),
		makeCmd("zintercard", 2, "z1", "z2"),
		makeCmd("expiretime", "k"),
		makeCmd("pexpiretime", "k"),
		makeCmd("bf.exists", "filter", "item"),
		makeCmd("cf.exists", "filter", "item"),
		makeCmd("topk.query", "sketch", "item"),
		makeCmd("cms.query", "sketch", "item"),
		makeCmd("tdigest.quantile", "sketch", "0.5"),
		makeCmd("ft.sugget", "sugkey", "pref"),
		makeCmd("vcard", "vs"),
		makeCmd("vsim", "vs", "ELE", "e"),
	}
	for _, cmd := range cacheable {
		if !isCacheable(cmd) {
			t.Errorf("expected %v to be cacheable per COMMAND metadata", cmd.Args())
		}
	}
}

// cscWireSmuggler renders one value on the wire and another via stringArg.
type cscWireSmuggler struct{ wire, str string }

func (s cscWireSmuggler) MarshalBinary() ([]byte, error) { return []byte(s.wire), nil }
func (s cscWireSmuggler) String() string                 { return s.str }

func TestIsCacheable_PolicyTokensMustBeWireFaithful(t *testing.T) {
	// Tokens that drive the decision (name, subcommand, numkeys) must be
	// wire-faithful; anything else fails closed.
	if cmd := makeCmd("xinfo", cscWireSmuggler{wire: "consumers", str: "stream"}, "s", "g"); isCacheable(cmd) {
		t.Error("non-wire-faithful subcommand token must not be cacheable")
	}

	if cmd := makeCmd(cscWireSmuggler{wire: "set", str: "get"}, "k", "v"); isCacheable(cmd) {
		t.Error("non-wire-faithful command name must not be cacheable")
	}

	cmd := makeCmd("zintercard", cscWireSmuggler{wire: "2", str: "1"}, "z1", "z2")
	if keys := extractRedisKeys(cmd); keys != nil {
		t.Errorf("non-wire-faithful numkeys must fail extraction, got %v", keys)
	}
	// []byte and *string are wire-faithful and keep working.
	if _, ok := cscCommandMetaFor(makeCmd("memory", []byte("usage"), "k")); !ok {
		t.Error("[]byte subcommand token should resolve (MEMORY USAGE)")
	}
	if cmd := makeCmd([]byte("get"), "k"); !isCacheable(cmd) {
		t.Error("[]byte command name should resolve (GET)")
	}
	get, usage := "get", "usage"
	if cmd := makeCmd(&get, "k"); !isCacheable(cmd) {
		t.Error("*string command name should resolve (GET)")
	}
	if _, ok := cscCommandMetaFor(makeCmd("memory", &usage, "k")); !ok {
		t.Error("*string subcommand token should resolve (MEMORY USAGE)")
	}
	var nilName *string
	if cmd := makeCmd(nilName, "k"); isCacheable(cmd) {
		t.Error("nil *string command name must fail closed")
	}
}

func TestCSCMetadataCorrections(t *testing.T) {
	// Corrections must survive in the shared default view.
	for name, correction := range commandMetadataCorrections {
		base, ok := commandInfoSnapshotByName()[name]
		if !ok {
			t.Errorf("correction %q has no snapshot record; re-audit or remove it", name)
			continue
		}
		meta, ok := defaultCommandMetadataView().cscTable[name]
		if !ok {
			t.Errorf("correction %q did not survive into the default table (bare container-parent key?)", name)
			continue
		}
		if cscIsClientSideCacheable(meta) {
			t.Errorf("correction %q must never resolve cacheable, got %+v", name, meta)
		}
		rec := defaultCommandMetadataView().records[name]
		for _, tip := range correction.tips {
			if !commandRecordHas(rec, tip, true) {
				t.Errorf("correction %q lost tip %q in the resolved record", name, tip)
			}
		}
		if len(correction.removeFlags) == 0 && correction.keySpecs == nil &&
			(len(rec.Flags) != len(base.Flags) || len(rec.KeySpecs) != len(base.KeySpecs)) {
			t.Errorf("correction %q must keep the base record's flags and key specs", name)
		}
	}
	if commandRecordHas(defaultCommandMetadataView().records["cf.compact"], "readonly", false) {
		t.Fatal("CF.COMPACT correction left the mutating command readonly")
	}
	jsonMGet := defaultCommandMetadataView().records["json.mget"]
	if len(jsonMGet.KeySpecs) != 1 || jsonMGet.KeySpecs[0].LastKey != -2 {
		t.Fatalf("JSON.MGET correction did not expose all keys: %+v", jsonMGet.KeySpecs)
	}
}

func TestCSCDeriveMeta(t *testing.T) {
	completeRange := KeySpec{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}
	unknownSpec := KeySpec{Flags: []string{"RO", "access"}, BeginSearch: "unknown", FindKeys: "unknown"}
	readonly := func(specs ...KeySpec) *CommandInfo {
		return &CommandInfo{Name: "cmd", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1, KeySpecs: specs}
	}

	if m := cscDeriveMeta(readonly(completeRange)); m.extract != cscKeyExtractRange || !cscIsClientSideCacheable(m) {
		t.Errorf("single complete range spec should derive cacheable range extraction, got %+v", m)
	}
	// Modern key specs must override a narrower legacy triple.
	disagreeingRange := completeRange
	disagreeingRange.LastKey = 1
	disagreeing := readonly(disagreeingRange)
	disagreeingMeta := cscDeriveMeta(disagreeing)
	if keys := cscExtractRedisKeys(disagreeingMeta, makeCmd("cmd", "k1", "k2")); len(keys) != 2 || keys[0] != "k1" || keys[1] != "k2" {
		t.Errorf("modern key spec must win over a narrower legacy triple, got %v (%+v)", keys, disagreeingMeta)
	}
	// Two complete specs: the legacy triple covers only the first, so no
	// extraction — a partial key list would drop invalidations.
	if m := cscDeriveMeta(readonly(completeRange, completeRange)); m.extract != cscKeyExtractNone {
		t.Errorf("two complete specs must derive no extraction, got %+v", m)
	}
	if m := cscDeriveMeta(readonly(completeRange, unknownSpec)); m.extract != cscKeyExtractNone {
		t.Errorf("unknown extra spec must derive no extraction, got %+v", m)
	}
	// incomplete/not_key specs never prove keyedness.
	incomplete := readonly(KeySpec{Flags: []string{"RO", "access", "incomplete"}, BeginSearch: "keyword", Keyword: "STREAMS", FindKeys: "range", KeyStep: 1})
	incomplete.FirstKeyPos, incomplete.StepCount = 0, 0
	if m := cscDeriveMeta(incomplete); cscHasKeyArgument(m) {
		t.Errorf("incomplete spec must not prove keyedness, got %+v", m)
	}
	notKey := readonly(KeySpec{Flags: []string{"not_key"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1})
	notKey.FirstKeyPos, notKey.StepCount = 0, 0
	if m := cscDeriveMeta(notKey); cscHasKeyArgument(m) {
		t.Errorf("not_key spec must not prove keyedness, got %+v", m)
	}
	for name, flags := range map[string][]string{
		"prefix":           {"RO", "access", "prefix"},
		"future flag":      {"RO", "access", "future_key_flag"},
		"missing mode":     {"access"},
		"conflicting mode": {"RO", "RW", "access"},
	} {
		t.Run(name+" key flags fail closed", func(t *testing.T) {
			record := readonly(KeySpec{
				Flags: flags, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			})
			meta := cscDeriveMeta(record)
			if cscIsClientSideCacheable(meta) || meta.extract != cscKeyExtractNone {
				t.Fatalf("unsafe key flags produced CSC metadata: %+v", meta)
			}
		})
	}
	// Keynum: positions are absolute (begin_search index + relative offsets).
	keynum := &CommandInfo{
		Name: "zdiffish", Flags: []string{"readonly"},
		KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1, FindKeys: "keynum", KeyNumIdx: 0, FirstKey: 1, KeyStep: 1}},
	}
	if m := cscDeriveMeta(keynum); m.extract != cscKeyExtractKeynum || m.numkeysAt != 1 || m.firstKey != 2 || m.step != 1 {
		t.Errorf("keynum derivation wrong: %+v", m)
	}
	// sort_ro accepts its primary range with an invocation guard.
	sortRO := readonly(completeRange, unknownSpec)
	sortRO.Name = "sort_ro"
	if m := cscDeriveMeta(sortRO); m.extract != cscKeyExtractRange || m.guard != cscInvocationGuardSortRONoByGet {
		t.Errorf("sort_ro adaptation should keep guarded range extraction, got %+v", m)
	}
	for name, specs := range map[string][]KeySpec{
		"extra third spec":      {completeRange, unknownSpec, unknownSpec},
		"different algorithm":   {completeRange, {Flags: []string{"RO", "access"}, BeginSearch: "future", FindKeys: "unknown"}},
		"write-capable unknown": {completeRange, {Flags: []string{"RW", "access"}, BeginSearch: "unknown", FindKeys: "unknown"}},
	} {
		t.Run("sort_ro rejects "+name, func(t *testing.T) {
			record := readonly(specs...)
			record.Name = "sort_ro"
			if got := cscDeriveMeta(record); got.extract != cscKeyExtractNone {
				t.Fatalf("unexpected extraction for unacknowledged shape: %+v", got)
			}
		})
	}
}

func TestCommandMetadataViewWithApplicationOverrides(t *testing.T) {
	// An application override record replaces every lower layer: it can
	// exclude a cacheable command...
	view := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"get": {Name: "get", Tips: []string{"dont_cache"}},
	})
	if isCacheableInView(view, makeCmd("get", "k")) {
		t.Error("application dont_cache override on GET must exclude it")
	}
	// ...or (at the application's own risk) admit a command no source knows.
	view = buildCommandMetadataView(nil, map[string]*CommandInfo{
		"myext.get": {
			Name: "myext.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
	})
	if !isCacheableInView(view, makeCmd("myext.get", "k")) {
		t.Error("application record for an unknown command should make it cacheable")
	}
	if keys := extractRedisKeysInView(view, makeCmd("myext.get", "k")); len(keys) != 1 || keys[0] != "k" {
		t.Errorf("application record extraction: got %v, want [k]", keys)
	}
	// The default view is untouched by per-client overrides.
	if isCacheable(makeCmd("myext.get", "k")) {
		t.Error("default view must not see application overrides")
	}
	// A nil override record means "unknown" and fails closed.
	view = buildCommandMetadataView(nil, map[string]*CommandInfo{"get": nil})
	if isCacheableInView(view, makeCmd("get", "k")) {
		t.Error("a nil override record must remove the command (fail closed)")
	}
}

func TestCommandMetadataViewLivePrecedence(t *testing.T) {
	// A live record replaces the snapshot's...
	live := map[string]*CommandInfo{"get": {Name: "get", Flags: []string{"readonly"}}}
	view := buildCommandMetadataView(live, nil)
	if isCacheableInView(view, makeCmd("get", "k")) {
		t.Error("live record without key metadata must exclude GET")
	}
	// ...an application override beats live...
	view = buildCommandMetadataView(live, map[string]*CommandInfo{
		"get": testReadOnlyCommandInfo("get"),
	})
	if !isCacheableInView(view, makeCmd("get", "k")) {
		t.Error("application override must beat the live record")
	}
	// ...and built-in corrections apply on top of live records too.
	touch := commandInfoSnapshotByName()["touch"]
	view = buildCommandMetadataView(map[string]*CommandInfo{"touch": touch}, nil)
	if isCacheableInView(view, makeCmd("touch", "k")) {
		t.Error("built-in correction must survive a live record for the same command")
	}
	// Snapshot records fill commands the live output does not mention.
	view = buildCommandMetadataView(live, nil)
	if !isCacheableInView(view, makeCmd("mget", "k")) {
		t.Error("snapshot must fill commands absent from live output")
	}
}

func TestCSCMetadataStoreSurvivesClone(t *testing.T) {
	client := NewClient(&Options{CommandMetadata: &CommandMetadataConfig{
		Mode: CommandMetadataPreferLive, Overrides: map[string]*CommandInfo{"get": nil},
	}})
	t.Cleanup(func() { _ = client.Close() })
	clone := client.WithTimeout(time.Second)
	if isCacheableInView(clone.metadataView(), makeCmd("get", "k")) {
		t.Fatal("clone lost the application override")
	}
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.cmdMeta.stop:
	default:
		t.Fatal("closing the clone did not stop its metadata owner")
	}
}

// Container commands resolve by their "parent|child" name.
func TestIsCacheable_ContainerSubcommands(t *testing.T) {
	if cmd := makeCmd("memory", "usage", "k"); isCacheable(cmd) {
		t.Error("MEMORY USAGE must not be cacheable: usage changes on mutations that never invalidate")
	}
	// Case-insensitive container lookup still resolves the subcommand record.
	if meta, ok := cscCommandMetaFor(makeCmd("MEMORY", "USAGE", "k")); !ok || meta.bits&cscTipDontCache == 0 {
		t.Error("MEMORY USAGE lookup must be case-insensitive and hit the override record")
	}
	if cmd := makeCmd("xinfo", "stream", "s"); isCacheable(cmd) {
		t.Error("XINFO STREAM must not be cacheable: group mutations don't invalidate the stream key")
	}
	if cmd := makeCmd("object", "encoding", "k"); isCacheable(cmd) {
		t.Error("OBJECT ENCODING has nondeterministic_output and must not be cacheable")
	}
	if cmd := makeCmd("xinfo", "consumers", "s", "g"); isCacheable(cmd) {
		t.Error("XINFO CONSUMERS has nondeterministic_output and must not be cacheable")
	}
	if cmd := makeCmd("memory", "doctor"); isCacheable(cmd) {
		t.Error("MEMORY DOCTOR is keyless and must not be cacheable")
	}
	if cmd := makeCmd("memory"); isCacheable(cmd) {
		t.Error("bare container parent must not be cacheable")
	}
}

func TestIsCacheable_RawWriteToRejected(t *testing.T) {
	cmd := NewRawWriteToCmd(context.Background(), &bytes.Buffer{}, "get", "k")
	if isCacheable(cmd) {
		t.Fatal("RawWriteToCmd must bypass CSC to preserve direct streaming")
	}
}

func TestIsSelectCmd(t *testing.T) {
	for _, cmd := range []Cmder{
		makeCmd("select", 1),
		makeCmd("SELECT", 1),
		makeCmd([]byte("select"), 1),
	} {
		if !isSelectCmd(cmd) {
			t.Errorf("expected %v to match SELECT", cmd.Args())
		}
	}
	for _, cmd := range []Cmder{
		makeCmd("get", "select"),
		makeCmd("swapdb", 0, 1),
	} {
		if isSelectCmd(cmd) {
			t.Errorf("expected %v not to match SELECT", cmd.Args())
		}
	}
}

func TestCSCStateCommandMatchers(t *testing.T) {
	for _, cmd := range []Cmder{
		makeCmd("auth", "password"),
		makeCmd("AUTH", "user", "password"),
		makeCmd([]byte("auth"), "password"),
	} {
		if !isAuthCmd(cmd) {
			t.Errorf("expected %v to match AUTH", cmd.Args())
		}
	}
	if isAuthCmd(makeCmd("get", "auth")) {
		t.Fatal("GET auth must not match AUTH")
	}

	for _, cmd := range []Cmder{
		makeCmd("hello", 2),
		makeCmd("HELLO", 3),
		makeCmd([]byte("hello"), []byte("2")),
	} {
		if !isProtocolChangingHelloCmd(cmd) {
			t.Errorf("expected %v to match state-changing HELLO", cmd.Args())
		}
	}
	for _, cmd := range []Cmder{
		makeCmd("hello"),
		makeCmd("get", "hello"),
	} {
		if isProtocolChangingHelloCmd(cmd) {
			t.Errorf("expected %v not to match state-changing HELLO", cmd.Args())
		}
	}

	for _, cmd := range []Cmder{
		makeCmd("reset"),
		makeCmd("RESET"),
		makeCmd([]byte("reset")),
	} {
		if !isResetCmd(cmd) {
			t.Errorf("expected %v to match RESET", cmd.Args())
		}
	}
	if isResetCmd(makeCmd("config", "resetstat")) {
		t.Fatal("CONFIG RESETSTAT must not match RESET")
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

func TestBuildCacheKey_RejectsBinaryMarshaler(t *testing.T) {
	cmd := makeCmd("GET", cscWireSmuggler{wire: "key", str: "key"})
	if key, ok := buildCacheKey(cmd); ok || key != "" {
		t.Fatalf("BinaryMarshaler produced cache key %q (ok=%v), want fail closed", key, ok)
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

	cmd = makeCmd("ZINTERCARD", 3, "z1", "z2", "z3")
	keys = extractRedisKeys(cmd)
	if len(keys) != 3 || keys[0] != "z1" || keys[2] != "z3" {
		t.Errorf("ZINTERCARD: expected [z1 z2 z3], got %v", keys)
	}
}

func TestExtractRedisKeys_NumKeysFailClosed(t *testing.T) {
	// A numkeys larger than the argument list must give nil, not a
	// truncated key list.
	for _, cmd := range []Cmder{
		makeCmd("ZDIFF", 5, "z1", "z2"),
		makeCmd("ZDIFF", 0, "z1"),
		makeCmd("ZDIFF", "not-a-number", "z1"),
		makeCmd("ZDIFF"),
		makeCmd("ZDIFF", "999999999999999999", "z1"),
	} {
		if keys := extractRedisKeys(cmd); keys != nil {
			t.Errorf("%v: expected nil keys, got %v", cmd.Args(), keys)
		}
	}
}

func TestExtractRedisKeys_RangeShortArgsFailClosed(t *testing.T) {
	// Too few arguments must give nil, not a partial key list.
	for _, cmd := range []Cmder{
		makeCmd("LCS", "k1"),
		makeCmd("LCS"),
		makeCmd("GET"),
	} {
		if keys := extractRedisKeys(cmd); keys != nil {
			t.Errorf("%v: expected nil keys, got %v", cmd.Args(), keys)
		}
	}
}

func TestCSCHasKeyArgument(t *testing.T) {
	// Keyedness is proven by a complete key spec OR the legacy firstKey/step
	// pair; lastKey is never consulted (it is -1 for variadic key lists).
	cases := []struct {
		meta cscCommandMeta
		want bool
	}{
		{cscCommandMeta{bits: cscFlagReadonly | cscHasKeySpec}, true},
		{cscCommandMeta{bits: cscFlagReadonly, firstKey: 1, step: 1}, true},
		{cscCommandMeta{bits: cscFlagReadonly, firstKey: 1, lastKey: -1, step: 2}, true},
		{cscCommandMeta{bits: cscFlagReadonly}, false},
		{cscCommandMeta{bits: cscFlagReadonly, firstKey: 1}, false}, // step 0
		{cscCommandMeta{bits: cscFlagReadonly, step: 1}, false},     // firstKey 0
	}
	for _, tc := range cases {
		if got := cscHasKeyArgument(tc.meta); got != tc.want {
			t.Errorf("cscHasKeyArgument(%+v) = %v, want %v", tc.meta, got, tc.want)
		}
	}
}

func TestExtractRedisKeys_LCS(t *testing.T) {
	cmd := makeCmd("LCS", "key1", "key2")
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "key1" || keys[1] != "key2" {
		t.Errorf("LCS: expected [key1 key2], got %v", keys)
	}
}

func TestExtractRedisKeys_KeylessCommand(t *testing.T) {
	cmd := makeCmd("ping")
	keys := extractRedisKeys(cmd)
	if keys != nil {
		t.Errorf("expected nil for keyless command, got %v", keys)
	}
}

func TestExtractRedisKeys_SortROByGetUncached(t *testing.T) {
	// Eligibility is command-level, so SORT_RO stays eligible; the BY/GET
	// forms read pattern keys this call cannot list, so THAT invocation gets
	// no key list and is served uncached.
	if cmd := makeCmd("sort_ro", "mylist", "LIMIT", "0", "10", "ALPHA"); !isCacheable(cmd) {
		t.Error("SORT_RO should be eligible at the command level")
	}
	if keys := extractRedisKeys(makeCmd("sort_ro", "mylist", "LIMIT", int64(0), int64(10), "ALPHA")); len(keys) != 1 || keys[0] != "mylist" {
		t.Errorf("plain SORT_RO: expected [mylist], got %v", keys)
	}
	for _, cmd := range []Cmder{
		makeCmd("sort_ro", "mylist", "BY", "weight_*"),
		makeCmd("sort_ro", "mylist", "get", "obj_*"),
		makeCmd("sort_ro", "mylist", "LIMIT", "0", "10", "By", "weight_*", "ALPHA"),
		makeCmd("sort_ro", "mylist", []byte("bY"), "weight_*"),
		makeCmd("sort_ro", "mylist", []byte("GET"), "obj_*"),
	} {
		if keys := extractRedisKeys(cmd); keys != nil {
			t.Errorf("%v: expected nil keys, got %v", cmd.Args(), keys)
		}
	}
	by := "BY"
	if keys := extractRedisKeys(makeCmd("sort_ro", "mylist", &by, "weight_*")); keys != nil {
		t.Errorf("pointer-encoded BY: expected nil keys, got %v", keys)
	}
}

type nonComparableCache struct {
	Cache
	marker []byte
}

type typedNilCache struct{ Cache }

type operationDurationRecorder struct {
	OTelRecorder
	calls    atomic.Int32
	attempts atomic.Int32
}

func (r *operationDurationRecorder) RecordOperationDuration(
	_ context.Context,
	_ time.Duration,
	_ Cmder,
	attempts int,
	_ error,
	_ ConnInfo,
	_ int,
) {
	r.calls.Add(1)
	r.attempts.Store(int32(attempts))
}

func testCSCNamespacedKey(db int, key string) string {
	return cscNamespacedKey(cscNamespacePrefix(db, ""), key)
}

// testCSCEntryKey builds the cache-entry key processCached would use under
// the default metadata view.
func testCSCEntryKey(db int, rawKey string) string {
	return cscEntryKey(cscNamespacePrefix(db, ""), defaultCommandMetadataView().cscFingerprint, rawKey)
}

type unusedStreamingProvider struct{}

func (unusedStreamingProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	panic("Subscribe must not be called without a connection")
}

func TestAttachCSC_EnabledForExplicitCache(t *testing.T) {
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: NewLocalCache(CacheConfig{MaxEntries: 16}),
	})
	defer client.Close()
	if client.csc == nil {
		t.Fatal("CSC must be enabled for an owner-aware cache")
	}
	if client.staticCmdMeta == nil || client.staticCmdMeta != client.metadataView() {
		t.Fatal("CSC must cache the shared default metadata view")
	}
}

func TestAttachCSC_DisablesForTypedNilCache(t *testing.T) {
	var cache *typedNilCache
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: cache,
	})
	defer client.Close()

	if client.csc != nil || client.cscTrackingRequested() || client.staticCmdMeta != nil {
		t.Fatal("a typed-nil cache must leave CSC disabled")
	}
}

func TestAttachCSC_DisabledForCredentialProviders(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Options)
	}{
		{
			name: "streaming",
			configure: func(opt *Options) {
				opt.StreamingCredentialsProvider = unusedStreamingProvider{}
			},
		},
		{
			name: "context",
			configure: func(opt *Options) {
				opt.CredentialsProviderContext = func(context.Context) (string, string, error) {
					panic("provider must not be called without a connection")
				}
			},
		},
		{
			name: "legacy",
			configure: func(opt *Options) {
				opt.CredentialsProvider = func() (string, string) {
					panic("provider must not be called without a connection")
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opt := &Options{
				Addr:                  "127.0.0.1:0", // never dialed
				Protocol:              3,
				ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
			}
			tc.configure(opt)
			client := NewClient(opt)
			t.Cleanup(func() { _ = client.Close() })

			if client.csc != nil || client.cscActive != nil {
				t.Fatal("CSC must stay detached when credentials can vary by identity")
			}
			if client.cscTrackingRequested() {
				t.Fatal("dynamic credentials must not enable CLIENT TRACKING for CSC")
			}
		})
	}
}

func TestAttachCSC_AllowsFixedCredentials(t *testing.T) {
	client := NewClient(&Options{
		Addr:                  "127.0.0.1:0", // never dialed
		Protocol:              3,
		Username:              "fixed-user",
		Password:              "fixed-password",
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	if client.csc == nil || client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("fixed Username/Password must remain compatible with CSC")
	}
}

func TestSharedCacheSeparatesFixedCredentialIdentities(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	clientA := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		Username:        "privileged",
		Password:        "secret-a",
		ClientSideCache: cache,
	})
	clientB := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		Username:        "restricted",
		Password:        "secret-b",
		ClientSideCache: cache,
	})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})

	if clientA.cscKeyPrefix == clientB.cscKeyPrefix {
		t.Fatal("different fixed ACL identities must not share a cache namespace")
	}
	if strings.Contains(clientA.cscKeyPrefix, "secret-a") ||
		strings.Contains(clientB.cscKeyPrefix, "secret-b") {
		t.Fatal("cache namespaces must not retain plaintext passwords")
	}

	redisKeyA := cscNamespacedKey(clientA.cscKeyPrefix, "secret")
	redisKeyB := cscNamespacedKey(clientB.cscKeyPrefix, "secret")
	cacheKeyA := cscNamespacedKey(clientA.cscKeyPrefix, "get-secret")
	cacheKeyB := cscNamespacedKey(clientB.cscKeyPrefix, "get-secret")
	if !cache.set(cacheKeyA, []string{redisKeyA}, []byte("a")) ||
		!cache.set(cacheKeyB, []string{redisKeyB}, []byte("b")) {
		t.Fatal("failed to seed identity-scoped cache entries")
	}

	handlerA := lookupInvalidateHandler(clientA.pushProcessor)
	if handlerA == nil {
		t.Fatal("client A invalidate handler is missing")
	}
	if err := handlerA.HandlePushNotification(
		context.Background(),
		push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, []interface{}{"secret"}},
	); err != nil {
		t.Fatalf("handle identity-scoped invalidation: %v", err)
	}
	if _, ok := cache.Get(context.Background(), cacheKeyA); ok {
		t.Fatal("client A invalidation did not delete its identity-scoped entry")
	}
	if value, ok := cache.Get(context.Background(), cacheKeyB); !ok || string(value) != "b" {
		t.Fatal("client A invalidation crossed into client B's identity namespace")
	}
}

// TestInvalBatchWindowRebuildOnChange pins #3965: a running batcher's window is
// fixed at creation, so when a second client binds to the same shared handler
// with a different window, setInvalBatchWindow must drop the running batcher so
// the next invalidation starts a fresh one with the new (e.g. stricter) window.
func TestInvalBatchWindowRebuildOnChange(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })

	h.setInvalBatchWindow(time.Second)
	b1 := h.ensureBatcher()
	if b1 == nil || b1.window != time.Second {
		t.Fatalf("first batcher window = %v, want 1s", b1)
	}

	// Stricter window from a second binding: the running batcher must be dropped.
	h.setInvalBatchWindow(10 * time.Millisecond)
	h.mu.Lock()
	running := h.batcher
	h.mu.Unlock()
	if running != nil {
		t.Fatal("batcher not dropped on window change — the stale window would be kept")
	}
	b2 := h.ensureBatcher()
	if b2 == nil || b2.window != 10*time.Millisecond {
		t.Fatalf("rebuilt batcher window = %v, want 10ms", b2)
	}
	if b2 == b1 {
		t.Fatal("expected a fresh batcher after the window change")
	}

	// Same window again must not churn the batcher.
	h.setInvalBatchWindow(10 * time.Millisecond)
	h.mu.Lock()
	same := h.batcher
	h.mu.Unlock()
	if same != b2 {
		t.Fatal("setInvalBatchWindow with an unchanged window must not rebuild the batcher")
	}

	// LOOSER window from a later binding must NOT apply: the effective window is
	// the strictest across attached clients, or the 10ms client's staleness bound
	// would be silently violated by a 1s attach.
	h.setInvalBatchWindow(time.Second)
	h.mu.Lock()
	kept, keptWindow := h.batcher, h.invalBatchWindow
	h.mu.Unlock()
	if kept != b2 || keptWindow != 10*time.Millisecond {
		t.Fatalf("a looser window applied over a stricter one: batcher rebuilt=%v window=%v, want kept 10ms", kept != b2, keptWindow)
	}

	// Explicit 0 (batching off, inline deletes) is strictest of all and must win.
	h.setInvalBatchWindow(0)
	h.mu.Lock()
	zeroWindow, zeroBatcher := h.invalBatchWindow, h.batcher
	h.mu.Unlock()
	if zeroWindow != 0 || zeroBatcher != nil {
		t.Fatalf("explicit 0 window must win (inline) and drop the batcher: window=%v batcher=%v", zeroWindow, zeroBatcher)
	}
	// ...and a later nonzero window must not loosen past it.
	h.setInvalBatchWindow(time.Second)
	h.mu.Lock()
	afterZero := h.invalBatchWindow
	h.mu.Unlock()
	if afterZero != 0 {
		t.Fatalf("nonzero window applied over an explicit 0: window=%v, want 0 (inline stays strictest)", afterZero)
	}
}

// TestInvalBatchStopAppliesQueuedDeletes pins #3965 (cursor High): stopping the
// batcher — as a window-change rebuild does — must apply the deletes still
// buffered in its channel, not just the in-progress batch, or the cache serves
// pre-invalidation values until TTL/MaxStaleness.
func TestInvalBatchStopAppliesQueuedDeletes(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour) // never fires on its own in this test

	nsKey := cscNamespacedKey("p:", "sq")
	cacheKey := cscNamespacedKey("p:", "get:sq")
	if !cache.set(cacheKey, []string{nsKey}, []byte("stale")) {
		t.Fatal("seed")
	}
	// Queue the delete on the batcher (1h window: buffered, not applied).
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, []interface{}{"sq"}}); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	if _, ok := cache.Get(ctx, cacheKey); !ok {
		t.Fatal("delete applied before the window elapsed — batching not in effect, test proves nothing")
	}

	// Tighten the window: the rebuild stops the 1h batcher, whose stop path must
	// drain + apply the queued delete.
	h.setInvalBatchWindow(10 * time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := cache.Get(ctx, cacheKey); !ok {
			break // delete applied
		}
		if time.Now().After(deadline) {
			t.Fatal("queued delete lost by the batcher stop/rebuild — entry still served")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestInvalBatchEnqueueAfterStopAppliesInline pins #3965 (cursor): a handler
// goroutine can hold a batcher pointer across a concurrent stop (window-change
// rebuild mid-enqueue-loop). enqueue on a stopped batcher must apply the delete
// inline — a key parked in a channel nothing drains would serve
// pre-invalidation values until TTL/MaxStaleness.
func TestInvalBatchEnqueueAfterStopAppliesInline(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour)
	b := h.ensureBatcher()
	if b == nil {
		t.Fatal("no batcher")
	}

	nsKey := cscNamespacedKey("p:", "as")
	cacheKey := cscNamespacedKey("p:", "get:as")
	if !cache.set(cacheKey, []string{nsKey}, []byte("stale")) {
		t.Fatal("seed")
	}

	b.stop()
	b.enqueue(nsKey) // stopped: must delete inline, not park in b.ch
	if _, ok := cache.Get(ctx, cacheKey); ok {
		t.Fatal("enqueue on a stopped batcher parked the delete — entry still served")
	}
}

// TestInvalBatchDroppedOnFlush pins #3965: a full cache Flush (FLUSHDB/FLUSHALL)
// must discard the batcher's queued per-key deletes, or a delete queued before
// the flush fires afterward and evicts an entry repopulated post-flush.
func TestInvalBatchDroppedOnFlush(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(80 * time.Millisecond) // batched; long enough not to fire before the flush

	nsKey := cscNamespacedKey("p:", "x")
	cacheKey := cscNamespacedKey("p:", "get:x")
	if !cache.set(cacheKey, []string{nsKey}, []byte("v1")) {
		t.Fatal("seed")
	}

	// Batched invalidation for x: queued, not yet applied (window not elapsed).
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, []interface{}{"x"}}); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	// Full flush: clears the cache AND must drop the queued delete for x.
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, nil}); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Repopulate x after the flush, then wait past the window: the dropped delete
	// must NOT fire and evict the fresh entry.
	if !cache.set(cacheKey, []string{nsKey}, []byte("v2")) {
		t.Fatal("repopulate")
	}
	time.Sleep(200 * time.Millisecond)
	if v, ok := cache.Get(ctx, cacheKey); !ok || string(v) != "v2" {
		t.Fatalf("repopulated entry evicted by a stale batched delete after flush: got %q ok=%v, want v2", v, ok)
	}
}

func TestAttachCSC_HandlerConflictDoesNotEnableTracking(t *testing.T) {
	proc := push.NewProcessor()
	if err := proc.RegisterHandler(invalidatePushName, &recordingHandler{}, true); err != nil {
		t.Fatalf("register foreign invalidate handler: %v", err)
	}

	client := NewClient(&Options{
		Addr:                      "127.0.0.1:0",
		Protocol:                  3,
		PushNotificationProcessor: proc,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	if client.csc != nil || client.cscActive != nil {
		t.Fatal("a handler conflict must leave CSC fully detached")
	}
	if client.cscTrackingRequested() {
		t.Fatal("a configured cache whose attachment failed must not enable tracking")
	}
	if err := client.cscCommandError(NewCmd(context.Background(), "select", 1)); err != nil {
		t.Fatalf("a configured cache whose attachment failed rejected SELECT: %v", err)
	}
}

// TestFulfillCached_FailsClosedOnZeroConnID: with an active eviction hook a real
// serving conn id is an invariant; a zero id would leave the entry unattributed
// and never evicted on close, so fulfillCached must fail closed (not cache it).
func TestFulfillCached_FailsClosedOnZeroConnID(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	if c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v")}, defaultCommandMetadataView()) {
		t.Fatal("fulfillCached must fail closed when an eviction hook is active and connID==0")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("unattributed entry must not be cached")
	}
}

func TestProcessCached_HitHonorsCanceledContext(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	c := &baseClient{
		opt:          &Options{Protocol: 3},
		csc:          cache,
		cscKeyPrefix: cscNamespacePrefix(0, ""),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := NewStringCmd(ctx, "get", "k")
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		t.Fatal("buildCacheKey failed")
	}
	cacheKey := testCSCEntryKey(0, rawKey)
	if !cache.set(cacheKey, []string{testCSCNamespacedKey(0, "k")}, []byte("$1\r\nv\r\n")) {
		t.Fatal("failed to seed cache")
	}
	cancel()

	view := c.metadataView()
	meta, ok := cscEligibleMeta(view, cmd)
	if !ok {
		t.Fatal("GET must be eligible")
	}
	if err := c.processCached(ctx, cmd, nil, view, meta, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached hit with canceled context: got %v, want context.Canceled", err)
	}
}

func TestProcessCached_NilHitIsTerminal(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	c := &baseClient{
		opt:          &Options{Protocol: 3},
		csc:          cache,
		cscKeyPrefix: cscNamespacePrefix(0, ""),
	}

	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "missing")
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		t.Fatal("buildCacheKey failed")
	}
	cacheKey := testCSCEntryKey(0, rawKey)
	if !cache.set(cacheKey, []string{testCSCNamespacedKey(0, "missing")}, []byte("$-1\r\n")) {
		t.Fatal("failed to seed negative cache entry")
	}

	view := c.metadataView()
	meta, ok := cscEligibleMeta(view, cmd)
	if !ok {
		t.Fatal("GET must be eligible")
	}
	if err := c.processCached(ctx, cmd, nil, view, meta, 0); err != Nil {
		t.Fatalf("negative cache hit: got %v, want redis.Nil", err)
	}
	if cache.Len() != 1 {
		t.Fatal("a valid redis.Nil cache hit must not be deleted")
	}
}

// TestProcessCached_CoalescerBailCarriesAttempt pins the convergence follow-up
// from codex on #3989 ("Count the failed coalesced attempt in retry metrics"):
// when a coalesced miss reached a session connection and then failed with a
// session/transport error, the pooled re-run must start at attempt 1, so the
// coalesced attempt is counted in state.attempts (the OTel duration metric)
// and consumes one unit of retry budget — instead of processWithRetry
// restarting from zero and reporting one attempt for an operation that made
// two. A bail that never reached a session (a pre-queue shed) carries nothing.
//
// Deterministic: the "session" is a goroutine that dequeues the request,
// attributes it to a connection (as the writer does before the write) and
// settles it with a session error; the re-run then fails at once on a pooler
// whose Get always errors (non-retryable), so processWithRetry runs exactly
// one iteration. MaxRetries=1 keeps the seed visible (processWithRetry clamps
// startAttempt to MaxRetries): 2 attempts when the coalesced attempt reached a
// conn, 1 otherwise.
// TestProcessCached_CoalescerBailCarriesAttempt pins the accounting of a coalesced
// miss that reached a session connection and then failed with a session error
// (codex on #3989 and #4002). That attempt counts against MaxRetries+1 and is
// visible in the OTel state on every exit: the pooled re-run (seeded from
// startAttempt), the exhausted-budget return (no re-run: it would be one attempt
// over budget), and the takeover-hit return (another waiter re-fetched the key
// while this one was failing). A pre-queue shed reached no connection and carries
// nothing.
func TestProcessCached_CoalescerBailCarriesAttempt(t *testing.T) {
	type fixture struct {
		c      *baseClient
		mc     *cscMissCoalescer
		cache  *LocalCache
		pooler *erroringPooler
	}
	newFixture := func(maxRetries int) fixture {
		cache := NewLocalCache(CacheConfig{MaxEntries: 16})
		pooler := &erroringPooler{}
		c := &baseClient{
			opt:          &Options{Protocol: 3, MaxRetries: maxRetries},
			csc:          cache,
			cscKeyPrefix: cscNamespacePrefix(0, ""),
			connPool:     pooler,
		}
		mc := &cscMissCoalescer{c: c, ch: make(chan *cscMissReq, 1), stop: make(chan struct{})}
		c.cscMissCoalescer.Store(mc)
		return fixture{c: c, mc: mc, cache: cache, pooler: pooler}
	}
	sessionConn := func(t *testing.T) *pool.Conn {
		t.Helper()
		server, client := net.Pipe()
		t.Cleanup(func() { server.Close(); client.Close() })
		return pool.NewConn(client)
	}
	// run drives processCached from startAttempt and returns the command, the
	// OTel state, and the error.
	run := func(t *testing.T, c *baseClient, startAttempt int) (*StringCmd, processState, error) {
		t.Helper()
		ctx := context.Background()
		cmd := NewStringCmd(ctx, "get", "k")
		var state processState
		done := make(chan error, 1)
		view := c.metadataView()
		meta, ok := cscEligibleMeta(view, cmd)
		if !ok {
			t.Fatal("GET must be eligible")
		}
		go func() { done <- c.processCached(ctx, cmd, &state, view, meta, startAttempt) }()
		select {
		case err := <-done:
			return cmd, state, err
		case <-time.After(5 * time.Second):
			t.Fatal("processCached did not return")
			return nil, state, nil
		}
	}
	sessErr := errors.New("csc test: session read failed")
	// failOnSession plays the coalescer session: dequeue the request, attribute it
	// to cn (the writer does this for a whole batch before the write), fail it.
	failOnSession := func(mc *cscMissCoalescer, cn *pool.Conn) {
		go func() {
			req := <-mc.ch
			req.servedBy = cn
			mc.settleErr(req, sessErr)
		}()
	}

	t.Run("reached_session_conn_counts", func(t *testing.T) {
		f := newFixture(1)
		cn := sessionConn(t)
		failOnSession(f.mc, cn)
		_, state, err := run(t, f.c, 0)
		if err == nil {
			t.Fatal("processCached succeeded; the erroring pooler must fail the re-run")
		}
		if state.attempts != 2 {
			t.Fatalf("state.attempts = %d; want 2 (the coalesced attempt on the session conn "+
				"plus the pooled re-run)", state.attempts)
		}
		if f.pooler.gets.Load() == 0 {
			t.Fatal("1 of 2 attempts spent: the re-run must reach the pool")
		}
		// The re-run never acquired a connection, so the last one that saw the
		// command is the session conn; processWithRetry keeps it, not nil.
		if state.lastConn != cn {
			t.Fatalf("state.lastConn = %v; want the session conn", state.lastConn)
		}
	})

	t.Run("pre_queue_shed_carries_nothing", func(t *testing.T) {
		f := newFixture(1)
		// Saturate the wire budget so fetch sheds before queueing: served == nil.
		f.mc.wireBytes.Store(cscMissWireBudgetBytes)
		_, state, err := run(t, f.c, 0)
		if err == nil {
			t.Fatal("processCached succeeded; the erroring pooler must fail the re-run")
		}
		if state.attempts != 1 {
			t.Fatalf("state.attempts = %d; want 1 (a shed never reached a connection)", state.attempts)
		}
	})

	// The coalesced attempt spends the last of the budget: no re-run. Two shapes —
	// retries disabled from a fresh start, and the FD divert's startAttempt=1 with
	// MaxRetries=1 (codex on #4002: three executions on a two-attempt budget).
	for _, tc := range []struct {
		name              string
		maxRetries, start int
	}{
		{"budget_exhausted_no_retries", 0, 0},
		{"budget_exhausted_after_fd_attempt", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(tc.maxRetries)
			cn := sessionConn(t)
			type errCall struct {
				cn      *pool.Conn
				retries int
			}
			var (
				callsMu sync.Mutex
				calls   []errCall
			)
			pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
				Error: func(_ context.Context, _ string, cn *pool.Conn, _ string, _ bool, retries int) {
					callsMu.Lock()
					calls = append(calls, errCall{cn, retries})
					callsMu.Unlock()
				},
			})
			t.Cleanup(func() { pool.SetAllMetricCallbacks(nil) })
			failOnSession(f.mc, cn)
			cmd, state, err := run(t, f.c, tc.start)
			if err != sessErr {
				t.Fatalf("err = %v; want the session cause %v itself, unwrapped like "+
					"processWithRetry's own exhaustion return", err, sessErr)
			}
			if cmd.Err() != sessErr {
				t.Fatalf("cmd.Err() = %v; want %v", cmd.Err(), sessErr)
			}
			if got := f.pooler.gets.Load(); got != 0 {
				t.Fatalf("pool Get called %d times; want 0: the budget is spent, a re-run "+
					"would be one attempt too many", got)
			}
			want := tc.start + 1
			if state.attempts != want {
				t.Fatalf("state.attempts = %d; want %d", state.attempts, want)
			}
			if state.lastConn != cn {
				t.Fatalf("state.lastConn = %v; want the session conn", state.lastConn)
			}
			callsMu.Lock()
			defer callsMu.Unlock()
			if len(calls) != 1 || calls[0].cn != cn || calls[0].retries != want-1 {
				t.Fatalf("error metric calls = %+v; want exactly one on the session conn "+
					"with retries=%d (the skipped re-run's emission)", calls, want-1)
			}
		})
	}

	// Takeover hit: while this request was failing, another waiter re-reserved the
	// key and fulfilled it, so the re-Reserve loses and the value comes from the
	// cache with no re-run. The coalesced attempt must still be reported.
	t.Run("takeover_hit_reports_attempt", func(t *testing.T) {
		f := newFixture(1)
		cn := sessionConn(t)
		rival := make(chan error, 1)
		go func() {
			req := <-f.mc.ch
			req.servedBy = cn
			// settleErr split open so a rival fits between its cancel and its wake:
			// cancel this reservation, let "another waiter" reserve and fulfill the
			// key, then fail this request with the tagged session error.
			f.cache.Cancel(req.cacheKey, req.token)
			tok, ok := f.cache.Reserve(req.cacheKey, []string{cscNamespacedKey(f.c.cscKeyPrefix, "k")})
			if !ok || !f.cache.FulfillOwned(req.cacheKey, tok, 0, []byte("$1\r\nv\r\n")) {
				rival <- errors.New("rival reserve/fulfill failed")
			} else {
				rival <- nil
			}
			f.mc.settle(req, cscSessionError{sessErr})
		}()
		cmd, state, err := run(t, f.c, 0)
		if rerr := <-rival; rerr != nil {
			t.Fatalf("precondition: %v", rerr)
		}
		if err != nil {
			t.Fatalf("processCached: %v; want the rival's cached value", err)
		}
		if got := cmd.Val(); got != "v" {
			t.Fatalf("cmd.Val() = %q; want the rival's value", got)
		}
		if got := f.pooler.gets.Load(); got != 0 {
			t.Fatalf("pool Get called %d times; want 0: a takeover hit needs no re-run", got)
		}
		if state.attempts != 1 {
			t.Fatalf("state.attempts = %d; want 1 (the coalesced attempt on the session conn)",
				state.attempts)
		}
		if state.lastConn != cn {
			t.Fatalf("state.lastConn = %v; want the session conn", state.lastConn)
		}
	})
}

func TestProcessCached_RecordsCacheHitDuration(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: cache,
	})
	t.Cleanup(func() {
		SetOTelRecorder(nil)
		_ = client.Close()
	})

	cmd := NewStringCmd(context.Background(), "get", "key")
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		t.Fatal("buildCacheKey failed")
	}
	cacheKey := cscEntryKey(client.cscKeyPrefix, client.metadataView().cscFingerprint, rawKey)
	if !cache.set(cacheKey, []string{cscNamespacedKey(client.cscKeyPrefix, "key")},
		[]byte("$5\r\nvalue\r\n")) {
		t.Fatal("failed to seed cache")
	}

	recorder := &operationDurationRecorder{}
	SetOTelRecorder(recorder)
	if got, err := client.Get(context.Background(), "key").Result(); err != nil || got != "value" {
		t.Fatalf("cached GET: value=%q err=%v", got, err)
	}
	if got := recorder.calls.Load(); got != 1 {
		t.Fatalf("operation duration calls: got %d, want 1", got)
	}
	if got := recorder.attempts.Load(); got != 0 {
		t.Fatalf("cache hit attempts: got %d, want 0", got)
	}
}

// TestCSCActive_ClonesStopServingWhenDrainerStops: a WithTimeout clone shares the
// owner's cscActive flag; stopping the owner's drainer (Close, or the GC cleanup)
// flips it, so the clone stops serving hits nothing is invalidating.
func TestCSCActive_ClonesStopServingWhenDrainerStops(t *testing.T) {
	client := NewClient(&Options{
		Addr:                  "127.0.0.1:0",
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	defer client.Close()
	if client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("precondition: cscActive must be set true when CSC is enabled")
	}

	clone := client.WithTimeout(time.Second)
	if clone.cscActive != client.cscActive {
		t.Fatal("WithTimeout clone must share the owner's cscActive flag")
	}

	client.baseClient.stopBackgroundDrainer()
	if clone.cscActive.Load() {
		t.Fatal("clone must observe cscActive=false once the owner's drainer stops")
	}
}

func TestStopBackgroundDrainerEvictsSharedCacheCoverage(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: cache,
	})
	t.Cleanup(func() { _ = client.Close() })

	hook := client.cscHook()
	if hook == nil {
		t.Fatal("precondition: shared-cache client must install its coverage hook")
	}
	const connID = uint64(44)
	hook.bumpInitGen(connID)
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, connID, []byte("v")) {
		t.Fatal("failed to seed shared cache entry")
	}

	client.stopBackgroundDrainer()
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("stopping one client's drainer must evict that pool's shared-cache entries")
	}
}

// TestCSCActive_CloneKeepsOwnerAlive: a surviving WithTimeout clone retains the
// canonical wrapper whose GC cleanup owns the shared drainer.
func TestCSCActive_CloneKeepsOwnerAlive(t *testing.T) {
	clone, active := func() (*Client, *atomic.Bool) {
		owner := NewClient(&Options{
			Addr:                  "127.0.0.1:0",
			Protocol:              3,
			ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		})
		cl := owner.WithTimeout(time.Second)
		// owner falls out of lexical scope here, but cl must keep it reachable
		// because its cleanup owns the drainer cl relies on.
		return cl, owner.cscActive
	}()

	if active == nil {
		t.Fatal("precondition: cscActive must be set")
	}
	for range 20 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	if !active.Load() {
		t.Fatal("a reachable clone must keep its CSC drainer owner alive")
	}
	if clone.lifecycleOwner == nil {
		t.Fatal("CSC clone must retain its canonical lifecycle owner")
	}
	if err := clone.Close(); err != nil {
		t.Fatalf("close clone: %v", err)
	}
	if active.Load() {
		t.Fatal("closing a CSC clone must stop its canonical owner's drainer")
	}
}

// TestReadBufferSize_ClampedForRESP3: a read buffer too small to hold a push
// header is clamped for RESP3 so client-reserved Pub/Sub frames are never
// consumed before their name is known.
func TestReadBufferSize_ClampedForRESP3(t *testing.T) {
	opt := &Options{Addr: "x:1", Protocol: 3, ReadBufferSize: 16}
	opt.init()
	if opt.ReadBufferSize != proto.MinRESP3ReadBufferSize {
		t.Fatalf("RESP3 ReadBufferSize should clamp to %d, got %d",
			proto.MinRESP3ReadBufferSize, opt.ReadBufferSize)
	}
}

// TestReadBufferSize_NotClampedForRESP2: RESP2 has no push frames, so a small
// buffer is left as configured.
func TestReadBufferSize_NotClampedForRESP2(t *testing.T) {
	opt := &Options{Addr: "x:1", Protocol: 2, ReadBufferSize: 16}
	opt.init()
	if opt.ReadBufferSize != 16 {
		t.Fatalf("RESP2 ReadBufferSize should not be clamped, got %d", opt.ReadBufferSize)
	}
}

// TestProcessCached_CachesServerNilReply exercises the complete miss/fill/hit
// path. The second GET must be answered locally even though the cached command
// still returns redis.Nil to its caller.
func TestProcessCached_CachesServerNilReply(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var getCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveNegativeCacheTestConn(netConn, &getCalls)
		}
	}()

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCache: cache,
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if err := client.Get(context.Background(), "missing").Err(); err != Nil {
			t.Fatalf("GET %d: got %v, want redis.Nil", i+1, err)
		}
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("server received %d GETs, want 1 (second lookup should hit CSC)", got)
	}
	if cache.Len() != 1 {
		t.Fatalf("negative lookup was not retained in CSC, Len=%d", cache.Len())
	}
}

func serveNegativeCacheTestConn(netConn net.Conn, getCalls *atomic.Int32) {
	serveTestRESPConn(netConn, func(command string) string {
		switch command {
		case "hello":
			return "%0\r\n"
		case "get":
			getCalls.Add(1)
			return "$-1\r\n"
		default:
			return "+OK\r\n"
		}
	})
}

func serveTestRESPConn(netConn net.Conn, replyFor func(command string) string) {
	defer netConn.Close()

	scanner := bufio.NewScanner(netConn)
	for scanner.Scan() {
		header := scanner.Text()
		if !strings.HasPrefix(header, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
		if err != nil || n <= 0 {
			return
		}

		command := ""
		for i := 0; i < n; i++ {
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
				return
			}
			if i == 0 {
				command = strings.ToLower(scanner.Text())
			}
		}

		if _, err := netConn.Write([]byte(replyFor(command))); err != nil {
			return
		}
	}
}

// TestIsClientTrackingCmd pins the guard's matcher: any CLIENT TRACKING
// subcommand matches, other CLIENT subcommands (incl. TRACKINGINFO) do not.
func TestIsClientTrackingCmd(t *testing.T) {
	tracking := "tracking"
	matching := []Cmder{
		makeCmd("client", "tracking", "on"),
		makeCmd("client", "tracking", "off"),
		makeCmd("CLIENT", "TRACKING", "on", "bcast"),
		makeCmd("Client", "Tracking"),
		makeCmd([]byte("client"), []byte("tracking"), "off"), // raw []byte args
		makeCmd("client", &tracking, "off"),                  // proto.Writer dereferences *string
	}
	for _, cmd := range matching {
		if !isClientTrackingCmd(cmd) {
			t.Errorf("expected %v to match CLIENT TRACKING", cmd.Args())
		}
	}
	nonMatching := []Cmder{
		makeCmd("client", "trackinginfo"),
		makeCmd("client", "info"),
		makeCmd("client", "kill", "id", "1"),
		makeCmd("get", "tracking"),
		makeCmd("client"),
	}
	for _, cmd := range nonMatching {
		if isClientTrackingCmd(cmd) {
			t.Errorf("expected %v NOT to match CLIENT TRACKING", cmd.Args())
		}
	}
}

// TestClientTrackingRejectedWithCSC: on a client with the built-in cache
// configured, CLIENT TRACKING must be rejected before it reaches a connection —
// it would flip an arbitrary pool conn's tracking state and leave it filling
// the cache with entries the server never invalidates. The guard fires without
// dialing, so no server is needed.
func TestClientTrackingRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.ClientTrackingOff(ctx).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}
	if err := c.ClientTrackingOn(ctx, nil).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}
	// The raw escape hatch is caught too: the guard matches leading args.
	// (Non-tracking CLIENT subcommands are covered by TestIsClientTrackingCmd's
	// non-matching cases — probing one here would dial for seconds.)
	if err := c.Do(ctx, "client", "tracking", "off").Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("raw Do(client tracking off) must be rejected with CSC enabled, got %v", err)
	}
	tracking := "tracking"
	if err := c.Do(ctx, "client", &tracking, "off").Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("pointer-encoded CLIENT TRACKING must be rejected with CSC enabled, got %v", err)
	}
	if err := c.Do(ctx, "client", cscWireSmuggler{wire: "tracking", str: "info"}, "off").Err(); !errors.Is(err, errUnverifiableCommandWithCSC) {
		t.Fatalf("marshaled CLIENT TRACKING subcommand must fail closed, got %v", err)
	}
	if err := c.Do(ctx, cscWireSmuggler{wire: "select", str: "get"}, 1).Err(); !errors.Is(err, errUnverifiableCommandWithCSC) {
		t.Fatalf("marshaled SELECT command token must fail closed, got %v", err)
	}
}

// TestClientTrackingRejectedWithCSC_Pipeline: pipelines bypass process(), so
// generalProcessPipeline mirrors the guard — a CLIENT TRACKING frame inside a
// Pipeline or TxPipeline must be rejected on a CSC client too.
func TestClientTrackingRejectedWithCSC_Pipeline(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOff(ctx)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("Pipelined ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}

	_, err = c.TxPipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOn(ctx, nil)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("TxPipelined ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}

	_, err = c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, cscWireSmuggler{wire: "client", str: "get"}, "tracking", "off")
		return nil
	})
	if !errors.Is(err, errUnverifiableCommandWithCSC) {
		t.Fatalf("pipelined marshaled CLIENT TRACKING must fail closed, got %v", err)
	}
}

func TestCSCDisablesWhenHELLO3FallsBackToRESP2(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var getCalls atomic.Int32
	var trackingCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "-ERR unknown command 'hello'\r\n"
				case "get":
					getCalls.Add(1)
					return "$-1\r\n"
				case "client":
					trackingCalls.Add(1)
					return "+OK\r\n"
				default:
					return "+OK\r\n"
				}
			})
		}
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if err := client.Get(context.Background(), "missing").Err(); err != Nil {
			t.Fatalf("GET %d: got %v, want redis.Nil", i+1, err)
		}
	}
	if client.cscActive == nil || client.cscActive.Load() {
		t.Fatal("CSC must be disabled after HELLO 3 falls back to RESP2")
	}
	if got := trackingCalls.Load(); got != 0 {
		t.Fatalf("server received %d CLIENT TRACKING commands after RESP2 fallback, want 0", got)
	}
	if got := getCalls.Load(); got != 2 {
		t.Fatalf("server received %d GETs, want 2 (RESP2 fallback must bypass CSC)", got)
	}
}

func TestCSCDisablesWhenClientTrackingIsRejected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var acceptCalls atomic.Int32
	var getCalls atomic.Int32
	var trackingCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			acceptCalls.Add(1)
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "%0\r\n"
				case "client":
					trackingCalls.Add(1)
					return "-ERR client tracking is disabled\r\n"
				case "get":
					getCalls.Add(1)
					return "$1\r\nv\r\n"
				default:
					return "+OK\r\n"
				}
			})
		}
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if got, err := client.Get(context.Background(), "key").Result(); err != nil || got != "v" {
			t.Fatalf("GET %d: got value %q, error %v; want value %q", i+1, got, err, "v")
		}
	}
	if client.cscActive == nil || client.cscActive.Load() {
		t.Fatal("CSC must be disabled after CLIENT TRACKING is rejected")
	}
	if got := trackingCalls.Load(); got != 1 {
		t.Fatalf("server received %d CLIENT TRACKING commands, want 1", got)
	}
	if got := getCalls.Load(); got != 2 {
		t.Fatalf("server received %d GETs, want 2 (tracking rejection must bypass CSC)", got)
	}
	if got := acceptCalls.Load(); got != 1 {
		t.Fatalf("server accepted %d connections, want 1 (tracking rejection must not discard a usable connection)", got)
	}
}

func TestCSCDoesNotEnableTrackingOnPubSubConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var trackingCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "%0\r\n"
				case "client":
					trackingCalls.Add(1)
				}
				return "+OK\r\n"
			})
		}
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	pubsub := client.pubSub()
	cn, err := pubsub.newConn(context.Background(), ln.Addr().String(), nil)
	if err != nil {
		t.Fatalf("create Pub/Sub connection: %v", err)
	}
	t.Cleanup(func() {
		client.pubSubPool.UntrackConn(cn)
		_ = cn.Close()
	})
	if !cn.IsPubSub() {
		t.Fatal("test connection is not marked as Pub/Sub")
	}
	if got := trackingCalls.Load(); got != 0 {
		t.Fatalf("Pub/Sub initialization sent %d CLIENT commands, want 0", got)
	}
}

func TestSelectRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Do(ctx, "select", 1).Err(); !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("raw SELECT must be rejected with CSC enabled, got %v", err)
	}

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "select", 1)
		return nil
	})
	if !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("pipelined SELECT must be rejected with CSC enabled, got %v", err)
	}
}

func TestConnectionStateCommandsRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: every guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	tests := []struct {
		name string
		args []interface{}
		want error
	}{
		{"AUTH", []interface{}{"auth", "password"}, errAuthWithCSC},
		{"HELLO 2", []interface{}{"hello", 2}, errHelloWithCSC},
		{"RESET", []interface{}{"reset"}, errResetWithCSC},
		{"SUBSCRIBE", []interface{}{"subscribe", "channel"}, errSubscribeWithCSC},
		{"PSUBSCRIBE", []interface{}{"psubscribe", "channel:*"}, errSubscribeWithCSC},
		{"SSUBSCRIBE", []interface{}{"ssubscribe", "channel"}, errSubscribeWithCSC},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.Do(ctx, tc.args...).Err(); !errors.Is(err, tc.want) {
				t.Fatalf("%v must be rejected with CSC enabled, got %v", tc.args, err)
			}
		})
	}

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "hello", 2)
		return nil
	})
	if !errors.Is(err, errHelloWithCSC) {
		t.Fatalf("pipelined HELLO 2 must be rejected with CSC enabled, got %v", err)
	}

	_, err = c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "subscribe", "channel")
		return nil
	})
	if !errors.Is(err, errSubscribeWithCSC) {
		t.Fatalf("pipelined SUBSCRIBE must be rejected with CSC enabled, got %v", err)
	}

	// Bare HELLO only reports connection properties and does not change
	// protocol, authentication, or tracking state.
	if err := c.cscCommandError(makeCmd("hello")); err != nil {
		t.Fatalf("bare HELLO must remain allowed, got %v", err)
	}
}

// TestOnConnectUsesCSCStateGuard verifies that initConn's exemption ends after
// the library's own CLIENT TRACKING command. OnConnect is user code and must
// not be able to mutate a tracked pool connection.
func TestOnConnectUsesCSCStateGuard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(netConn net.Conn) {
				defer netConn.Close()
				scanner := bufio.NewScanner(netConn)
				command := 0
				for scanner.Scan() {
					if !strings.HasPrefix(scanner.Text(), "*") {
						continue
					}
					command++
					if command == 1 {
						// HELLO 3 returns an empty RESP3 map.
						_, _ = netConn.Write([]byte("%0\r\n"))
					} else {
						_, _ = netConn.Write([]byte("+OK\r\n"))
					}
				}
			}(netConn)
		}
	}()

	c := NewClient(&Options{
		Addr:                  ln.Addr().String(),
		Protocol:              3,
		MaxRetries:            -1,
		DisableIdentity:       true,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		OnConnect: func(ctx context.Context, cn *Conn) error {
			return cn.Select(ctx, 1).Err()
		},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Ping(context.Background()).Err(); !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("OnConnect SELECT must be rejected after init's exemption ends, got %v", err)
	}
}

// TestClientTrackingAllowedWithoutCSC: without the built-in cache the guard
// predicate is off entirely (asserted directly — a live dial would prove
// nothing more and costs seconds against an unreachable address).
func TestClientTrackingAllowedWithoutCSC(t *testing.T) {
	c := NewClient(&Options{Addr: "localhost:1", Protocol: 3})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.baseClient.cscCommandError(
		NewCmd(context.Background(), "client", "tracking", "on"),
	); err != nil {
		t.Fatalf("a client without CSC rejected CLIENT TRACKING: %v", err)
	}
}

type recordingCache struct {
	Cache
	owner        Cache
	fulfillCalls int
}

func (c *recordingCache) FulfillOwned(
	cacheKey string,
	token, ownerConnID uint64,
	value []byte,
) bool {
	c.fulfillCalls++
	return c.owner.FulfillOwned(cacheKey, token, ownerConnID, value)
}

func (c *recordingCache) EvictByConn(connID uint64) int {
	return c.owner.EvictByConn(connID)
}

func TestLocalCache_FulfillOwned_EvictByConn(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	var owner Cache = cache

	// Two entries owned by conn 1, one by conn 2.
	for _, kv := range []struct {
		key    string
		connID uint64
	}{{"get:a", 1}, {"get:b", 1}, {"get:c", 2}} {
		tok, sf := cache.Reserve(kv.key, []string{kv.key})
		if !sf {
			t.Fatalf("Reserve(%s) should fetch", kv.key)
		}
		if !owner.FulfillOwned(kv.key, tok, kv.connID, []byte("v")) {
			t.Fatalf("FulfillOwned(%s) failed", kv.key)
		}
	}

	if n := owner.EvictByConn(1); n != 2 {
		t.Fatalf("EvictByConn(1) removed %d, want 2", n)
	}
	if _, ok := cache.Get(context.Background(), "get:a"); ok {
		t.Fatal("conn-1 entry a should be evicted")
	}
	if _, ok := cache.Get(context.Background(), "get:b"); ok {
		t.Fatal("conn-1 entry b should be evicted")
	}
	if _, ok := cache.Get(context.Background(), "get:c"); !ok {
		t.Fatal("conn-2 entry c must survive")
	}
	// Idempotent: evicting again removes nothing.
	if n := owner.EvictByConn(1); n != 0 {
		t.Fatalf("second EvictByConn(1) removed %d, want 0", n)
	}
}

func TestLocalCache_OwnerIndexCleanedOnInvalidation(t *testing.T) {
	// The owning-conn index must be cleaned when an entry is removed for other
	// reasons (invalidation, LRU) so a later EvictByConn can't touch a re-used
	// cache key and the index cannot leak.
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := Cache(cache)
	lc := cache

	tok, _ := cache.Reserve("get:k", []string{"rk"})
	if !owner.FulfillOwned("get:k", tok, 7, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	// Invalidate via the redis key; the owner index for conn 7 must be gone.
	if n := cache.DeleteByRedisKey("rk"); n != 1 {
		t.Fatalf("DeleteByRedisKey removed %d, want 1", n)
	}
	shard := lc.shardFor("get:k")
	shard.mu.RLock()
	_, present := shard.byConnID[7]
	shard.mu.RUnlock()
	if present {
		t.Fatal("byConnID must be cleaned when the entry is invalidated")
	}
	if n := owner.EvictByConn(7); n != 0 {
		t.Fatalf("EvictByConn(7) after invalidation removed %d, want 0", n)
	}
}

// TestCSCEvictOnRemoveHook: the pool OnRemove hook must evict exactly the
// removed connection's owned entries and leave others intact.
func TestCSCEvictOnRemoveHook(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := Cache(cache)

	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	defer cn.Close()
	id := cn.GetID()

	tok, _ := cache.Reserve("get:owned", []string{"owned"})
	if !owner.FulfillOwned("get:owned", tok, id, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	tok2, _ := cache.Reserve("get:other", []string{"other"})
	if !owner.FulfillOwned("get:other", tok2, id+1000, []byte("v")) {
		t.Fatal("FulfillOwned (other conn) failed")
	}

	hook := &cscEvictOnRemoveHook{evictor: owner}
	hook.OnRemove(context.Background(), cn, nil)

	if _, ok := cache.Get(context.Background(), "get:owned"); ok {
		t.Fatal("removed conn's entry must be evicted by OnRemove")
	}
	if _, ok := cache.Get(context.Background(), "get:other"); !ok {
		t.Fatal("another conn's entry must survive OnRemove")
	}
}

// TestCSCConnCloseHook_EvictsOnAnyClose: the per-conn onCscClose hook installed
// by initConn must evict a conn's owned entries when the conn is closed for ANY
// reason — including ConnMaxLifetime / idle-timeout retirement via
// ConnPool.CloseConn, which bypasses the OnRemove pool hook. Without it the
// server drops the conn's tracking table on close while its cached entries
// linger uninvalidated (Window-2 staleness on normal connection retirement).
func TestCSCConnCloseHook_EvictsOnAnyClose(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := Cache(cache)
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache}

	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	id := cn.GetID()

	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !owner.FulfillOwned("get:k", tok, id, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	if cache.Len() != 1 {
		t.Fatalf("setup: want 1 entry, got %d", cache.Len())
	}

	// Install the hook exactly as initConn does, then close the conn directly:
	// no pool OnRemove fires, modelling the CloseConn/ConnMaxLifetime path.
	c.cscInstallConnCloseHook(cn)
	if err := cn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("conn close must evict the conn's owned entries")
	}
}

// TestCSCEvictOwnedEntries_UsesSharedHookWhenCscNil: the handoff/reinit eviction
// path must reach the parent's shared cache through the carried eviction hook
// even when the client's own csc is nil (the Client.Conn / Tx shape), and must
// NOT record the recently-removed ring (the same conn keeps serving on the fresh
// socket, so post-handoff fulfills are legitimate).
func TestCSCEvictOwnedEntries_UsesSharedHookWhenCscNil(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	derived := &baseClient{opt: &Options{Protocol: 3}, csc: nil, cscPoolHook: hook}

	const connID = uint64(9)
	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", tok, connID, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}

	derived.cscEvictOwnedEntries(connID)
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("handoff eviction must evict via the shared hook when csc is nil")
	}
	// The conn keeps serving: fetches capturing the post-eviction generation
	// must remain valid (the eviction bumps, it does not tombstone).
	if got := hook.initGenOf(connID); got == 0 {
		t.Fatal("handoff must leave a live generation for the replacement socket")
	}
}

func TestCSCHookInvalidateAllCoverage(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	owner := Cache(cache)
	hook := &cscEvictOnRemoveHook{
		evictor: owner,
		initGen: make(map[uint64]uint64),
	}

	const (
		conn1 = uint64(11)
		conn2 = uint64(22)
	)
	hook.bumpInitGen(conn1)
	hook.bumpInitGen(conn2)
	oldGen := hook.initGenOf(conn1)

	for _, entry := range []struct {
		key    string
		connID uint64
	}{
		{"get:one", conn1},
		{"get:two", conn2},
	} {
		token, _ := cache.Reserve(entry.key, []string{entry.key})
		if !owner.FulfillOwned(entry.key, token, entry.connID, []byte("v")) {
			t.Fatalf("FulfillOwned(%q) failed", entry.key)
		}
	}

	lateToken, _ := cache.Reserve("get:late", []string{"late"})
	hook.invalidateAllCoverage()

	if cache.Len() != 1 {
		t.Fatalf("valid entries from stopped coverage must be evicted; got len=%d", cache.Len())
	}
	if hook.fulfillOwnedIfCovered("get:late", lateToken, conn1, oldGen, []byte("stale")) {
		t.Fatal("an in-flight fetch from revoked coverage must not publish")
	}
	cache.Cancel("get:late", lateToken)
}

func TestFulfillCached_RejectsInactiveCSC(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	const connID = uint64(33)
	hook.bumpInitGen(connID)

	active := &atomic.Bool{}
	active.Store(false)
	c := &baseClient{
		opt:         &Options{Protocol: 3},
		csc:         cache,
		cscPoolHook: hook,
		cscActive:   active,
	}
	token, _ := cache.Reserve("get:k", []string{"k"})
	if c.fulfillCached("get:k", token, &cscFetchCapture{
		raw:     []byte("$1\r\nv\r\n"),
		connID:  connID,
		initGen: hook.initGenOf(connID),
	}, defaultCommandMetadataView()) {
		t.Fatal("a fetch must not publish after its CSC drainer stops")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("inactive CSC left a cached entry behind")
	}
}

// TestNewTx_CarriesSharedEvictionHook: a Tx must carry the parent's shared
// eviction hook (so close/reinit hooks installed on a Watch-initialized
// connection evict from the parent cache) but must not serve cached reads itself.
func TestNewTx_CarriesSharedEvictionHook(t *testing.T) {
	client := NewClient(&Options{
		Addr:                  "127.0.0.1:0", // never dialed in this unit test
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	defer client.Close()
	if client.cscPoolHook == nil {
		t.Fatal("precondition: parent must have a shared eviction hook")
	}

	tx := client.newTx()
	defer func() { _ = tx.Close(context.Background()) }()
	if tx.cscPoolHook != client.cscPoolHook {
		t.Fatal("newTx must carry the parent's shared eviction hook")
	}
	if tx.csc != nil {
		t.Fatal("Tx must not serve cached reads (csc must stay nil)")
	}
}

// TestCSCConnCloseHook_NoOrphanWhenCloseRacesFulfill: the close-hook path
// (cscOnConnClose, used by ConnPool.CloseConn / ConnMaxLifetime retirement) must
// record the recently-removed ring BEFORE evicting, so a fulfill that lands after
// the conn closed (reply released → conn closed → fulfillCached) does not leave an
// orphaned entry with no invalidation coverage.
func TestCSCConnCloseHook_NoOrphanWhenCloseRacesFulfill(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(7)
	// The conn served (first init bumped its generation, captured at reply
	// time), then closes before the entry exists (fulfill has not run yet).
	hook.bumpInitGen(connID)
	gen := hook.initGenOf(connID)
	c.cscOnConnClose(connID)

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	// Fulfill attributes to the just-closed conn; the generation guard must
	// drop it (the close deleted the conn's entry, so it reads 0 != gen).
	c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen}, defaultCommandMetadataView())
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("entry owned by a conn closed before fulfill must not survive")
	}
}

// TestFulfillCached_RaceWithConnRemoval: if the owning connection is removed
// around the fulfill (its OnRemove eviction ran before the entry existed),
// fulfillCached must drop the orphaned entry rather than leave it resident with
// no invalidation coverage (the Window-2 TOCTOU).
func TestFulfillCached_RaceWithConnRemoval(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	// Model "conn 42 served, then was just removed" (no need for a real conn
	// whose GetID == 42): first init bumped its generation, the fetch captured
	// it, and the OnRemove eviction ran before the entry existed.
	hook.bumpInitGen(connID)
	gen := hook.initGenOf(connID)
	hook.markRemoved(connID)

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	if c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen}, defaultCommandMetadataView()) {
		t.Fatal("coverage loss must reject fulfillment before publication")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("entry owned by a just-removed conn must not remain resident")
	}
}

// TestFulfillCached_CoverageLossSkipsPublication verifies the generation guard
// runs before FulfillOwned changes the placeholder to Valid. This matters to
// concurrent Get waiters: publishing and deleting immediately afterward still
// leaves a window in which a waiter can return an uncovered value.
func TestFulfillCached_CoverageLossSkipsPublication(t *testing.T) {
	base := NewLocalCache(CacheConfig{MaxEntries: 64})
	cache := &recordingCache{
		Cache: base,
		owner: base,
	}
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	hook.bumpInitGen(connID)
	gen := hook.initGenOf(connID)

	token, shouldFetch := cache.Reserve("get:k", []string{"k"})
	if !shouldFetch {
		t.Fatal("Reserve should fetch")
	}
	local := base
	shard := local.shardFor("get:k")
	shard.mu.RLock()
	waitCh := shard.entries["get:k"].waitCh
	shard.mu.RUnlock()

	// Removal wins before fulfillment. The cache method must never be called:
	// fulfillCached cancels the placeholder, waking waiters to observe a miss.
	hook.markRemoved(connID)
	if c.fulfillCached("get:k", token, &cscFetchCapture{
		raw:     []byte("v"),
		connID:  connID,
		initGen: gen,
	}, defaultCommandMetadataView()) {
		t.Fatal("a reply without invalidation coverage must not be published")
	}
	if cache.fulfillCalls != 0 {
		t.Fatalf("FulfillOwned was called %d times after coverage loss; want 0", cache.fulfillCalls)
	}
	select {
	case <-waitCh:
	default:
		t.Fatal("coverage loss must cancel the placeholder and wake waiters")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("a waiter must observe a miss, never a transient uncovered value")
	}
}

// TestInitGenLifecycle pins the map bounding that the coverage guard relies
// on: first init bumps to >=1, removal (markRemoved) and failed init
// (forgetConn) delete the entry — so a served conn's captured generation
// always mismatches after its conn goes away, and the map stays bounded to
// live conns.
func TestInitGenLifecycle(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{evictor: cache}

	const connID = uint64(5)
	hook.bumpInitGen(connID)
	if got := hook.initGenOf(connID); got != 1 {
		t.Fatalf("first init must bump the generation to 1, got %d", got)
	}
	hook.markRemoved(connID)
	if got := hook.initGenOf(connID); got != 0 {
		t.Fatalf("markRemoved must delete the generation entry, got %d", got)
	}

	// Failed init: forgetConn drops the entry the failed init's bump created.
	hook.bumpInitGen(connID)
	hook.forgetConn(connID)
	if got := hook.initGenOf(connID); got != 0 {
		t.Fatalf("forgetConn must delete the generation entry, got %d", got)
	}
}

// TestFulfillCached_RaceWithHandoffReinit: a maintenance handoff replaces a
// conn's socket (and its server-side tracking) while the conn id keeps serving.
// If the re-init eviction runs between a fetch's reply (read on the OLD socket)
// and its fulfill, the published entry has no invalidation coverage — the
// init-generation check must drop it. The removed-ring cannot cover this: the
// conn was never removed.
func TestFulfillCached_RaceWithHandoffReinit(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}

	// Reply read on the pre-handoff socket: generation captured while the conn
	// was still held (what _process does).
	gen := c.cscConnInitGen(connID)

	// The handoff worker re-inits the conn after it was released but before
	// fulfillCached runs: bumps the generation, then evicts (a no-op here — the
	// entry does not exist yet; only the placeholder does).
	c.cscEvictOwnedEntries(connID)

	c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen}, defaultCommandMetadataView())
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("entry fetched on a socket replaced before fulfill must not remain resident")
	}
}

func TestFulfillCached_HandoffBumpsCoverageBeforeSocketSwap(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	oldServer, oldClient := net.Pipe()
	defer oldServer.Close()
	defer oldClient.Close()
	newServer, newClient := net.Pipe()
	defer newServer.Close()
	defer newClient.Close()
	cn := pool.NewConn(oldClient)
	connID := cn.GetID()

	// Model the first successful tracked initialization and capture a reply
	// from that socket.
	hook.bumpInitGen(connID)
	c.cscInstallConnReinitHook(cn)
	token, _ := cache.Reserve("get:k", []string{"k"})
	oldGen := c.cscConnInitGen(connID)

	cn.SetInitConnFunc(func(context.Context, *pool.Conn) error {
		cn.GetStateMachine().Transition(pool.StateIdle)
		return nil
	})
	if err := cn.SetNetConnAndInitConn(context.Background(), newClient); err != nil {
		t.Fatalf("replace socket: %v", err)
	}
	if got := c.cscConnInitGen(connID); got == oldGen {
		t.Fatal("socket replacement did not change CSC coverage generation")
	}

	if c.fulfillCached("get:k", token, &cscFetchCapture{
		raw:     []byte("v"),
		connID:  connID,
		initGen: oldGen,
	}, defaultCommandMetadataView()) {
		t.Fatal("an old-socket reply must be rejected after handoff")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("old-socket reply became visible after handoff")
	}
}

// TestFulfillCached_PostHandoffFetchIsCached: after a handoff, fetches served by
// the conn's NEW socket capture the post-bump generation and must be cached
// normally — the generation check only drops entries from the replaced socket.
func TestFulfillCached_PostHandoffFetchIsCached(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	// Handoff completed; the conn keeps serving on its new socket.
	c.cscEvictOwnedEntries(connID)

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	gen := c.cscConnInitGen(connID) // captured at reply time, post-bump

	if !c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen}, defaultCommandMetadataView()) {
		t.Fatal("post-handoff fetch on the new socket should be cached")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); !ok {
		t.Fatal("post-handoff entry must remain resident")
	}
}

// TestFulfillCached_NoHookUsesUnownedFulfill: without an evict-on-remove hook,
// fulfillCached publishes with ownerConnID zero and still caches the value.
func TestFulfillCached_NoHookUsesUnownedFulfill(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache} // cscPoolHook nil

	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: 7}, defaultCommandMetadataView()) {
		t.Fatal("fulfillCached should store an unowned value when no hook is present")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); !ok {
		t.Fatal("value should be cached")
	}
}

// TestCSCStrategyValidation_ClampsUnknown: an out-of-range strategy must not
// thread the per-strategy gates into "tracking on, nothing draining".
func TestCSCStrategyValidation_ClampsUnknown(t *testing.T) {
	opt := &Options{ClientSideCacheStrategy: CSCStrategy(99)}
	opt.init()
	if opt.ClientSideCacheStrategy != CSCStrategySharedTracking {
		t.Fatalf("unknown strategy must clamp to SharedTracking, got %d", opt.ClientSideCacheStrategy)
	}
}

func TestCSCNamespaceUsesACLUsername(t *testing.T) {
	newClient := func(username, password string) *Client {
		return NewClient(&Options{
			Addr:                  "127.0.0.1:0",
			Protocol:              3,
			Username:              username,
			Password:              password,
			ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		})
	}

	oldPassword := newClient("alice", "old-secret")
	newPassword := newClient("alice", "new-secret")
	otherUser := newClient("bob", "new-secret")
	t.Cleanup(func() {
		_ = oldPassword.Close()
		_ = newPassword.Close()
		_ = otherUser.Close()
	})

	if oldPassword.cscKeyPrefix != newPassword.cscKeyPrefix {
		t.Fatal("password rotation changed the cache namespace for the same ACL user")
	}
	if oldPassword.cscKeyPrefix == otherUser.cscKeyPrefix {
		t.Fatal("different ACL users must have different cache namespaces")
	}
}

// TestBaseClientClone_CarriesCSCPointers: clone() must carry the shared cache and
// the eviction-hook handle a clone reads for attribution, but not the owner-only
// lifecycle fields.
func TestBaseClientClone_CarriesCSCPointers(t *testing.T) {
	c := &baseClient{
		opt:           &Options{},
		csc:           NewLocalCache(CacheConfig{MaxEntries: 16}),
		staticCmdMeta: defaultCommandMetadataView(),
	}
	// cscPoolHook IS carried: a clone reads it to attribute fetches to the
	// shared eviction hook. The owner-only fields are NOT carried, so a derived
	// client's Close can't stop the owner's drainer or flush its cache.
	hook := &cscEvictOnRemoveHook{}
	active := &atomic.Bool{}
	active.Store(true)
	c.cscPoolHook = hook
	c.cscActive = active
	c.cscKeyPrefix = cscNamespacePrefix(0, "user")
	c.cscOwnsCache = true
	c.cscDrainHandle = &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}

	cl := c.clone()
	if cl.csc == nil {
		t.Fatal("clone dropped csc")
	}
	if cl.cscPoolHook != hook {
		t.Fatal("clone must copy cscPoolHook (needed for attribution)")
	}
	if cl.cscActive != active {
		t.Fatal("clone must share the successful CSC attachment signal")
	}
	if cl.cscKeyPrefix != c.cscKeyPrefix {
		t.Fatal("clone must retain the shared cache namespace")
	}
	if cl.staticCmdMeta != c.staticCmdMeta {
		t.Fatal("clone must retain the cached default metadata view")
	}
	if cl.cscOwnsCache {
		t.Fatal("clone must not copy cscOwnsCache (owner-only)")
	}
	if cl.cscDrainHandle != nil {
		t.Fatal("clone must not copy cscDrainHandle (owner-only)")
	}
}

// TestRegisterInvalidateHandler_IdempotentForSameCache: a derived client
// sharing the parent's push processor must be able to re-attach the same
// cache (Client.Conn), while a different cache must still be refused.
func TestRegisterInvalidateHandler_IdempotentForSameCache(t *testing.T) {
	proc := push.NewProcessor()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	keyPrefix := cscNamespacePrefix(0, "")

	if err := registerInvalidateHandler(proc, cache, keyPrefix); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := registerInvalidateHandler(proc, cache, keyPrefix); err != nil {
		t.Fatalf("re-registration of the same cache+db must succeed, got: %v", err)
	}
	if err := registerInvalidateHandler(proc, NewLocalCache(CacheConfig{MaxEntries: 16}), keyPrefix); err == nil {
		t.Fatal("registering a different cache on the same processor must fail")
	}
}

func TestRegisterInvalidateHandler_ConcurrentSameCache(t *testing.T) {
	proc := push.NewProcessor()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	keyPrefix := cscNamespacePrefix(0, "")

	const clients = 32
	start := make(chan struct{})
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	wg.Add(clients)
	for range clients {
		go func() {
			defer wg.Done()
			<-start
			errs <- registerInvalidateHandler(proc, cache, keyPrefix)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("compatible concurrent registration failed: %v", err)
		}
	}
	if got := boundCache(lookupInvalidateHandler(proc)); got != cache {
		t.Fatal("concurrent registration did not retain the shared cache binding")
	}
}

func TestRegisterInvalidateHandler_NonComparableCacheDoesNotPanic(t *testing.T) {
	proc := push.NewProcessor()
	cache := nonComparableCache{
		Cache:  NewLocalCache(CacheConfig{MaxEntries: 16}),
		marker: []byte("non-comparable"),
	}
	keyPrefix := cscNamespacePrefix(0, "")

	if err := registerInvalidateHandler(proc, cache, keyPrefix); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := registerInvalidateHandler(proc, cache, keyPrefix); !errors.Is(err, errInvalidateHandlerBound) {
		t.Fatalf("second registration: got %v, want errInvalidateHandlerBound", err)
	}
}

// plainPooler is a Pooler with no DrainIdleConns capability.
type plainPooler struct{ pool.Pooler }

// drainOnlyPooler can sweep idle conns but cannot register the lifecycle hook
// that serializes cache publication with connection removal/reinitialization.
type drainOnlyPooler struct{ plainPooler }

func (*drainOnlyPooler) DrainIdleConns(
	context.Context,
	*pool.DrainState,
	func(*pool.Conn) error,
) {
}

// TestAttachCSC_StrategyGates: attachCSC refuses poolers without idle-conn
// draining (a sticky pool serving hits would be unboundedly stale — nothing
// applies invalidations).
func TestAttachCSC_StrategyGates(t *testing.T) {
	ctx := context.Background()

	sticky := &baseClient{
		opt:           &Options{Protocol: 3, ClientSideCacheStrategy: CSCStrategySharedTracking},
		pushProcessor: push.NewProcessor(),
		connPool:      &plainPooler{},
	}
	sticky.attachCSC(ctx, NewLocalCache(CacheConfig{MaxEntries: 16}))
	if sticky.csc != nil {
		t.Fatal("SharedTracking without a drainable pooler must stay uncached")
	}
	if sticky.cscDrainHandle != nil {
		t.Fatal("no drainer may be started when attachCSC refused the pooler")
	}

	hookless := &baseClient{
		opt:           &Options{Protocol: 3, ClientSideCacheStrategy: CSCStrategySharedTracking},
		pushProcessor: push.NewProcessor(),
		connPool:      &drainOnlyPooler{},
	}
	hookless.attachCSC(ctx, NewLocalCache(CacheConfig{MaxEntries: 16}))
	if hookless.csc != nil {
		t.Fatal("SharedTracking without lifecycle hooks must stay uncached")
	}
	if hookless.cscDrainHandle != nil {
		t.Fatal("no drainer may be started when lifecycle-hook registration is unavailable")
	}
}

// TestCSCTrackingRequested: tracking requires a successful, still-active CSC
// attachment. Merely configuring a cache is insufficient because handler
// registration or another attachment gate can fail.
func TestCSCTrackingRequested(t *testing.T) {
	cfg := &ClientSideCacheConfig{MaxEntries: 16}
	active := &atomic.Bool{}
	active.Store(true)
	stopped := &atomic.Bool{}
	cases := []struct {
		name string
		c    *baseClient
		want bool
	}{
		{"successful attachment", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}, cscActive: active}, true},
		{"derived client: csc nil, attachment shared", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}, cscActive: active}, true},
		{"configured but attachment failed", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}}, false},
		{"attachment stopped", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}, cscActive: stopped}, false},
		{"resp2", &baseClient{opt: &Options{Protocol: 2, ClientSideCacheConfig: cfg}, cscActive: active}, false},
		{"non-zero db", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg, DB: 1}, cscActive: active}, false},
	}
	for _, tc := range cases {
		if got := tc.c.cscTrackingRequested(); got != tc.want {
			t.Errorf("%s: cscTrackingRequested() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestCSCTrackingSignalSharedWithStickyClients(t *testing.T) {
	parent := NewClient(&Options{
		Addr:                  "127.0.0.1:0",
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = parent.Close() })

	conn := parent.Conn()
	t.Cleanup(func() { _ = conn.Close() })
	if conn.cscActive != parent.cscActive || !conn.cscTrackingRequested() {
		t.Fatal("Client.Conn must share the parent's active attachment signal")
	}

	tx := parent.newTx()
	t.Cleanup(func() { _ = tx.baseClient.Close() })
	if tx.cscActive != parent.cscActive || !tx.cscTrackingRequested() {
		t.Fatal("Tx must share the parent's active attachment signal")
	}
}

type borrowedConnPool struct {
	pool.Pooler
	cn *pool.Conn
}

func (p *borrowedConnPool) Get(context.Context) (*pool.Conn, error) {
	return p.cn, nil
}

func (*borrowedConnPool) Put(context.Context, *pool.Conn) {}

func TestStickyClaimRevokesParentCacheCoverage(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})
	cn := pool.NewConn(client)

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	hook.bumpInitGen(cn.GetID())
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, cn.GetID(), []byte("v")) {
		t.Fatal("failed to seed parent-owned cache entry")
	}

	base := &baseClient{
		connPool:    &borrowedConnPool{cn: cn},
		cscPoolHook: hook,
	}
	sticky := base.newStickyConnPool()
	claimed, err := sticky.Get(context.Background())
	if err != nil {
		t.Fatalf("sticky Get: %v", err)
	}
	if claimed != cn {
		t.Fatal("sticky pool returned an unexpected connection")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("sticky claim left parent-cache entries without drainer coverage")
	}
	if got := hook.initGenOf(cn.GetID()); got != 2 {
		t.Fatalf("sticky claim generation: got %d, want 2", got)
	}
	sticky.Put(context.Background(), claimed)
	if err := sticky.Close(); err != nil {
		t.Fatalf("sticky Close: %v", err)
	}
}

// recordingHookPool is a poolHookSupport that counts RemovePoolHook calls.
type recordingHookPool struct {
	pool.Pooler
	removed int
}

func (p *recordingHookPool) AddPoolHook(pool.PoolHook)    {}
func (p *recordingHookPool) RemovePoolHook(pool.PoolHook) { p.removed++ }
func (p *recordingHookPool) SupportsPoolHooks() bool      { return true }
func (p *recordingHookPool) DrainIdleConns(
	context.Context, *pool.DrainState, func(*pool.Conn) error,
) {
}

// TestClone_SharesHookButOnlyOwnerDeregisters: a clone copies cscPoolHook (so it
// can attribute fetches to the shared eviction hook) but must not deregister it
// on Close; only the owner (the client holding the drain handle) does.
func TestClone_SharesHookButOnlyOwnerDeregisters(t *testing.T) {
	rp := &recordingHookPool{}
	owner := &baseClient{
		opt:         &Options{},
		connPool:    rp,
		cscPoolHook: &cscEvictOnRemoveHook{},
	}
	owner.startBackgroundDrainer()
	if owner.cscDrainHandle == nil {
		t.Fatal("owner did not start its drainer")
	}

	cl := owner.clone()
	if cl.cscPoolHook != owner.cscPoolHook {
		t.Fatal("clone should share the eviction hook for attribution")
	}
	if cl.cscDrainHandle != nil {
		t.Fatal("clone must not own the drain handle")
	}

	// Clone Close: no drain handle -> early return, must not touch the hook.
	cl.stopBackgroundDrainer()
	if rp.removed != 0 {
		t.Fatalf("clone must not deregister the shared hook, got %d removals", rp.removed)
	}

	// Owner Close: deregisters the hook exactly once.
	owner.stopBackgroundDrainer()
	if rp.removed != 1 {
		t.Fatalf("owner must deregister the hook once, got %d", rp.removed)
	}
}

// fakeTimeout is a net.Error reporting a timeout.
type fakeTimeout struct{}

func (fakeTimeout) Error() string   { return "i/o timeout" }
func (fakeTimeout) Timeout() bool   { return true }
func (fakeTimeout) Temporary() bool { return true }

// TestDrainErrorClassificationContract pins the drain-path classification for
// errors surfaced after reply consumption starts. At that point a timeout means
// the reader may be desynchronized and the connection must be removed.
func TestDrainErrorClassificationContract(t *testing.T) {
	const addr = "localhost:6379"

	timeoutErr := &net.OpError{Op: "read", Net: "tcp", Err: fakeTimeout{}}
	if !isBadConn(timeoutErr, false, addr) {
		t.Error("net i/o timeout must be fatal on the drain path (conn removed)")
	}
	if !isBadConn(io.EOF, false, addr) {
		t.Error("io.EOF must be fatal (conn removed)")
	}
	if !isBadConn(context.DeadlineExceeded, false, addr) {
		t.Error("context.DeadlineExceeded must be fatal")
	}
}

// invalidateFrame builds a RESP3 `>` push frame: ["invalidate", [key]].
func invalidateFrame(key string) []byte {
	return []byte(fmt.Sprintf(">2\r\n$10\r\ninvalidate\r\n*1\r\n$%d\r\n%s\r\n", len(key), key))
}

type recordingHandler struct {
	mu sync.Mutex
	n  int
}

func (h *recordingHandler) HandlePushNotification(_ context.Context, _ push.NotificationHandlerContext, _ []interface{}) error {
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

type closingHandler struct {
	closeReturned chan error
}

func (h *closingHandler) HandlePushNotification(
	_ context.Context, handlerCtx push.NotificationHandlerContext, _ []interface{},
) error {
	closer, ok := handlerCtx.Client.(interface{ Close() error })
	if !ok {
		return errors.New("handler client does not implement Close")
	}
	h.closeReturned <- closer.Close()
	return nil
}

func newIdleTCPConnPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case server = <-accepted:
	case <-time.After(time.Second):
		_ = client.Close()
		t.Fatal("accept timed out")
	}
	return server, client
}

// newReaderBufferedPushConn returns a conn with frame buffered in proto.Reader
// but no socket-visible data — the coalesced-push case (an invalidate left in
// cn.rd after a prior reply).
func newReaderBufferedPushConn(t *testing.T, frame []byte) (*pool.Conn, func()) {
	t.Helper()
	server, client := net.Pipe()
	cn := pool.NewConn(client)
	go func() { _, _ = server.Write(frame) }()
	// PeekReplyType fills the bufio buffer without consuming the frame.
	if err := cn.WithReader(context.Background(), time.Second, func(rd *proto.Reader) error {
		_, err := rd.PeekReplyType()
		return err
	}); err != nil {
		_ = server.Close()
		_ = client.Close()
		t.Fatalf("priming reader buffer: %v", err)
	}
	return cn, func() { _ = server.Close(); _ = client.Close() }
}

type releaseRecordingPool struct {
	pool.Pooler
	puts    int
	removes int
}

func (p *releaseRecordingPool) Put(context.Context, *pool.Conn) {
	p.puts++
}

func (p *releaseRecordingPool) Remove(context.Context, *pool.Conn, error) {
	p.removes++
}

func TestReleaseConnRemovesConnectionAfterPartialPushRead(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	partial := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfo")
	if _, err := server.Write(partial); err != nil {
		t.Fatalf("write partial push: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("partial push never became readable")
	}

	cp := &releaseRecordingPool{}
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		connPool:      cp,
		pushProcessor: push.NewProcessor(),
	}
	c.releaseConn(context.Background(), cn, nil)

	// The invariant is no PARTIALLY-CONSUMED conn is ever re-pooled. On a loaded
	// runner the drain's short probe deadline can expire before consuming any
	// byte — a benign timeout: nothing was read, the frame is intact in the
	// socket, and re-pooling is safe (the next drain consumes it whole). Only a
	// Put after bytes moved into the reader is the desync bug. An empty reader
	// buffer alone does NOT prove zero consumption (a partial parse can eat
	// every available byte and still leave the buffer empty), so before
	// skipping, prove the stream is intact: complete the frame and require the
	// whole push to parse. A parse failure means a desynced conn was re-pooled
	// — exactly the regression this test pins.
	if cp.puts == 1 && cp.removes == 0 && !cn.HasBufferedData() {
		if _, err := server.Write([]byte("o\r\n")); err != nil {
			t.Fatalf("completing push frame: %v", err)
		}
		if err := cn.WithReader(context.Background(), 2*time.Second, func(rd *proto.Reader) error {
			_, err := rd.ReadReply()
			return err
		}); err != nil {
			t.Fatalf("re-pooled conn is desynced: completed push failed to parse: %v", err)
		}
		t.Skip("probe timed out before consuming anything; conn re-pooled intact (whole-frame parse verified) — mid-frame path not exercised this run")
	}
	if cp.removes != 1 || cp.puts != 0 {
		t.Fatalf("partial push read must remove, not re-pool, the connection: removes=%d puts=%d",
			cp.removes, cp.puts)
	}
}

// TestCSCMissReadDrainsSocketPendingPushBeforeReply pins Finding A: a
// reply-expected CSC reader must block past a push that is still ON THE SOCKET
// (not yet buffered) ahead of the command reply. The old Buffered drain stopped
// the instant the reader buffer emptied, so a second invalidation arriving after
// the first was consumed would be read by ReadRawReply as the command's reply and
// cached under the wrong key. The blocking drain (drainPushFrames(..., true))
// keeps skipping pushes until a non-push frame is next.
func TestCSCMissReadDrainsSocketPendingPushBeforeReply(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		pushProcessor: push.NewProcessor(),
	}

	pushX := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nx\r\n")
	pushY := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\ny\r\n")
	reply := []byte("$5\r\nhello\r\n")

	type res struct {
		raw []byte
		err error
	}
	done := make(chan res, 1)
	go func() {
		var raw []byte
		err := cn.WithReader(context.Background(), 5*time.Second, func(rd *proto.Reader) error {
			if e := c.drainPushFrames(context.Background(), cn, rd, true); e != nil {
				return e
			}
			var e error
			raw, e = rd.ReadRawReply()
			return e
		})
		done <- res{raw, err}
	}()

	// First push, then a pause long enough for the reader to consume it and EMPTY
	// its buffer while blocked on the next frame — the exact window the Buffered
	// drain used to return in. Then a SECOND push ahead of the real reply.
	if _, err := server.Write(pushX); err != nil {
		t.Fatalf("write first push: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := server.Write(pushY); err != nil {
		t.Fatalf("write second push: %v", err)
	}
	if _, err := server.Write(reply); err != nil {
		t.Fatalf("write reply: %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("reader failed: %v", r.err)
		}
		if string(r.raw) != string(reply) {
			t.Fatalf("blocking drain must skip both pushes and return the reply; got %q want %q",
				r.raw, reply)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reader did not complete")
	}
}

// bufferedNetConn models a wrapper such as tls.Conn: bytes may already be
// buffered inside the wrapper while NetConn's raw socket is empty.
type bufferedNetConn struct {
	net.Conn
	buffered *bytes.Reader
}

func (c *bufferedNetConn) Read(p []byte) (int, error) {
	if c.buffered.Len() > 0 {
		return c.buffered.Read(p)
	}
	return c.Conn.Read(p)
}

func (c *bufferedNetConn) NetConn() net.Conn {
	return c.Conn
}

func TestDrainPushNotifications_ConsumesWrappedBufferedPush(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris", "illumos":
	default:
		t.Skip("platform has no non-consuming raw-socket probe")
	}

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(&bufferedNetConn{
		Conn:     client,
		buffered: bytes.NewReader(invalidateFrame("foo")),
	})
	cn.MarkCscReadPending()

	processorSucceeded, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("drain wrapped push: %v", err)
	}
	if !processorSucceeded {
		t.Fatal("successful wrapped-buffer processing must reset consecutive failure damping")
	}
	if rec.count() != 1 {
		t.Fatal("push hidden in the wrapper buffer was not consumed")
	}
	if cn.TakeCscReadPending() {
		t.Fatal("wrapped-reader drain request was not consumed")
	}
}

func TestDrainPushNotifications_EmptyWrappedProbeStaysShort(t *testing.T) {
	oldHardReadCap := cscDrainHardReadCap
	cscDrainHardReadCap = 200 * time.Millisecond
	defer func() { cscDrainHardReadCap = oldHardReadCap }()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: push.NewProcessor()}
	cn := pool.NewConn(&bufferedNetConn{
		Conn:     client,
		buffered: bytes.NewReader(nil),
	})
	cn.MarkCscReadPending()

	start := time.Now()
	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("empty wrapped probe: %v", err)
	}
	if processed {
		t.Fatal("empty wrapped probe must not report processor success")
	}
	if elapsed := time.Since(start); elapsed >= 50*time.Millisecond {
		t.Fatalf("empty wrapped probe held the connection for %v", elapsed)
	}
}

// TestDrainPushNotifications_ConsumesReaderBufferedPush is a regression guard: a
// push buffered in proto.Reader (no socket data) must still drain — the gate
// checks HasBufferedData(), not only MaybeHasData().
func TestDrainPushNotifications_ConsumesReaderBufferedPush(t *testing.T) {
	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	if !cn.HasBufferedData() {
		t.Fatal("precondition: frame was not buffered in the reader")
	}

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("drainPushNotifications returned error: %v", err)
	}
	if !processed {
		t.Fatal("drain with buffered data must report processed=true")
	}
	if rec.count() == 0 {
		t.Fatal("reader-buffered invalidate was not consumed/dispatched (gate skipped it)")
	}
}

func TestDrainPushNotifications_AllowsFragmentedFrame(t *testing.T) {
	oldHardReadCap := cscDrainHardReadCap
	cscDrainHardReadCap = 200 * time.Millisecond
	defer func() { cscDrainHardReadCap = oldHardReadCap }()

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(client)

	frame := invalidateFrame("fragmented")
	split := len(frame) - 4
	if _, err := server.Write(frame[:split]); err != nil {
		t.Fatalf("write frame prefix: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("frame prefix never became socket-readable")
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		_, _ = server.Write(frame[split:])
	}()

	start := time.Now()
	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("drain fragmented frame: %v", err)
	}
	if !processed || rec.count() != 1 {
		t.Fatalf("fragmented frame was not processed: processed=%v calls=%d",
			processed, rec.count())
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("drain waited after the fragmented frame completed: %v", elapsed)
	}
}

// erroringProcessor returns err from ProcessPendingNotifications, delegating
// other methods to the embedded Processor. Wrapping the built-in processor
// this way still classifies as CUSTOM in drainPushNotifications (the type
// assertion is exact) — which is the point: a wrapper gives no
// no-bytes-consumed guarantee either.
type erroringProcessor struct {
	*push.Processor
	err error
}

func (p erroringProcessor) ProcessPendingNotifications(_ context.Context, _ push.NotificationHandlerContext, _ *proto.Reader) error {
	return p.err
}

// TestDrainPushNotifications_CustomProcessorErrorIsFatal: a custom processor
// (any non-*push.Processor, including a wrapper around the built-in one) gives
// no guarantee that no bytes were consumed before its error, so the reader may
// be mid-frame — the error must be connection-fatal (non-nil), exactly like
// the built-in processor's mid-frame errors, so the drainer removes the conn
// instead of re-pooling a possibly desynced reader. (This inverts the earlier
// not-fatal behavior, which re-pooled the conn on the same evidence.)
func TestDrainPushNotifications_CustomProcessorErrorIsFatal(t *testing.T) {
	proc := erroringProcessor{push.NewProcessor(), errors.New("semantic boom")}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	if _, err := c.drainPushNotifications(cn); err == nil {
		t.Fatal("custom-processor drain error must be fatal so the conn is removed, got nil")
	}
}

// TestDrainPushNotifications_CustomProcessorSkipsKernelOnlyData pins the spec gate
// (push.md): a custom processor gets work only when real RESP bytes are BUFFERED.
// Here a full frame sits on the socket (CheckForData readable) but nothing is
// buffered yet — the kernel-only case that over TLS can be a control record with no
// RESP bytes. The custom processor must be skipped (not invoked, not fatal); the
// frame is left for a later drain with buffered bytes or the MaxStaleness backstop.
func TestDrainPushNotifications_CustomProcessorSkipsKernelOnlyData(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris", "illumos":
	default:
		t.Skip("platform has no non-consuming raw-socket probe")
	}

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	// A wrapper around the built-in processor is classified CUSTOM; erroringProcessor
	// returns its error WITHOUT reading, so if the gate wrongly invokes it the drain
	// returns a (fatal) non-nil error.
	proc := erroringProcessor{push.NewProcessor(), errors.New("custom must be skipped on kernel-only data")}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(client)

	if _, err := server.Write(invalidateFrame("foo")); err != nil {
		t.Fatalf("write frame: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("frame never became socket-readable")
	}
	if cn.HasBufferedData() {
		t.Fatal("precondition: nothing should be buffered in the reader yet")
	}

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("custom processor was invoked on kernel-only data (must be skipped): %v", err)
	}
	if processed {
		t.Fatal("custom processor must not report success on kernel-only data")
	}
}

// TestDrainPushNotifications_CustomProcessorSkippedOnProbedBytes pins that the
// custom-processor gate keys on bytes ALREADY buffered, not on a byte the probe just
// peeked from an opaque transport. An opaque wrapper hides real invalidate bytes
// from the socket check and requests a drain (MarkCscReadPending); the probe can
// peek a byte into the buffer, but a custom processor must still be skipped this
// pass — handing its blocking loop probed (not pre-buffered) data risks a fatal
// spurious retire (spec: push.md). Skipping is bounded (periodic fallback +
// MaxStaleness). A built-in processor is unaffected.
func TestDrainPushNotifications_CustomProcessorSkippedOnProbedBytes(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	proc := erroringProcessor{push.NewProcessor(), errors.New("custom must not run on probed (not pre-buffered) bytes")}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(&bufferedNetConn{
		Conn:     client,
		buffered: bytes.NewReader(invalidateFrame("foo")),
	})
	cn.MarkCscReadPending()

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("custom processor was run on probed (not pre-buffered) bytes and retired the conn: %v", err)
	}
	if processed {
		t.Fatal("custom processor must not report success on probed (not pre-buffered) bytes")
	}
}

// TestDrainPushNotifications_RelaxedBudgetToleratesFragmentedFrame pins that the
// background drain uses pushDrainBudget (push.md): while maintenance relaxation is
// active, a push frame fragmented past the small hard cap must still complete rather
// than be treated as a fatal mid-frame desync that evicts this conn's CSC coverage.
func TestDrainPushNotifications_RelaxedBudgetToleratesFragmentedFrame(t *testing.T) {
	oldHardReadCap := cscDrainHardReadCap
	cscDrainHardReadCap = 10 * time.Millisecond // tail arrives after this, within relaxed
	defer func() { cscDrainHardReadCap = oldHardReadCap }()

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(client)
	// Activate relaxation so pushDrainBudget raises the cap well above 10ms.
	cn.SetRelaxedTimeout(500*time.Millisecond, 500*time.Millisecond)

	frame := invalidateFrame("fragmented")
	split := len(frame) - 4
	if _, err := server.Write(frame[:split]); err != nil {
		t.Fatalf("write frame prefix: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("frame prefix never became socket-readable")
	}
	go func() {
		time.Sleep(60 * time.Millisecond) // past the 10ms hard cap, within the 500ms budget
		_, _ = server.Write(frame[split:])
	}()

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("relaxed drain of fragmented frame errored (budget not applied?): %v", err)
	}
	if !processed || rec.count() != 1 {
		t.Fatalf("fragmented frame not processed under relaxed budget: processed=%v calls=%d",
			processed, rec.count())
	}
}

// TestBackgroundDrainerLifecycle verifies start stores a handle on the client,
// double-start is a no-op, and stop joins the goroutine. The handle is
// intentionally RETAINED (not cleared) after stop: cscPoolHook is read on the
// command hot path, so niling the CSC fields under a concurrent Close would race;
// repeat stops are made idempotent by teardownOnce instead.
func TestBackgroundDrainerLifecycle(t *testing.T) {
	cp := pool.NewConnPool(&pool.Options{
		Dialer:   func(context.Context) (net.Conn, error) { return nil, errors.New("no dial in lifecycle test") },
		PoolSize: 1,
	})
	defer cp.Close()
	c := &baseClient{opt: &Options{Protocol: 3}, connPool: cp}

	c.startBackgroundDrainer()
	h := c.cscDrainHandle
	if h == nil {
		t.Fatal("startBackgroundDrainer did not store a drain handle")
	}

	// Double-start must not replace the handle.
	c.startBackgroundDrainer()
	if c.cscDrainHandle != h {
		t.Fatal("double start replaced the drain handle")
	}

	c.stopBackgroundDrainer()
	// The handle is retained (see doc above), but the goroutine must have exited.
	if c.cscDrainHandle != h {
		t.Fatal("stopBackgroundDrainer must retain the drain handle")
	}
	select {
	case <-h.done:
	default:
		t.Fatal("stopBackgroundDrainer returned before the drainer goroutine exited")
	}

	// Stop again: idempotent, no panic, no double-close of the stop channel.
	c.stopBackgroundDrainer()
}

func TestHandlerContextCloseDuringCSCDrainIsDeferred(t *testing.T) {
	closeReturned := make(chan error, 1)
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", &closingHandler{closeReturned: closeReturned}, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	h := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	resourcesClosed := make(chan struct{})
	onClose := &onCloseHooks{}
	onClose.register("test", func() error {
		close(resourcesClosed)
		return nil
	})
	c := &baseClient{
		cscDrainHandle: h,
		opt:            &Options{Protocol: 3},
		pushProcessor:  proc,
		onClose:        onClose,
	}
	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("key"))
	defer cleanup()

	drainReturned := make(chan error, 1)
	go func() {
		_, err := c.drainPushNotifications(cn)
		drainReturned <- err
	}()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("handler Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler Close joined its own drain and deadlocked")
	}
	select {
	case <-h.stop:
	case <-time.After(time.Second):
		t.Fatal("Close did not signal the drainer to stop")
	}
	select {
	case err := <-drainReturned:
		if err != nil {
			t.Fatalf("drainPushNotifications: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not return after the handler")
	}
	select {
	case <-resourcesClosed:
		t.Fatal("resources closed before the active drain returned")
	default:
	}
	ordinaryCloseStarted := make(chan struct{})
	ordinaryCloseReturned := make(chan error, 1)
	go func() {
		close(ordinaryCloseStarted)
		ordinaryCloseReturned <- c.Close()
	}()
	<-ordinaryCloseStarted
	select {
	case err := <-ordinaryCloseReturned:
		t.Fatalf("ordinary Close returned before teardown completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// The real drainer closes done immediately after the pass returns.
	close(h.done)
	select {
	case err := <-ordinaryCloseReturned:
		if err != nil {
			t.Fatalf("ordinary Close during deferred teardown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ordinary Close did not return after teardown completed")
	}
	select {
	case <-resourcesClosed:
	case <-time.After(time.Second):
		t.Fatal("deferred Close did not finish after the drain returned")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
}

// TestCscDrainIntervalClampsMinimum: sub-millisecond DrainInterval values are
// clamped to cscMinDrainInterval (unreliable timers would silently loosen the
// staleness bound); values at or above the floor pass through.
func TestCscDrainIntervalClampsMinimum(t *testing.T) {
	sub := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: 100 * time.Microsecond},
	}}
	if got := sub.cscDrainInterval(); got != cscMinDrainInterval {
		t.Fatalf("sub-ms DrainInterval must clamp to %v, got %v", cscMinDrainInterval, got)
	}

	above := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: 10 * time.Millisecond},
	}}
	if got := above.cscDrainInterval(); got != 10*time.Millisecond {
		t.Fatalf("above-floor DrainInterval must pass through, got %v", got)
	}

	unset := &baseClient{opt: &Options{Protocol: 3}}
	if got := unset.cscDrainInterval(); got != cscDrainSkipWindow {
		t.Fatalf("unset DrainInterval must default to %v, got %v", cscDrainSkipWindow, got)
	}
}

// TestInvalidateHandlerDecodesPayloads: the SharedTracking invalidate handler
// must evict for both string and []byte key names in the push payload.
func TestInvalidateHandlerDecodesPayloads(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.set("get:foo", []string{testCSCNamespacedKey(0, "foo")}, []byte("1"))
	cache.set("get:quux", []string{testCSCNamespacedKey(0, "quux")}, []byte("2"))
	h := &invalidateHandler{cache: cache, keyPrefix: cscNamespacePrefix(0, "")}

	err := h.HandlePushNotification(context.Background(), push.NotificationHandlerContext{},
		[]interface{}{"invalidate", []interface{}{"foo", []byte("quux")}})
	if err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}
	if n := cache.Len(); n != 0 {
		t.Fatalf("both entries should be invalidated, Len=%d", n)
	}
}

// TestInvalidateHandlerInlineNoRefreshKeepsRefetched pins the cursor Medium finding
// (csc_integration.go inline !canRefresh path): with refresh OFF, the inline invalidation
// must still honor the fetch-order guard — the mirror of the batcher's !canRefresh branch
// (TestInvalBatcherNoRefreshGuardKeepsRefetched). An entry whose fetchSeq exceeds the
// push's observe snapshot (a miss reserved AFTER the invalidation was observed) must NOT
// be cancelled, or coalesced waiters wake as spurious extra misses.
//
// Deterministic seam: Reserve stamps the entry's fetchSeq from the global cscFetchSeq;
// storing the global back below it makes the handler's snapshot (loaded from cscFetchSeq)
// precede the entry, modelling the reserved-after-observe race without a live goroutine.
//
// Red-check: revert the inline branch to cache.DeleteByRedisKey — the entry is
// unconditionally evicted and this fails.
func TestInvalidateHandlerInlineNoRefreshKeepsRefetched(t *testing.T) {
	ctx := context.Background()
	old := cscFetchSeq.Load()
	defer cscFetchSeq.Store(old)

	lc := NewLocalCache(CacheConfig{MaxEntries: 1024})
	rk := testCSCNamespacedKey(0, "rk")
	tok, fetch := lc.Reserve("ck:hot", []string{rk})
	if tok == 0 || !fetch {
		t.Fatalf("Reserve = (%d, %v); want a fresh reservation", tok, fetch)
	}
	if !lc.fulfill("ck:hot", tok, 0, []byte("v")) {
		t.Fatal("fulfill failed")
	}
	// The entry's fetchSeq is now > 0. Drop the global BELOW it so the handler's
	// observe-snapshot (cscFetchSeq.Load()) precedes the entry: the guard must keep it.
	cscFetchSeq.Store(0)

	// refresh nil + batcher nil (window 0) -> the inline !canRefresh path with lc != nil.
	h := &invalidateHandler{cache: lc, keyPrefix: cscNamespacePrefix(0, "")}
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{"invalidate", []interface{}{"rk"}}); err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}

	if _, ok := lc.Get(ctx, "ck:hot"); !ok {
		t.Fatal("inline refresh-off invalidation evicted a value refetched after the observe " +
			"snapshot; the fetch-order guard must apply on the inline path too (cursor Medium)")
	}
}

// TestInvalidateHandlerNilPayloadFlushes: a nil <keys> payload (emitted by the
// server on FLUSHDB/FLUSHALL) must flush the entire cache.
func TestInvalidateHandlerNilPayloadFlushes(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.set("get:foo", []string{testCSCNamespacedKey(0, "foo")}, []byte("1"))
	cache.set("get:quux", []string{testCSCNamespacedKey(0, "quux")}, []byte("2"))
	h := &invalidateHandler{cache: cache, keyPrefix: cscNamespacePrefix(0, "")}

	err := h.HandlePushNotification(context.Background(), push.NotificationHandlerContext{},
		[]interface{}{"invalidate", nil})
	if err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}
	if n := cache.Len(); n != 0 {
		t.Fatalf("nil payload must flush the whole cache, Len=%d", n)
	}
}

// boundCache reads the handler's current cache binding under its lock.
func boundCache(h *invalidateHandler) Cache {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.cache
}

// TestInvalidateHandlerReleasedOnClose: closing a client that OWNS its cache
// (ClientSideCacheConfig) must release the invalidate handler's BINDING. An
// application-supplied processor outlives the client; a handler left bound to
// the dead cache would make a successor client's registration fail and
// silently disable its CSC. The handler itself stays registered — and
// protected — so application code cannot unregister invalidation out from
// under a live client.
func TestInvalidateHandlerReleasedOnClose(t *testing.T) {
	p := NewPushNotificationProcessor()

	c1 := NewClient(&Options{
		Addr:                      "localhost:1", // never dialed
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	if c1.baseClient.csc == nil {
		t.Fatal("first client should have CSC attached")
	}
	ih, ok := p.GetHandler(invalidatePushName).(*invalidateHandler)
	if !ok {
		t.Fatal("invalidate handler should be registered while the client lives")
	}
	// The handler is protected: user-level unregistration must FAIL, so a live
	// client's invalidation can't be silently removed by application code.
	if err := p.UnregisterHandler(invalidatePushName); err == nil {
		t.Fatal("UnregisterHandler must fail for the protected invalidate handler")
	}

	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Still registered, but the binding is released.
	if p.GetHandler(invalidatePushName) == nil {
		t.Fatal("handler must stay registered (protected) after Close; only its binding is released")
	}
	if boundCache(ih) != nil {
		t.Fatal("owned-cache binding must be released on Close")
	}

	// A successor client reusing the processor rebinds the handler.
	c2 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c2.Close() })
	if c2.baseClient.csc == nil {
		t.Fatal("successor client must be able to attach CSC after the first Close")
	}
	if boundCache(ih) != c2.baseClient.csc {
		t.Fatal("successor client must rebind the released handler to its own cache")
	}
}

// TestInvalidateHandlerRetainedForSharedCache: with an explicitly supplied
// (shared) cache the client does not own it — other clients on the same
// processor may still rely on the handler, so Close must leave it registered.
func TestInvalidateHandlerRetainedForSharedCache(t *testing.T) {
	p := NewPushNotificationProcessor()
	shared := NewLocalCache(CacheConfig{MaxEntries: 16})

	c1 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCache:           shared,
	})
	c2 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCache:           shared,
	})
	if c1.baseClient.csc == nil || c2.baseClient.csc == nil {
		t.Fatal("both clients should share CSC on the same cache+processor")
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ih, ok := p.GetHandler(invalidatePushName).(*invalidateHandler)
	if !ok {
		t.Fatal("shared-cache handler must survive one client's Close: the second client still needs invalidations")
	}
	if boundCache(ih) != shared {
		t.Fatal("shared-cache binding must not be released by a non-owning client's Close")
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("close second client: %v", err)
	}
	if boundCache(ih) != nil {
		t.Fatal("shared-cache binding must be released after the last client closes")
	}
}

// newDampingClient builds a baseClient whose drainer ticks every millisecond,
// draining the given conns round-robin (one per pass) through an
// always-erroring custom processor.
func newDampingClient(cns ...*pool.Conn) *baseClient {
	return &baseClient{
		opt: &Options{
			Protocol:              3,
			ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: time.Millisecond},
		},
		connPool:      &drainablePooler{cns: cns},
		pushProcessor: erroringProcessor{push.NewProcessor(), errors.New("always fails")},
	}
}

// waitDrainerSelfStop asserts the drainer stops itself (damping) and that CSC
// serving is disabled.
func waitDrainerSelfStop(t *testing.T, c *baseClient) {
	t.Helper()
	h := c.cscDrainHandle
	if h == nil {
		t.Fatal("drainer did not start")
	}
	select {
	case <-h.done:
		// Drainer self-stopped after the damping threshold.
	case <-time.After(5 * time.Second):
		t.Fatal("drainer did not self-stop on persistent custom-processor errors")
	}
	if c.cscActive.Load() {
		t.Fatal("cscActive must be false after the damping threshold: stale hits must not be served")
	}
}

// TestBackgroundDrainerDisablesCSCOnPersistentCustomErrors: a custom processor
// that fails every drain would otherwise remove (and force a redial of) a conn
// per tick forever. After cscDrainCustomErrCap consecutive failures the drainer
// must disable CSC serving (cscActive=false) and stop, instead of churning.
func TestBackgroundDrainerDisablesCSCOnPersistentCustomErrors(t *testing.T) {
	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	c := newDampingClient(cn)
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	if err := registerInvalidateHandler(c.pushProcessor, cache, cscNamespacePrefix(0, "")); err != nil {
		t.Fatalf("register invalidate handler: %v", err)
	}
	ih := lookupInvalidateHandler(c.pushProcessor)
	if ih == nil {
		t.Fatal("invalidate handler was not registered")
	}
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	const ownerConnID = uint64(55)
	hook.bumpInitGen(ownerConnID)
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, ownerConnID, []byte("v")) {
		t.Fatal("failed to seed owned entry")
	}
	c.csc = cache
	c.cscPoolHook = hook
	dp := c.connPool.(*drainablePooler)
	dp.AddPoolHook(hook)
	c.startBackgroundDrainer()
	t.Cleanup(c.stopBackgroundDrainer)
	waitDrainerSelfStop(t, c)
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("damping must evict entries whose invalidation coverage just stopped")
	}
	if boundCache(ih) != nil {
		t.Fatal("damping must release the invalidate-handler binding")
	}
	if got := dp.removedHooks(); got != 1 {
		t.Fatalf("damping must remove the inactive pool hook once, got %d removals", got)
	}
}

// TestBackgroundDrainerDampingSurvivesCleanConns: in a real pool, each fatal
// drain removes its conn and the freshly dialed replacement has nothing
// buffered — its drain is a no-op. Such clean drains must NOT reset the
// damping counter, or a persistently failing processor would churn conns
// forever without ever tripping the cap.
func TestBackgroundDrainerDampingSurvivesCleanConns(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris", "illumos":
	default:
		t.Skip("platform has no non-consuming socket probe for an empty connection")
	}

	pushy, cleanup1 := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup1()
	// A conn with nothing buffered and nothing on the socket: its drain is a
	// clean no-op. Use TCP so the Unix non-consuming socket probe can prove
	// emptiness; opaque wrappers deliberately request a bounded read instead.
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()
	clean := pool.NewConn(client)

	// Alternate failing and clean conns per drain pass.
	c := newDampingClient(pushy, clean)
	c.startBackgroundDrainer()
	t.Cleanup(c.stopBackgroundDrainer)
	waitDrainerSelfStop(t, c)
}

// drainablePooler is a non-*pool.ConnPool Pooler implementing idleConnDrainer.
// With cns set, each DrainIdleConns pass hands the callback one conn,
// round-robin; without, passes are just counted.
type drainablePooler struct {
	pool.Pooler
	cns []*pool.Conn

	mu          sync.Mutex
	called      int
	removedHook int
}

func (d *drainablePooler) DrainIdleConns(_ context.Context, _ *pool.DrainState, fn func(cn *pool.Conn) error) {
	d.mu.Lock()
	n := d.called
	d.called++
	d.mu.Unlock()
	if len(d.cns) > 0 {
		_ = fn(d.cns[n%len(d.cns)])
	}
}

func (d *drainablePooler) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.called
}

func (d *drainablePooler) AddPoolHook(pool.PoolHook) {}

func (d *drainablePooler) RemovePoolHook(pool.PoolHook) {
	d.mu.Lock()
	d.removedHook++
	d.mu.Unlock()
}

func (d *drainablePooler) SupportsPoolHooks() bool {
	return true
}

func (d *drainablePooler) removedHooks() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.removedHook
}

// pubsubMessageFrame builds a RESP3 `>` push frame: ["message", ch, payload].
func pubsubMessageFrame(ch, payload string) []byte {
	return []byte(fmt.Sprintf(">3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(ch), ch, len(payload), payload))
}

// TestDrainPushNotifications_LeavesPubSubFrames: the drain loop must not
// consume pub/sub-reserved push frames — they belong to the pub/sub system
// (same guard as the built-in processor, cf. PR #3842).
func TestDrainPushNotifications_LeavesPubSubFrames(t *testing.T) {
	proc := push.NewProcessor()
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, pubsubMessageFrame("ch", "hello"))
	defer cleanup()

	if _, err := c.drainPushNotifications(cn); err != nil {
		t.Fatalf("drainPushNotifications returned error: %v", err)
	}
	if !cn.HasBufferedData() {
		t.Fatal("pub/sub message frame was consumed by the drain loop; it must stay buffered")
	}
}

// TestDrainPushNotifications_IncompleteFrameTimeouts distinguishes a
// non-consuming name peek from a reply read that has already consumed bytes.
func TestDrainPushNotifications_IncompleteFrameTimeouts(t *testing.T) {
	tests := map[string]struct {
		partial   []byte
		wantFatal bool
	}{
		"name": {
			partial: []byte(">2\r\n$10\r\ninval"),
		},
		"payload": {
			partial:   []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfo"),
			wantFatal: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			proc := push.NewProcessor()
			c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
			cn, cleanup := newReaderBufferedPushConn(t, test.partial)
			defer cleanup()

			_, err := c.drainPushNotifications(cn)
			if test.wantFatal && err == nil {
				t.Fatal("timeout after reply consumption must be fatal")
			}
			if !test.wantFatal && err != nil {
				t.Fatalf("non-consuming peek timeout must end the batch: %v", err)
			}
		})
	}
}

// TestBackgroundDrainerUsesOptionalInterface: any Pooler implementing
// idleConnDrainer gets background draining, not only *pool.ConnPool.
func TestBackgroundDrainerUsesOptionalInterface(t *testing.T) {
	dp := &drainablePooler{}
	c := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: time.Millisecond},
	}, connPool: dp}

	c.startBackgroundDrainer()
	defer c.stopBackgroundDrainer()

	deadline := time.After(2 * time.Second)
	for dp.calls() == 0 {
		select {
		case <-deadline:
			t.Fatal("drainer never called the pooler's DrainIdleConns")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestBackgroundDrainerCleanupOnGC verifies the runtime.AddCleanup safety net:
// a client that starts a drainer and is then dropped WITHOUT Close must have its
// drainer goroutine stopped once the *Client wrapper is garbage-collected.
func TestBackgroundDrainerCleanupOnGC(t *testing.T) {
	cp := pool.NewConnPool(&pool.Options{
		Dialer:   func(context.Context) (net.Conn, error) { return nil, errors.New("no dial in cleanup test") },
		PoolSize: 1,
	})
	defer cp.Close()

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	const ownerConnID = uint64(66)
	hook.bumpInitGen(ownerConnID)
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, ownerConnID, []byte("v")) {
		t.Fatal("failed to seed owned entry")
	}

	// Build a *Client with a running drainer, register the cleanup, and return
	// ONLY its done channel so the *Client becomes unreachable when this returns.
	done := func() <-chan struct{} {
		c := &Client{baseClient: &baseClient{
			opt:         &Options{Protocol: 3},
			connPool:    cp,
			csc:         cache,
			cscPoolHook: hook,
		}}
		c.baseClient.startBackgroundDrainer()
		h := c.baseClient.cscDrainHandle
		if h == nil {
			t.Fatal("drainer did not start")
		}
		cscRegisterCleanups(c)
		return h.done
	}()

	deadline := time.After(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-done:
			if _, ok := cache.Get(context.Background(), "get:k"); ok {
				t.Fatal("GC cleanup stopped the drainer without evicting its coverage")
			}
			return
		case <-time.After(50 * time.Millisecond):
		}
		select {
		case <-deadline:
			t.Fatal("drainer goroutine did not stop after the client was GC'd")
		default:
		}
	}
}

// TestInvalidationStatsSplitByDedup proves the invalidation/deletion split that
// #3 introduced: Invalidations counts every key named in incoming pushes (at the
// handler choke point, BEFORE dedup), while Deletions counts the keys the window
// batcher actually applied (AFTER dedup). Their gap is the dedup — the signal the
// split exists to expose, and the invariant every prior stats-counting round
// violated.
func TestInvalidationStatsSplitByDedup(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour) // buffer everything; flush deterministically via stop-drain

	// One push naming 'hot' 200 times plus 'cold' once: 201 incoming keys.
	keys := make([]interface{}, 0, 201)
	for i := 0; i < 200; i++ {
		keys = append(keys, "hot")
	}
	keys = append(keys, "cold")
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, keys}); err != nil {
		t.Fatalf("invalidate: %v", err)
	}

	// Invalidations are counted at the handler, before dedup: all 201.
	if inv := cache.InvalidationStats(); inv != 201 {
		t.Fatalf("InvalidationStats = %d, want 201 (every incoming key, pre-dedup)", inv)
	}

	// Deletions are applied post-dedup by the window batcher. The 1h window never
	// fires on its own — force the flush via the stop-drain.
	h.mu.RLock()
	b := h.batcher
	h.mu.RUnlock()
	if b == nil {
		t.Fatal("expected a windowed batcher for a nonzero window")
	}
	b.stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if del, _ := cache.DeletionStats(); del >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	del, _ := cache.DeletionStats()
	if del != 2 {
		t.Fatalf("deletions = %d, want 2 (dedup collapses 200 'hot' + 1 'cold')", del)
	}
	// The gap between incoming and applied IS the dedup.
	if got := cache.InvalidationStats() - del; got != 199 {
		t.Fatalf("Invalidations - Deletions = %d, want 199 (the dedup gap)", got)
	}
}

// TestBatcherRepointedToSurvivorOnRefreshClose pins the cursor finding: when the
// active refresh owner closes, clearRefreshQueue must repoint the running batcher
// at the surviving sibling BEFORE stopping it, so the stop-drain feeds evicted-hot
// keys to the live survivor's refresher rather than the closed owner's (whose
// drainer is gone). Asserting the pointer is enough: it is the whole fix.
func TestBatcherRepointedToSurvivorOnRefreshClose(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour)

	qA := &cscRefreshQueue{}
	qB := &cscRefreshQueue{}
	h.setRefreshQueue(qA)
	h.setRefreshQueue(qB) // B is the active owner

	b := h.ensureBatcher()
	if b == nil {
		t.Fatal("expected a windowed batcher")
	}
	if got := b.refresh.Load(); got != qB {
		t.Fatalf("batcher refresh = %p, want active owner qB %p", got, qB)
	}

	// Active owner B closes: survivor A is restored AND the batcher is repointed at
	// A before its stop-drain. clearRefreshQueue now detaches+signals only and
	// returns the batcher; join it (its run() was started by ensureBatcher) so the
	// stop-drain finishes before the test ends and no goroutine touches the shared
	// LocalCache concurrently with the next test under -race.
	if bb := h.clearRefreshQueue(qB); bb != nil {
		bb.join()
	}
	if got := b.refresh.Load(); got != qA {
		t.Fatalf("after owner close: batcher refresh = %p, want survivor qA %p (stop-drain would feed the dead owner)", got, qA)
	}
}

// TestPushDrainBudgetHonorsRelaxation pins the drain budget used by every push
// drain (pushDrainWithin): the hard cap normally, RAISED to the connection's
// relaxed timeout while maintenance relaxation is active. Without the raise a
// push frame fragmented past the cap — likeliest mid-failover, exactly when
// relaxation is on — times out mid-frame, and the pre-command path then retires
// the healthy connection (and fails the command outright with MaxRetries=0).
func TestPushDrainBudgetHonorsRelaxation(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	defer cn.Close()

	const hardCap = 50 * time.Millisecond
	if got := pushDrainBudget(cn, hardCap); got != hardCap {
		t.Fatalf("no relaxation: budget = %v, want the hard cap %v", got, hardCap)
	}
	cn.SetRelaxedTimeout(10*time.Second, 10*time.Second)
	if got := pushDrainBudget(cn, hardCap); got != 10*time.Second {
		t.Fatalf("relaxed: budget = %v, want the relaxed 10s", got)
	}
	cn.ClearRelaxedTimeout()
	if got := pushDrainBudget(cn, hardCap); got != hardCap {
		t.Fatalf("cleared: budget = %v, want the hard cap %v", got, hardCap)
	}
	// A relaxed window SMALLER than the cap must not lower the budget: the raise
	// is one-directional.
	cn.SetRelaxedTimeout(time.Millisecond, time.Millisecond)
	if got := pushDrainBudget(cn, hardCap); got != hardCap {
		t.Fatalf("small relaxed window: budget = %v, want the hard cap %v", got, hardCap)
	}
}

// TestCSCMissCoalescerEnqueueHonorsCallerCancel pins that while a coalesced miss is
// merely WAITING TO ENQUEUE (queue full / worker stalled, pre-I/O), a caller that
// cancels its ctx aborts even when ContextTimeoutEnabled is false — the enqueue
// select watches the ORIGINAL ctx, not the Background wctx that (correctly) gates the
// reply wait. Without the fix the enqueue select watches Background, so a cancelled
// caller blocks on a full queue indefinitely.
func TestCSCMissCoalescerEnqueueHonorsCallerCancel(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	mc := &cscMissCoalescer{
		c:    &baseClient{opt: &Options{}, csc: cache}, // ContextTimeoutEnabled defaults false
		ch:   make(chan *cscMissReq, 1),
		stop: make(chan struct{}),
	}
	mc.ch <- &cscMissReq{} // fill ch to capacity so the enqueue send blocks

	token, fetch := cache.Reserve("ck", []string{"rk"})
	if token == 0 || !fetch {
		t.Fatalf("Reserve = (%d, %v); want a fresh reservation", token, fetch)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // caller already cancelled

	errCh := make(chan error, 1)
	go func() {
		_, err := mc.fetch(ctx, makeCmd("get", "rk"), "ck", token, mc.c.metadataView())
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("fetch returned %v, want context.Canceled (enqueue must honor caller "+
				"cancel even with ContextTimeoutEnabled=false)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fetch blocked on a full queue despite a cancelled caller ctx; the enqueue " +
			"select must watch the caller ctx, not the ContextTimeoutEnabled-gated wctx")
	}
}

type rejectingLimiter struct {
	allow  atomic.Int64
	report atomic.Int64
	err    error
}

func (l *rejectingLimiter) Allow() error         { l.allow.Add(1); return l.err }
func (l *rejectingLimiter) ReportResult(_ error) { l.report.Add(1) }

// TestGetConnLimitedSurfacesRejection pins F-A: getConnLimited reports a
// Limiter.Allow rejection distinctly (limited=true) with the error RAW — not
// reported (ReportResult is for dial results only) and not tagged for the
// coalescer's re-run. A rejection tagged errCSCRetryUncached/cscSessionError would
// send processCached back through processWithRetry -> getConn -> Allow a SECOND
// time, letting a stateful limiter admit the very miss it just denied.
func TestGetConnLimitedSurfacesRejection(t *testing.T) {
	sentinel := errors.New("breaker open")
	lim := &rejectingLimiter{err: sentinel}
	c := &baseClient{opt: &Options{Limiter: lim}} // Allow rejects before any dial; connPool untouched

	cn, limited, err := c.getConnLimited(context.Background())
	if cn != nil || !limited || err != sentinel {
		t.Fatalf("getConnLimited = (%v, limited=%v, %v); want (nil, true, sentinel)", cn, limited, err)
	}
	if got := lim.allow.Load(); got != 1 {
		t.Fatalf("Allow called %d times, want 1", got)
	}
	if got := lim.report.Load(); got != 0 {
		t.Fatalf("ReportResult called %d times for an admission denial, want 0", got)
	}
	// Must not be a re-run-triggering error (that path would re-Allow).
	var se cscSessionError
	if err == errCSCRetryUncached || errors.As(err, &se) {
		t.Fatal("a limiter rejection is tagged for the coalescer re-run; it would call Allow twice")
	}
}

// TestCSCMissLimiterDenialOnlyFailsWakingRequestPlainly pins a cursor review
// finding on #3989: a Limiter.Allow rejection at session acquire is specific
// to the ONE request that woke the session (first) and triggered that Allow
// call — not to every OTHER request already sitting in mc.ch, which never
// called Allow themselves. Failing them all with first's raw, non-retryable
// error (the previous drainQueuePlain) denied operations the limiter was
// never asked about. first must still get the raw error (a re-run would call
// Allow a second time — see TestGetConnLimitedSurfacesRejection); every
// already-queued request must instead get errCSCRetryUncached, so
// processCached re-runs it on the ordinary per-command path and each calls
// Allow independently.
func TestCSCMissLimiterDenialOnlyFailsWakingRequestPlainly(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	sentinel := errors.New("csc test: limiter denied")
	lim := &rejectingLimiter{err: sentinel}
	mc := &cscMissCoalescer{
		c:    &baseClient{opt: &Options{Limiter: lim}, csc: cache},
		ch:   make(chan *cscMissReq, 4),
		stop: make(chan struct{}),
	}

	mkReq := func(key string) *cscMissReq {
		token, ok := cache.Reserve(key, []string{key})
		if !ok {
			t.Fatalf("Reserve(%q) declined a fresh key", key)
		}
		return &cscMissReq{cacheKey: key, token: token, done: make(chan error, 1)}
	}

	first := mkReq("first")
	queued := mkReq("queued")
	// Send order matters: runFullDuplexSession pulls exactly one request off
	// mc.ch as "first" via a blocking receive, so first must be sent first;
	// queued must still be sitting in the channel when Limiter.Allow denies
	// the session.
	mc.ch <- first
	mc.ch <- queued

	stopped, backoff := mc.runFullDuplexSession()
	if stopped || !backoff {
		t.Fatalf("runFullDuplexSession = (stopped=%v, backoff=%v); want (false, true)", stopped, backoff)
	}

	select {
	case err := <-first.done:
		if err != sentinel {
			t.Fatalf("first.done = %v; want the raw sentinel (first triggered the denied Allow call)", err)
		}
	default:
		t.Fatal("first was not settled")
	}

	select {
	case err := <-queued.done:
		if err != errCSCRetryUncached {
			t.Fatalf("queued.done = %v; want errCSCRetryUncached — queued never called Allow, so it "+
				"must retry on the ordinary per-command path instead of inheriting first's denial", err)
		}
	default:
		t.Fatal("queued was not settled")
	}

	if got := lim.allow.Load(); got != 1 {
		t.Fatalf("Allow called %d times, want exactly 1 (only for first)", got)
	}
}

// --- CSC teardown lifecycle (F1/F2/F3) -------------------------------------

// newCSCTeardownClient builds a client with BOTH refresh-on-invalidate and
// reader-miss coalescing enabled and a fast drain tick. No live redis is needed:
// the drainer, refresher, and coalescer goroutines all park at startup (no idle
// conns to drain, no misses to fetch), which is exactly the teardown surface these
// tests exercise. It asserts all three background workers are running.
func newCSCTeardownClient(t *testing.T) (*Client, *cscMissCoalescer, *cscRevalidateHandle, *cscDrainHandle) {
	t.Helper()
	client := NewClient(&Options{
		Addr:                               "127.0.0.1:0",
		Protocol:                           3,
		ClientSideCacheConfig:              &ClientSideCacheConfig{MaxEntries: 16, DrainInterval: time.Millisecond},
		ClientSideCacheRefreshOnInvalidate: true,
		ClientSideCacheCoalesceMisses:      true,
	})
	mc := client.cscMissCoalescer.Load()
	rh := client.cscRefreshHandle
	dh := client.cscDrainHandle
	if mc == nil || rh == nil || dh == nil {
		client.Close()
		t.Fatalf("precondition: coalescer(%v) refresher(%v) drainer(%v) must all be running", mc, rh, dh)
	}
	return client, mc, rh, dh
}

// TestCSCSelfDisableStopsRefresherAndCoalescer pins F1: when CSC turns ITSELF off
// (here via disableCSCServing; also custom-processor damping), the drainer exits
// its loop and its defer must tear the refresher and coalescer down the same way
// Close does — otherwise those goroutines run for the client's life (the refresher
// parked on its window, a coalescer session able to hold a pool connection). Before
// the fix the defer only unbound the queue and released the handler, so the
// refresher goroutine never joined and this test times out on rh.done.
func TestCSCSelfDisableStopsRefresherAndCoalescer(t *testing.T) {
	client, mc, rh, dh := newCSCTeardownClient(t)
	defer client.Close()

	// Self-disable: the drainer observes cscActive=false on its next (~1ms) tick,
	// exits, and (with the fix) stops the refresher + coalescer.
	client.disableCSCServing(context.Background(), "test self-disable")

	select {
	case <-rh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresher goroutine leaked: self-disable did not join it")
	}
	select {
	case <-mc.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("coalescer leaked: self-disable did not signal its sessions to stop")
	}
	select {
	case <-dh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainer did not exit after self-disable")
	}
}

// TestCSCConstructionRaceDeclinesToStartAfterTeardown pins the fix for a bot
// review finding: attachSharedTrackingCSC starts the drainer BEFORE assigning
// cscRefreshHandle/cscMissCoalescer, so stopCSCRefresherAndCoalescer's
// one-time workersStopOnce body can in principle run (via the drainer's own
// self-disable defer reacting to an async conn init's disableCSCServing)
// WHILE startCSCRefresher/startCSCMissCoalescer are still constructing.
// workersStopOnce alone only guarantees the teardown body runs once — not
// that it runs after a worker exists for it to stop. Forcing teardown to run
// FIRST (deterministic, no goroutine timing needed) must make both start
// functions decline to publish anything, rather than create a goroutine
// nothing will ever stop again.
func TestCSCConstructionRaceDeclinesToStartAfterTeardown(t *testing.T) {
	lc := NewLocalCache(CacheConfig{MaxEntries: 16})
	dh := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	active := &atomic.Bool{}
	active.Store(true)
	c := &baseClient{
		opt: &Options{
			ClientSideCacheRefreshOnInvalidate: true,
			ClientSideCacheCoalesceMisses:      true,
		},
		csc:            lc,
		cscKeyPrefix:   "p:",
		cscDrainHandle: dh,
		cscActive:      active,
	}

	// Set ONLY workersTornDown, bypassing the rest of stopCSCRefresherAndCoalescer's
	// body — cscActive stays TRUE. This isolates the new check: if it were removed,
	// both start functions' pre-existing "cscActive already false" fast path would
	// not fire either (cscActive is true here), so a pass can only be explained by
	// the workersMu/workersTornDown check actually running.
	dh.workersMu.Lock()
	dh.workersTornDown = true
	dh.workersMu.Unlock()

	// A worker that starts AFTER losing the race must decline entirely: no
	// queue/handle/coalescer published, no goroutine leaked past Close().
	c.startCSCRefresher()
	if c.cscRefreshQueue != nil || c.cscRefreshHandle != nil {
		t.Fatal("startCSCRefresher published a queue/handle after teardown already ran " +
			"(workersStopOnce can never be Do'd again to stop it — leaked goroutine)")
	}
	c.startCSCMissCoalescer()
	if c.cscMissCoalescer.Load() != nil {
		t.Fatal("startCSCMissCoalescer published a coalescer after teardown already ran " +
			"(workersStopOnce can never be Do'd again to stop it — leaked goroutine holding a pool conn)")
	}
}

// TestCSCStopCSCRefresherAndCoalescerSetsWorkersTornDown pins the plumbing
// TestCSCConstructionRaceDeclinesToStartAfterTeardown assumes: the real
// teardown entry point actually sets the flag the start functions check, not
// just a hand-set test double.
func TestCSCStopCSCRefresherAndCoalescerSetsWorkersTornDown(t *testing.T) {
	dh := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	active := &atomic.Bool{}
	active.Store(true)
	c := &baseClient{cscDrainHandle: dh, cscActive: active}

	c.stopCSCRefresherAndCoalescer()

	dh.workersMu.Lock()
	tornDown := dh.workersTornDown
	dh.workersMu.Unlock()
	if !tornDown {
		t.Fatal("stopCSCRefresherAndCoalescer did not set workersTornDown")
	}
	if active.Load() {
		t.Fatal("stopCSCRefresherAndCoalescer did not deactivate cscActive")
	}
}

// TestCSCStartRefresherHoldsLockAcrossBatcherJoin pins a second bot-review
// round (cursor) on the workersTornDown fix: checking the flag only around
// the initial field write (cscRefreshHandle/cscMissCoalescer) was not
// enough — startCSCRefresher's remaining setup (setRefreshQueue's
// predecessor-batcher join) and startCSCMissCoalescer's wg.Add loop both run
// AFTER that write, so a concurrent stopCSCRefresherAndCoalescer could still
// see the published handle and block on <-h.done for a goroutine not yet
// launched (unbounded on a slow/stuck predecessor batcher; for the
// coalescer, a concurrent wg.Wait()/wg.Add is sync.WaitGroup's documented
// misuse case). The fix moved the ENTIRE publish sequence (see
// startCSCRefresher's "publish" closure) inside the workersMu critical
// section.
//
// This installs a controlled predecessor batcher (no run() goroutine backs
// it, so its join blocks until the test releases it) standing in for that
// slow window, and checks workersTornDown WHILE startCSCRefresher is
// confirmed still parked in the join — not just that teardown eventually
// finishes. Verified against the version that only locks around the field
// write (commit e2a103e5): there, workersMu is free by the time this checks
// it (start released it before setRefreshQueue/join), so the TryLock below
// succeeds and the test correctly fails.
//
// Covers only the refresher: its predecessor-batcher join is the one
// controllable blocking hook available without wiring a live connection.
// startCSCMissCoalescer's mirrored wg.Add/wg.Wait window is fixed by the
// same lock widening (see its publish closure) but has no separate test —
// there is no equivalent controllable hook in its publish sequence.
func TestCSCStartRefresherHoldsLockAcrossBatcherJoin(t *testing.T) {
	proc := NewPushNotificationProcessor()
	lc := NewLocalCache(CacheConfig{MaxEntries: 16})
	if err := registerInvalidateHandler(proc, lc, "p:"); err != nil {
		t.Fatalf("registerInvalidateHandler: %v", err)
	}
	ih := lookupInvalidateHandler(proc)

	controlledDone := make(chan struct{})
	ih.mu.Lock()
	ih.batcher = &cscInvalBatcher{
		cache:  lc,
		ch:     make(chan cscInvalItem, 1),
		wake:   make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		done:   controlledDone,
	}
	ih.mu.Unlock()

	dh := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	active := &atomic.Bool{}
	active.Store(true)
	c := &baseClient{
		opt: &Options{
			ClientSideCacheRefreshOnInvalidate: true,
			PushNotificationProcessor:          proc,
		},
		csc:            lc,
		cscKeyPrefix:   "p:",
		cscDrainHandle: dh,
		cscActive:      active,
	}

	startDone := make(chan struct{})
	go func() {
		c.startCSCRefresher()
		close(startDone)
	}()

	// Poll (bounded, 2s deadline) until startCSCRefresher has actually
	// acquired workersMu, rather than sleeping a fixed interval and hoping it
	// got scheduled in time: a not-yet-scheduled goroutine passes a plain
	// "hasn't returned yet" check identically, which is exactly the kind of
	// timing assumption that flakes under -race/CI load. TryLock never
	// blocks, so polling it cannot deadlock on the very lock being observed;
	// a failed TryLock is the (only) proof the lock is currently held.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if !dh.workersMu.TryLock() {
			break // observed held: start has reached the lock
		}
		dh.workersMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("startCSCRefresher never acquired workersMu within 2s")
		}
		time.Sleep(time.Millisecond)
	}

	teardownDone := make(chan struct{})
	go func() {
		c.stopCSCRefresherAndCoalescer()
		close(teardownDone)
	}()

	// Give teardown a moment to attempt (and, on a regression, complete) its
	// own acquisition. Not itself load-bearing for correctness: the assertion
	// below only depends on start still holding the lock, which the poll
	// above already established and which cannot change until this test
	// closes controlledDone.
	time.Sleep(20 * time.Millisecond)

	// TryLock, not Lock: on the fix, start holds workersMu for the whole
	// publish sequence (including this join), so Lock here would block until
	// close(controlledDone) below — deadlocking the test on itself. TryLock
	// never blocks, so it can observe "still held" without contending for the
	// very lock we're testing the holder of.
	if dh.workersMu.TryLock() {
		dh.workersMu.Unlock()
		t.Fatal("workersMu was free while startCSCRefresher was still blocked in the predecessor " +
			"batcher's join — it must hold the lock across the whole publish sequence, not just " +
			"the initial field write, or a concurrent teardown can get past it (and set " +
			"workersTornDown) before publish has actually finished")
	}

	close(controlledDone) // release the join
	select {
	case <-startDone:
	case <-time.After(time.Second):
		t.Fatal("startCSCRefresher did not finish after the batcher join was released")
	}
	select {
	case <-teardownDone:
	case <-time.After(time.Second):
		t.Fatal("stopCSCRefresherAndCoalescer did not finish after startCSCRefresher completed")
	}
}

// TestCSCConstructionWinsRaceStartsNormally is the control for
// TestCSCConstructionRaceDeclinesToStartAfterTeardown: absent a concurrent
// teardown, both start functions must publish and launch exactly as before —
// the fix only changes behavior on the losing side of the race.
//
// startCSCMissCoalescer launches real fullDuplexLoop goroutines against a
// baseClient with a nil connPool. That's safe only because those goroutines
// select on mc.stop/mc.ch before ever touching the pool, and this test's mc.ch
// never receives — stopCSCRefresherAndCoalescer below signals stop first. If a
// future change pre-feeds cscMissCoalescer.ch here, this becomes a nil-pointer
// panic on the pool, not a race-detector finding.
func TestCSCConstructionWinsRaceStartsNormally(t *testing.T) {
	lc := NewLocalCache(CacheConfig{MaxEntries: 16})
	dh := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	active := &atomic.Bool{}
	active.Store(true)
	c := &baseClient{
		opt: &Options{
			ClientSideCacheRefreshOnInvalidate: true,
			ClientSideCacheCoalesceMisses:      true,
		},
		csc:            lc,
		cscKeyPrefix:   "p:",
		cscDrainHandle: dh,
		cscActive:      active,
	}

	c.startCSCRefresher()
	if c.cscRefreshQueue == nil || c.cscRefreshHandle == nil {
		t.Fatal("startCSCRefresher did not publish when no teardown raced it")
	}
	c.startCSCMissCoalescer()
	if c.cscMissCoalescer.Load() == nil {
		t.Fatal("startCSCMissCoalescer did not publish when no teardown raced it")
	}

	// Clean up what was started so the test does not leak goroutines.
	c.stopCSCRefresherAndCoalescer()
}

// TestCSCTeardownReleasesCoalescerBeforeRefresher pins F3's ordering: teardown must
// stop the coalescer (releasing its held pool connection) BEFORE it signals and
// joins the refresher, whose stop-drain flush needs a main-pool connection. It also
// pins that this happens WHILE serving is still active (the refresher's flush would
// otherwise be a no-op). We block the coalescer's join with an extra wg counter and
// assert the refresher has not been signalled while that join is parked. With the
// wrong order (refresher first) rh.stop is closed before the coalescer join, so the
// mid-teardown assertion fires.
func TestCSCTeardownReleasesCoalescerBeforeRefresher(t *testing.T) {
	client, mc, rh, _ := newCSCTeardownClient(t)

	// Park stopCSCMissCoalescer's wg.Wait() so we can observe the intermediate
	// teardown state. Add precedes the concurrent Wait and the counter is already
	// >=1 (a session is running), so this is a legal WaitGroup use. Release via a
	// once-guarded cleanup so an early t.Fatal cannot leave the join (and the
	// deferred Close) deadlocked.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCoalescer := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseCoalescer)
	mc.wg.Add(1)
	go func() { defer mc.wg.Done(); <-release }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		client.stopCSCRefresherAndCoalescer()
	}()

	// Step 1 signalled the coalescer to stop (stopWorkers closed mc.stop) ...
	select {
	case <-mc.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("coalescer was never signalled to stop")
	}
	// ... but the refresher must NOT be signalled yet: its flush needs the pool
	// connection the coalescer is still (artificially) holding.
	select {
	case <-rh.stop:
		t.Fatal("refresher was signalled before the coalescer released its connection (F3 ordering regression)")
	case <-time.After(150 * time.Millisecond):
	}
	// Serving must still be active so the refresher's final flush is not a no-op.
	if client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("cscActive was cleared before the refresher's final flush ran")
	}

	// Let the coalescer join complete; the refresher teardown then proceeds.
	releaseCoalescer()
	select {
	case <-rh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresher did not stop after the coalescer released")
	}
	<-done
	if client.cscActive.Load() {
		t.Fatal("cscActive must be false after teardown completes")
	}
	client.Close()
}

// TestCSCHandlerCloseKeepsServingUntilRefresherFlush pins F2: a handler-initiated
// close (a custom push handler calling handlerCtx.Client.Close()) must NOT
// preemptively deactivate serving. The async canonical close drives the teardown,
// so the refresher's final flush runs with cscActive still true and re-fetches the
// in-window keys. Before the fix cscHandlerClient.Close stored cscActive=false
// synchronously, so this assertion fails immediately after the handler Close.
func TestCSCHandlerCloseKeepsServingUntilRefresherFlush(t *testing.T) {
	client, mc, _, dh := newCSCTeardownClient(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCoalescer := func() { releaseOnce.Do(func() { close(release) }) }
	// Order matters: Close is deferred FIRST so it runs LAST; releaseCoalescer is
	// deferred second so it runs BEFORE Close on unwind. An early t.Fatal would
	// otherwise deadlock — the parked coalescer join holds the drainer teardownOnce
	// that the deferred Close would then block on.
	defer client.Close()
	defer releaseCoalescer()

	// Park the coalescer join so the async close cannot reach the deactivate step
	// (which follows the coalescer stop and the refresher flush in the canonical
	// order). This makes the "still serving" observation deterministic.
	mc.wg.Add(1)
	go func() { defer mc.wg.Done(); <-release }()

	// Handler-initiated close (runs on the drainer goroutine in production; here we
	// invoke the same entry point directly). Returns immediately; teardown is async.
	hc := cscHandlerClient{baseClient: client.baseClient}
	if err := hc.Close(); err != nil {
		t.Fatalf("handler close: %v", err)
	}

	// The async close entered teardown (coalescer signalled) ...
	select {
	case <-mc.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("handler close did not start the canonical teardown")
	}
	// ... and serving is STILL active: the fix removed the preemptive deactivate.
	if client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("handler close deactivated CSC before the refresher's final flush (F2 regression)")
	}

	// Unblock; the teardown completes and deactivates.
	releaseCoalescer()
	select {
	case <-dh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown did not complete after the coalescer join was released")
	}
	if client.cscActive.Load() {
		t.Fatal("CSC still serving after teardown completed")
	}
}

// TestPushDrainWithinShortBoundaryUnderRelaxation pins #3989: the speculative drain's
// FIRST-byte wait stays bounded by the short cap even while maintenance relaxation is
// active. Otherwise a TLS control record (socket-readable, zero RESP bytes) would block
// the drain for the full relaxed timeout, stalling a ready command or the idle drainer.
func TestPushDrainWithinShortBoundaryUnderRelaxation(t *testing.T) {
	oldCap := cscDrainHardReadCap
	cscDrainHardReadCap = 20 * time.Millisecond
	defer func() { cscDrainHardReadCap = oldCap }()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	cn.SetRelaxedTimeout(2*time.Second, 2*time.Second) // relaxation active: budget >> cap
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: push.NewProcessor()}

	// No data on the pipe: with no RESP frame begun, the drain must return within the
	// short cap, not wait out the 2s relaxed budget.
	start := time.Now()
	if err := c.pushDrainWithin(context.Background(), cn, cscDrainHardReadCap); err != nil {
		t.Fatalf("pushDrainWithin: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("speculative drain waited %v with no frame; must be bounded by the short cap "+
			"(%v), not the relaxed timeout (#3989)", elapsed, cscDrainHardReadCap)
	}
}

// TestInvalidateHandlerFullFlushPairsBatcherWithSnapshotCache pins #3989 fCCqu: a
// full-cache flush must drop the batcher only when it still belongs to the cache being
// flushed. If a release+rebind (A->B) races between the caller's snapshot and the
// flush, dropping B's batcher while flushing A would skip B's queued deletes and B
// would serve stale.
func TestInvalidateHandlerFullFlushPairsBatcherWithSnapshotCache(t *testing.T) {
	cacheA := NewLocalCache(CacheConfig{MaxEntries: 16})
	cacheB := NewLocalCache(CacheConfig{MaxEntries: 16})
	batcherB := newTestBatcher(cacheB, 64, time.Hour)

	// Handler now bound to cache B (post-rebind); the full-flush was snapshotted for A.
	h := &invalidateHandler{cache: cacheB, batcher: batcherB}

	cacheA.set("get:x", []string{"x"}, []byte("v"))
	if cacheA.Len() == 0 {
		t.Fatal("precondition: cacheA should hold an entry")
	}
	epochBefore := batcherB.epoch.Load()

	// Snapshot was A, but h.cache is B: flush A, and DO NOT drop B's batcher.
	h.fullFlush(cacheA)

	if got := batcherB.epoch.Load(); got != epochBefore {
		t.Fatalf("fullFlush dropped the rebound cache B's batcher (epoch %d -> %d) while flushing A (#3989)",
			epochBefore, got)
	}
	if cacheA.Len() != 0 {
		t.Fatal("fullFlush did not flush the snapshot cache A")
	}

	// Control: an unchanged binding (h.cache == cache) DOES drop the batcher.
	epochB2 := batcherB.epoch.Load()
	h.fullFlush(cacheB)
	if batcherB.epoch.Load() == epochB2 {
		t.Fatal("fullFlush must drop the batcher when the binding is unchanged")
	}
}

// TestInvalidateHandlerFullFlushNonComparableCacheNoPanic pins the sameCache guard in
// fullFlush: comparing h.cache == cache directly panics when the cache's dynamic type is
// non-comparable (a struct with a slice field), crashing the push goroutine on every
// FLUSHDB/FLUSHALL. sameCache compares safely (#3989).
func TestInvalidateHandlerFullFlushNonComparableCacheNoPanic(t *testing.T) {
	inner := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache := nonComparableCache{Cache: inner, marker: []byte("m")}
	batcher := newTestBatcher(cache, 64, time.Hour)
	h := &invalidateHandler{cache: cache, batcher: batcher}

	inner.set("get:x", []string{"x"}, []byte("v"))
	if inner.Len() == 0 {
		t.Fatal("precondition: cache should hold an entry")
	}

	// A bare == would panic here; sameCache must guard it. The batcher is not dropped for
	// a non-comparable type (sameCache returns false), but the snapshot is still flushed.
	h.fullFlush(cache)

	if inner.Len() != 0 {
		t.Fatal("fullFlush did not flush the non-comparable cache")
	}
}

// A refresh republish must NOT renew a key's reader-access recency, or every
// invalidation would keep refreshing a key nobody reads anymore (a self-sustaining
// refetch loop). After the republish + restoreAccessToken, the entry must be COLD
// relative to the horizon of its last real read.
func TestRefreshRepublishDoesNotRenewDemand(t *testing.T) {
	lc := NewLocalCache(CacheConfig{MaxEntries: 16})
	const key, rk = "get:k", "rk"

	// Reader miss fill: the entry is Valid and hot.
	tok, _ := lc.Reserve(key, []string{rk})
	if !lc.fulfill(key, tok, 0, []byte("v")) {
		t.Fatal("seed fulfill failed")
	}

	// One refresh cycle: collect the hot target (captures the reader-access token and
	// deletes the entry), republish a fresh value, restore the captured token.
	targets := lc.deleteByRedisKeyCollectingHot(rk, lc.LRUClock()-1, ^uint64(0), nil)
	if len(targets) != 1 {
		t.Fatalf("want 1 hot target, got %d", len(targets))
	}
	tok2, _ := lc.Reserve(key, []string{rk})
	if !lc.fulfill(key, tok2, 0, []byte("v2")) {
		t.Fatal("republish fulfill failed")
	}
	lc.restoreAccessToken(key, targets[0].accessNs)

	// No reader touched the key since. At the horizon of its last real read the entry
	// must be COLD (lastAccessNs == that token, not > it). Without the restore the
	// republish would leave a newer token here and the entry would still be collected
	// — the self-sustaining loop.
	if hot := lc.deleteByRedisKeyCollectingHot(rk, targets[0].accessNs, ^uint64(0), nil); len(hot) != 0 {
		t.Fatalf("refreshed-but-unread entry still hot after restore; got %d targets", len(hot))
	}
}

func TestCSCKeyArgTypes(t *testing.T) {
	text := "1"
	type namedInt int
	for _, tc := range []struct {
		arg interface{}
		ok  bool
	}{
		{"key", true},
		{[]byte("key"), true},
		{[]byte(nil), true},
		{int(1), true},
		{int8(1), true},
		{int16(1), true},
		{int32(1), true},
		{int64(1), true},
		{uint(1), true},
		{uint8(1), true},
		{uint16(1), true},
		{uint32(1), true},
		{uint64(1), true},
		{nil, false},
		{&text, false},
		{(*string)(nil), false},
		{float32(1), false},
		{float64(1), false},
		{true, false},
		{uintptr(1), false},
		{namedInt(1), false},
		{cscWireSmuggler{wire: "key", str: "key"}, false},
	} {
		t.Run(fmt.Sprintf("%T/%v", tc.arg, tc.arg), func(t *testing.T) {
			cmd := makeCmd("get", tc.arg)
			if got := keyArgOK(cmd, 1); got != tc.ok {
				t.Fatalf("keyArgOK = %v, want %v", got, tc.ok)
			}
			meta, _ := cscCommandMetaFor(cmd)
			if got := cscCanExtractRedisKeys(meta, cmd); got != tc.ok {
				t.Fatalf("cscCanExtractRedisKeys = %v, want %v", got, tc.ok)
			}
			if got := cscExtractRedisKeys(meta, cmd); (got != nil) != tc.ok {
				t.Fatalf("cscExtractRedisKeys = %v, want extraction success %v", got, tc.ok)
			}
		})
	}
	for _, pos := range []int{-1, 2} {
		if keyArgOK(makeCmd("get", "key"), pos) {
			t.Fatalf("keyArgOK accepted missing position %d", pos)
		}
	}
}

func TestCSCIntArgMatchesWireParsing(t *testing.T) {
	text := "1"
	for _, arg := range []interface{}{
		int(1), int8(-1), int16(2), int32(math.MaxInt32), int64(math.MaxInt64), int64(math.MinInt64),
		uint(1), uint8(2), uint16(3), uint32(math.MaxUint32), uint64(math.MaxUint64),
		uint64(math.MaxInt), uint64(math.MaxInt) + 1,
		"1", "+1", "01", "-0", "-1", " 1", "1.0", "", "18446744073709551616",
		[]byte("2"), []byte("-2"), []byte("invalid"), []byte(nil),
		&text, nil, true, float64(1),
		cscWireSmuggler{wire: "1", str: "2"},
	} {
		t.Run(fmt.Sprintf("%T/%v", arg, arg), func(t *testing.T) {
			cmd := makeCmd("zdiff", arg, "key")
			wire, ok := keyArg(cmd, 1)
			want, err := strconv.Atoi(wire)
			wantOK := ok && err == nil
			got, gotOK := cscIntArg(cmd, 1)
			if gotOK != wantOK || gotOK && got != want {
				t.Fatalf("cscIntArg = (%d, %v), wire parse = (%d, %v)", got, gotOK, want, wantOK)
			}
		})
	}
	for _, pos := range []int{-1, 3} {
		if _, ok := cscIntArg(makeCmd("zdiff", 1, "key"), pos); ok {
			t.Fatalf("cscIntArg accepted missing position %d", pos)
		}
	}
}

func TestCSCSubscribeCaseFolding(t *testing.T) {
	for _, name := range []string{
		"subscribe", "SUBSCRIBE", "PSubscribe", "ssubscribe", "GET", "unsubscribe",
		"subscrİbe", "pSUBSCRİBE", "ſubscribe", "ſſubscribe", "subscr\xffbe",
	} {
		lower := strings.ToLower(name)
		want := lower == "subscribe" || lower == "psubscribe" || lower == "ssubscribe"
		for _, arg := range []interface{}{name, []byte(name), &name} {
			if got := isSubscribeCmd(makeCmd(arg, "channel")); got != want {
				t.Errorf("isSubscribeCmd(%T(%q)) = %v, want %v", arg, name, got, want)
			}
		}
	}
}

// buildCacheKey preserves the original renderer as a compatibility oracle and
// benchmark baseline for cscRenderEntryKey.
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

func TestCSCRenderEntryKey(t *testing.T) {
	text := "value"
	for _, args := range [][]interface{}{
		nil,
		{"get", "key"},
		{[]byte("MGET"), []byte("key\x00\r\n"), 42},
		{"set", "key", &text, nil, true, 1.5},
		{"get", strings.Repeat("x", 128<<10)},
		{"get", struct{}{}},
		{"get", cscWireSmuggler{wire: "key", str: "other"}},
	} {
		cmd := makeCmd(args...)
		raw, wantOK := buildCacheKey(cmd)
		got, ok := cscRenderEntryKey("namespace\x00", "fingerprint", cmd)
		if ok != wantOK {
			t.Fatalf("render success = %v, want %v", ok, wantOK)
		}
		if !ok {
			continue
		}
		want := cscEntryKey("namespace\x00", "fingerprint", raw)
		if got != want {
			t.Fatal("entry key differs from the original RESP rendering")
		}
		// A later render may reuse the buffer but must not change a prior key.
		for range 10 {
			cscRenderEntryKey("other", "generation", makeCmd("get", "other"))
		}
		if got != want {
			t.Fatal("entry key changed after buffer reuse")
		}
	}
}

func TestCSCEntryKeyPrefixLen(t *testing.T) {
	for _, prefix := range []string{"", "namespace\x00", "שלום"} {
		if got, want := cscEntryKeyPrefixLen(prefix, "fingerprint"), len(cscEntryKey(prefix, "fingerprint", "")); got != want {
			t.Fatalf("prefix length = %d, want %d", got, want)
		}
	}
}

func TestCommandMetadataSnapshotNormalizedAndImmutable(t *testing.T) {
	if len(commandInfoSnapshotByName()) != len(commandInfoSnapshotRecords) {
		t.Fatal("snapshot contains duplicate command names")
	}
	before := make(map[string]*CommandInfo, len(commandInfoSnapshotByName()))
	for name, info := range commandInfoSnapshotByName() {
		if info.Name != name || !reflect.DeepEqual(cloneCommandInfoForName(name, info), info) {
			t.Fatalf("snapshot record %q requires normalization", name)
		}
		before[name] = cloneCommandInfo(info)
	}

	// All mutable layers may take their inputs from the shared snapshot.
	buildCommandMetadataView(nil, nil)
	buildCommandMetadataViewForServer(commandInfoSnapshotByName(), nil, "6.2.0")
	buildCommandMetadataView(commandInfoSnapshotByName(), commandInfoSnapshotByName())
	if !reflect.DeepEqual(commandInfoSnapshotByName(), before) {
		t.Fatal("building metadata views mutated the snapshot")
	}
}

func TestCSCTableFingerprintEncoding(t *testing.T) {
	for _, table := range []map[string]cscCommandMeta{
		nil,
		defaultCommandMetadataView().cscTable,
		{
			"a\x00\n": {bits: 255, extract: 255, guard: 255, firstKey: -32768, lastKey: 32767, step: -1},
			"z":       {numkeysAt: -32768},
		},
	} {
		// Preserve the original wire encoding, including sorting, separators,
		// signed fields, and the truncated hexadecimal digest.
		names := make([]string, 0, len(table))
		for name := range table {
			names = append(names, name)
		}
		sort.Strings(names)
		h := sha256.New()
		for _, name := range names {
			m := table[name]
			fmt.Fprintf(h, "%s\x00%d %d %d %d %d %d %d\n",
				name, m.bits, m.extract, m.guard, m.firstKey, m.lastKey, m.step, m.numkeysAt)
		}
		if got, want := cscTableFingerprint(table), hex.EncodeToString(h.Sum(nil)[:16]); got != want {
			t.Fatalf("fingerprint = %q, want %q", got, want)
		}
	}
}

// TestCSCPerformanceCompatibility pins the sorted eligible-command dump and
// cache namespace before performance changes. Set CSC_COMPATIBILITY_DUMP to a
// file path to compare the full dump byte-for-byte between revisions.
func TestCSCPerformanceCompatibility(t *testing.T) {
	view := (&baseClient{}).metadataView()
	var names []string
	for name, meta := range view.cscTable {
		if cscIsClientSideCacheable(meta) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	dump := strings.Join(names, "\n") + "\n"
	if len(names) != 120 {
		t.Fatalf("eligible commands: got %d, want 120", len(names))
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(dump))); got != "cd52438234a475701fd546e2a685cf493e7486bd78ebe2bbb8ac7f224485b609" {
		t.Fatalf("eligible-command dump changed: sha256=%s", got)
	}
	if view.cscFingerprint != "0255d4ea837ce42f4814b7b4b9ff820c" {
		t.Fatalf("default CSC fingerprint changed: %s", view.cscFingerprint)
	}
	if path := os.Getenv("CSC_COMPATIBILITY_DUMP"); path != "" {
		if err := os.WriteFile(path, []byte(view.cscFingerprint+"\n"+dump), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
