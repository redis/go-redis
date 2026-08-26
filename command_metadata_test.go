package redis

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/routing"
)

// testTrustedLiveRecords returns sample Redis 8.10 records plus extras.
func testTrustedLiveRecords(extra map[string]*CommandInfo) map[string]*CommandInfo {
	records := map[string]*CommandInfo{
		"eval_ro": {Name: "eval_ro", Flags: []string{"readonly", "script_runner"}},
		"ttl":     {Name: "ttl", Flags: []string{"readonly", "fast"}, Tips: []string{"nondeterministic_output"}},
	}
	for k, v := range extra {
		records[k] = v
	}
	return records
}

func testCommandMetadataFetchResult(records map[string]*CommandInfo) commandMetadataFetchResult {
	return commandMetadataFetchResult{
		records:           records,
		serverVersion:     "8.10.0",
		serverFingerprint: "8.10.0",
	}
}

func testCommandMetadataFetchResultFor(
	records map[string]*CommandInfo,
	version, fingerprint string,
) commandMetadataFetchResult {
	return commandMetadataFetchResult{
		records:           records,
		serverVersion:     version,
		serverFingerprint: fingerprint,
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestCommandMetadataStoreNilForDefaultConfig(t *testing.T) {
	if s := newCommandMetadataStore(nil, nil); s != nil {
		t.Error("nil config must share the default view (nil store)")
	}
	if s := newCommandMetadataStore(&CommandMetadataConfig{}, nil); s != nil {
		t.Error("zero config must share the default view (nil store)")
	}
	if s := newCommandMetadataStore(&CommandMetadataConfig{
		Overrides: map[string]*CommandInfo{"get": nil},
	}, nil); s == nil {
		t.Error("a config with overrides needs its own store")
	}
	if s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive}, nil); s == nil {
		t.Error("PreferLive needs its own store")
	}
}

func TestCommandMetadataEnsureLiveForStaticOrNilConfig(t *testing.T) {
	for _, cfg := range []*CommandMetadataConfig{nil, {}} {
		var calls atomic.Int32
		s := newCommandMetadataStoreForLive(cfg,
			func(context.Context) (commandMetadataFetchResult, error) {
				calls.Add(1)
				return testCommandMetadataFetchResultFor(
					testTrustedLiveRecords(nil), "8.10.0", "server-a",
				), nil
			})
		if err := s.ensureLive(context.Background()); err != nil {
			t.Fatalf("ensureLive(%v): %v", cfg, err)
		}
		if !s.view().live {
			t.Fatalf("ensureLive(%v) did not publish a live view", cfg)
		}
		if got := s.serverFingerprint(); got != "server-a" {
			t.Fatalf("ensureLive(%v) adopted fingerprint %q, want server-a", cfg, got)
		}
		if err := s.ensureLive(context.Background()); err != nil {
			t.Fatalf("second ensureLive(%v): %v", cfg, err)
		}
		if calls.Load() != 1 {
			t.Fatalf("ensureLive(%v) fetched %d times, want 1", cfg, calls.Load())
		}
		s.stopAndJoin()
	}
}

func TestCommandMetadataStaticStoreNeverStartsWorker(t *testing.T) {
	s := newCommandMetadataStore(&CommandMetadataConfig{
		Overrides: map[string]*CommandInfo{"get": nil},
	}, func(context.Context) (commandMetadataFetchResult, error) {
		t.Error("static mode must never fetch")
		return commandMetadataFetchResult{}, nil
	})
	s.onConnInit()
	s.requestRefresh()
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		t.Error("static store must not start a worker")
	}
	s.stopAndJoin() // must not hang on a never-started worker
}

func TestCommandMetadataStandaloneStoreDoesNotRequireCSC(t *testing.T) {
	client := NewClient(&Options{
		Protocol: 2,
		CommandMetadata: &CommandMetadataConfig{Overrides: map[string]*CommandInfo{
			"get": nil,
		}},
	})
	defer client.Close()
	if client.baseClient.cmdMeta == nil {
		t.Fatal("non-default metadata config did not create a standalone store")
	}
	if client.baseClient.csc != nil {
		t.Fatal("metadata-only client unexpectedly enabled CSC")
	}
}

func TestCommandMetadataWithTimeoutCloneKeepsOwnerAlive(t *testing.T) {
	clone, store := func() (*Client, *commandMetadataStore) {
		owner := NewClient(&Options{
			Addr: "127.0.0.1:0",
			CommandMetadata: &CommandMetadataConfig{
				Mode: CommandMetadataPreferLive,
			},
		})
		return owner.WithTimeout(time.Second), owner.cmdMeta
	}()

	if clone.lifecycleOwner == nil {
		t.Fatal("metadata clone must retain its canonical lifecycle owner")
	}
	for range 20 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-store.stop:
		t.Fatal("a reachable clone must keep its metadata-worker owner alive")
	default:
	}
	if err := clone.Close(); err != nil {
		t.Fatalf("close clone: %v", err)
	}
	select {
	case <-store.stop:
	default:
		t.Fatal("closing a metadata clone must stop its canonical owner's store")
	}
}

func TestCommandMetadataPreferLivePublishes(t *testing.T) {
	live := testTrustedLiveRecords(map[string]*CommandInfo{
		"myext.get": {
			Name: "myext.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
	})
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			return testCommandMetadataFetchResult(live), nil
		})
	defer s.stopAndJoin()

	static := s.view()
	if static.live {
		t.Fatal("initial view must be static")
	}
	if isCacheableInView(static, makeCmd("myext.get", "k")) {
		t.Fatal("static view must not know the live-only command")
	}

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool { return s.view().live }) {
		t.Fatal("live view never published")
	}
	upgraded := s.view()
	if !isCacheableInView(upgraded, makeCmd("myext.get", "k")) {
		t.Error("live record should make myext.get cacheable")
	}
	// Snapshot fallback and corrections remain effective.
	if !isCacheableInView(upgraded, makeCmd("get", "k")) {
		t.Error("snapshot GET must remain cacheable under the live view")
	}
	if isCacheableInView(upgraded, makeCmd("touch", "k")) {
		t.Error("built-in corrections must survive the live upgrade")
	}
	if upgraded.cscFingerprint == static.cscFingerprint {
		t.Error("a decision change must change the fingerprint")
	}
	// Connection churn must not refetch a live view.
	s.onConnInit()
	select {
	case <-s.refresh:
		t.Error("onConnInit must not re-request after a live view is published")
	default:
	}
}

func TestCommandMetadataPre810PublishesRecordsAndFailsClosedForUnknownCSC(t *testing.T) {
	keyed := func(name string) *CommandInfo {
		return &CommandInfo{
			Name: name, Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		}
	}
	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			calls.Add(1)
			return testCommandMetadataFetchResultFor(map[string]*CommandInfo{
				"get":        keyed("get"),
				"oldext.get": keyed("oldext.get"),
			}, "7.4.0", "7.4.0"), nil
		})
	defer s.stopAndJoin()

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool { return s.view().live }) {
		t.Fatal("pre-8.10 live output was not published")
	}
	view := s.view()
	if view.serverVersion != "7.4.0" {
		t.Fatalf("live view server version = %q, want 7.4.0", view.serverVersion)
	}
	if view.records["oldext.get"] == nil {
		t.Fatal("pre-8.10 records must remain available to shared metadata consumers")
	}
	if !commandRecordHas(view.records["oldext.get"], "dont_cache", true) {
		t.Error("pre-8.10 live-only record must carry a shared dont_cache correction")
	}
	if !isCacheableInView(view, makeCmd("get", "k")) {
		t.Error("a snapshot-known safe command should remain CSC-eligible")
	}
	if isCacheableInView(view, makeCmd("oldext.get", "k")) {
		t.Error("a live-only pre-8.10 command must fail closed for CSC")
	}
	first := calls.Load()
	// Connection churn must not refetch a live view.
	s.onConnInit()
	s.onConnInit()
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != first {
		t.Errorf("onConnInit re-fetched after live publication: %d -> %d", first, calls.Load())
	}
}

func TestCommandMetadataFetchErrorRetries(t *testing.T) {
	oldMin := cmdMetaBackoffMin
	cmdMetaBackoffMin = time.Millisecond
	defer func() { cmdMetaBackoffMin = oldMin }()

	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			if calls.Add(1) < 3 {
				return commandMetadataFetchResult{}, errors.New("transient dial failure")
			}
			return testCommandMetadataFetchResult(testTrustedLiveRecords(nil)), nil
		})
	defer s.stopAndJoin()

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool { return s.view().live }) {
		t.Fatalf("refresh never recovered after transient errors (%d calls)", calls.Load())
	}
}

func TestCommandMetadataPeriodicRefreshRetriesWhileLive(t *testing.T) {
	oldMin := cmdMetaBackoffMin
	cmdMetaBackoffMin = time.Millisecond
	defer func() { cmdMetaBackoffMin = oldMin }()

	var calls atomic.Int32
	periodicFailed := make(chan struct{})
	s := newCommandMetadataStore(&CommandMetadataConfig{
		Mode:            CommandMetadataPreferLive,
		RefreshInterval: 500 * time.Millisecond,
	}, func(context.Context) (commandMetadataFetchResult, error) {
		switch calls.Add(1) {
		case 1:
			return testCommandMetadataFetchResult(testTrustedLiveRecords(nil)), nil
		case 2:
			close(periodicFailed)
			return commandMetadataFetchResult{}, errors.New("transient periodic refresh failure")
		default:
			return testCommandMetadataFetchResult(testTrustedLiveRecords(nil)), nil
		}
	})
	defer s.stopAndJoin()

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool { return s.view().live }) {
		t.Fatal("initial live view was never published")
	}
	select {
	case <-periodicFailed:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic refresh never ran")
	}
	if !waitForCondition(t, 100*time.Millisecond, func() bool { return calls.Load() >= 3 }) {
		t.Fatalf("failed periodic refresh was not retried promptly (%d calls)", calls.Load())
	}
}

func TestCommandMetadataFingerprint(t *testing.T) {
	if defaultCommandMetadataView.cscFingerprint == "" {
		t.Fatal("default view must have a fingerprint")
	}
	same := buildCommandMetadataView(nil, nil)
	if same.cscFingerprint != defaultCommandMetadataView.cscFingerprint {
		t.Error("identical inputs must produce identical fingerprints")
	}
	overridden := buildCommandMetadataView(nil, map[string]*CommandInfo{"get": nil})
	if overridden.cscFingerprint == defaultCommandMetadataView.cscFingerprint {
		t.Error("a decision change must change the fingerprint")
	}
}

func TestCommandMetadataViewImmutableAfterBuild(t *testing.T) {
	override := &CommandInfo{Name: "get", Tips: []string{"dont_cache"}}
	view := buildCommandMetadataView(nil, map[string]*CommandInfo{"get": override})
	override.Tips[0] = "mutated"
	if !commandRecordHas(view.records["get"], "dont_cache", true) {
		t.Error("a published view must not observe later mutations of the override record")
	}
}

func TestCommandMetadataNormalizesEffectiveRecords(t *testing.T) {
	upper := &CommandInfo{
		Name:  "MODULE.GET",
		Flags: []string{"READONLY", "Future_Flag"},
		Tips: []string{
			"REQUEST_POLICY:ALL_SHARDS",
			"RESPONSE_POLICY:AGG_SUM",
			"Future_Tip:MiXeD",
		},
		FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{
			Flags:       []string{"ro", "ACCESS", "PREFIX", "Future_Key_Flag"},
			BeginSearch: "INDEX",
			Index:       1,
			FindKeys:    "RANGE",
			KeyStep:     1,
		}},
	}
	lower := cloneCommandInfo(upper)
	lower.Name = "module.get"
	lower.Flags[0] = "readonly"
	lower.Tips[0] = "request_policy:all_shards"
	lower.Tips[1] = "response_policy:agg_sum"
	lower.KeySpecs[0].Flags[0] = "RO"
	lower.KeySpecs[0].Flags[1] = "access"
	lower.KeySpecs[0].Flags[2] = "prefix"
	lower.KeySpecs[0].BeginSearch = "index"
	lower.KeySpecs[0].FindKeys = "range"

	upperView := buildCommandMetadataView(nil, map[string]*CommandInfo{"MODULE.GET": upper})
	lowerView := buildCommandMetadataView(nil, map[string]*CommandInfo{"module.get": lower})
	if upperView.cscFingerprint != lowerView.cscFingerprint {
		t.Fatal("equivalent normalized records produced different CSC decisions")
	}
	record := upperView.records["module.get"]
	if record == nil || record.Name != "module.get" || record.Flags[0] != "readonly" ||
		record.Tips[0] != "request_policy:all_shards" ||
		record.Tips[1] != "response_policy:agg_sum" ||
		record.KeySpecs[0].Flags[0] != "RO" || record.KeySpecs[0].Flags[1] != "access" ||
		record.KeySpecs[0].Flags[2] != "prefix" ||
		record.KeySpecs[0].BeginSearch != "index" || record.KeySpecs[0].FindKeys != "range" {
		t.Fatalf("effective record was not normalized: %+v", record)
	}
	if record.Flags[1] != "Future_Flag" || record.Tips[2] != "Future_Tip:MiXeD" ||
		record.KeySpecs[0].Flags[3] != "Future_Key_Flag" {
		t.Fatalf("unknown extensions were not preserved verbatim: %+v", record)
	}
	upperRouting := upperView.routingTable["module.get"]
	lowerRouting := lowerView.routingTable["module.get"]
	if !upperRouting.valid || !lowerRouting.valid ||
		upperRouting.policy.Request != lowerRouting.policy.Request ||
		upperRouting.policy.Response != lowerRouting.policy.Response ||
		upperRouting.keyState != lowerRouting.keyState {
		t.Fatalf("equivalent normalized records produced different routing metadata: upper=%+v lower=%+v", upperRouting, lowerRouting)
	}
}

func TestCommandMetadataNormalizationKeepsMalformedSafetySignalsFailClosed(t *testing.T) {
	keyed := func(name string, tips ...string) *CommandInfo {
		return &CommandInfo{
			Name: name, Flags: []string{"READONLY"}, Tips: tips,
			FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{
				Flags: []string{"RO"}, BeginSearch: "INDEX", Index: 1,
				FindKeys: "RANGE", KeyStep: 1,
			}},
		}
	}
	script := keyed("module.script")
	script.Flags = append(script.Flags, "SCRIPT_RUNNER:true")
	blocking := keyed("module.blocking")
	blocking.Flags = append(blocking.Flags, "BLOCKING:1")
	malformedReadonly := keyed("module.readonly")
	malformedReadonly.Flags = []string{"READONLY:true"}
	view := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"module.dontcache": keyed("module.dontcache", "DONT_CACHE:any"),
		"module.random":    keyed("module.random", "NONDETERMINISTIC_OUTPUT:any"),
		"module.route":     keyed("module.route", "REQUEST_POLICY"),
		"module.script":    script,
		"module.blocking":  blocking,
		"module.readonly":  malformedReadonly,
	})

	for _, name := range []string{
		"module.dontcache", "module.random", "module.script", "module.blocking", "module.readonly",
	} {
		if isCacheableInView(view, makeCmd(name, "key")) {
			t.Errorf("malformed negative signal for %s was ignored", name)
		}
	}
	if got := view.records["module.dontcache"].Tips[0]; got != "dont_cache" {
		t.Errorf("dont_cache normalization = %q, want dont_cache", got)
	}
	if got := view.records["module.random"].Tips[0]; got != "nondeterministic_output" {
		t.Errorf("nondeterministic_output normalization = %q, want nondeterministic_output", got)
	}
	if got := view.records["module.script"].Flags[1]; got != "script_runner" {
		t.Errorf("script_runner normalization = %q, want script_runner", got)
	}
	if got := view.records["module.blocking"].Flags[1]; got != "blocking" {
		t.Errorf("blocking normalization = %q, want blocking", got)
	}
	if got := view.records["module.readonly"].Flags[0]; got != "READONLY:true" {
		t.Errorf("malformed positive readonly was normalized to %q", got)
	}
	if got := view.records["module.route"].Tips[0]; got != requestPolicy {
		t.Errorf("request-policy normalization = %q, want %q", got, requestPolicy)
	}
	if _, ok := view.routingTable["module.route"]; ok {
		t.Error("missing request-policy value did not invalidate routing metadata")
	}
}

func TestCommandMetadataLegacyShapedUnknownIsRoutingOnly(t *testing.T) {
	legacy := &CommandInfo{
		Name: "legacy.get", Flags: []string{"readonly"},
		FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
	}
	view := buildCommandMetadataViewForServerWithLegacy(
		map[string]*CommandInfo{"LEGACY.GET": legacy},
		nil,
		"8.10.0",
		map[string]struct{}{"LEGACY.GET": {}},
	)
	cmd := makeCmd("legacy.get", "key")
	if isCacheableInView(view, cmd) {
		t.Fatal("live-only legacy-shaped record must not prove CSC eligibility")
	}
	if !commandRecordHas(view.records["legacy.get"], "dont_cache", true) {
		t.Fatal("legacy-shaped live-only record must carry a shared dont_cache correction")
	}
	meta, ok := routingLookupMeta(view, cmd)
	if !ok {
		t.Fatal("legacy-shaped record was not retained for routing")
	}
	if pos, ok := routingFirstKeyPos(meta, cmd); !ok || pos != 1 {
		t.Fatalf("legacy routing key = (%d, %v), want (1, true)", pos, ok)
	}
}

func TestCommandMetadataLegacyShapeUsesServerVersionCompatibility(t *testing.T) {
	legacyGet := &CommandInfo{
		Name: "get", Flags: []string{"readonly"},
		FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
	}
	legacyTTL := &CommandInfo{
		Name: "ttl", Flags: []string{"readonly"},
		FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
	}
	legacyXPending := &CommandInfo{
		Name: "xpending", Flags: []string{"readonly"},
		FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
	}
	live := map[string]*CommandInfo{
		"get": legacyGet, "ttl": legacyTTL, "xpending": legacyXPending,
	}
	legacy := map[string]struct{}{"get": {}, "ttl": {}, "xpending": {}}

	pre810 := buildCommandMetadataViewForServerWithLegacy(
		live, nil, "6.2.0", legacy,
	)
	if !isCacheableInView(pre810, makeCmd("get", "key")) {
		t.Error("known pre-8.10 legacy record should use its legacy key positions")
	}
	if commandRecordHas(pre810.records["get"], "dont_cache", true) {
		t.Error("known pre-8.10 legacy record received an unnecessary dont_cache correction")
	}
	for _, name := range []string{"ttl", "xpending"} {
		if isCacheableInView(pre810, makeCmd(name, "key")) {
			t.Errorf("known pre-8.10 legacy %s lost its nondeterministic exclusion", name)
		}
		if !commandRecordHas(pre810.records[name], "nondeterministic_output", true) {
			t.Errorf("known pre-8.10 legacy %s did not retain its snapshot exclusion", name)
		}
	}

	redis810 := buildCommandMetadataViewForServerWithLegacy(
		live, nil, "8.10.0", legacy,
	)
	if isCacheableInView(redis810, makeCmd("get", "key")) {
		t.Error("legacy-shaped record from an 8.10+ server must fail closed for CSC")
	}
	if !commandRecordHas(redis810.records["get"], "dont_cache", true) {
		t.Error("8.10+ legacy inconsistency was not represented in the shared record")
	}
}

func TestCommandMetadataViewCopiesLiveRecords(t *testing.T) {
	live := &CommandInfo{
		Name:  "module.read",
		Flags: []string{"readonly"},
		Tips:  []string{"request_policy:all_shards"},
		KeySpecs: []KeySpec{{
			Flags: []string{"RO"}, BeginSearch: "index", Index: 1,
			FindKeys: "range", KeyStep: 1,
		}},
		CommandPolicy: &routing.CommandPolicy{
			Request: routing.ReqAllShards,
			Tips:    map[string]string{routing.ReadOnlyCMD: ""},
		},
	}
	view := buildCommandMetadataView(map[string]*CommandInfo{"module.read": live}, nil)
	live.Flags[0] = "write"
	live.Tips[0] = "request_policy:all_nodes"
	live.KeySpecs[0].Index = 7
	live.KeySpecs[0].Flags[0] = "RW"
	live.CommandPolicy.Request = routing.ReqAllNodes
	delete(live.CommandPolicy.Tips, routing.ReadOnlyCMD)

	got := view.records["module.read"]
	if got == live || got.Flags[0] != "readonly" || got.Tips[0] != "request_policy:all_shards" ||
		got.KeySpecs[0].Index != 1 || got.KeySpecs[0].Flags[0] != "RO" ||
		got.CommandPolicy.Request != routing.ReqAllShards {
		t.Fatalf("live record was not deeply copied: %+v", got)
	}
	if _, ok := got.CommandPolicy.Tips[routing.ReadOnlyCMD]; !ok {
		t.Fatal("live CommandPolicy tips share the caller's map")
	}
}

func TestCommandMetadataLiveTombstonesBlockLowerLayers(t *testing.T) {
	view := buildCommandMetadataViewForServer(map[string]*CommandInfo{
		"GET":   nil,
		"TOUCH": nil,
	}, nil, "8.10.0")
	for _, name := range []string{"get", "touch"} {
		if _, ok := view.records[name]; ok {
			t.Errorf("live tombstone for %s exposed a lower-layer record", name)
		}
		if _, ok := view.tombstones[name]; !ok {
			t.Errorf("live tombstone for %s was not preserved in the view", name)
		}
		if _, ok := view.cscTable[name]; ok {
			t.Errorf("live tombstone for %s exposed a lower-layer CSC entry", name)
		}
	}

	keyedGet := &CommandInfo{
		Name: "myext.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
	view = buildCommandMetadataViewForServer(
		map[string]*CommandInfo{"myext.get": nil},
		map[string]*CommandInfo{"MYEXT.GET": keyedGet},
		"7.4.0",
	)
	if !isCacheableInView(view, makeCmd("myext.get", "k")) {
		t.Error("the highest-priority application override did not replace a live tombstone")
	}
	if _, ok := view.tombstones["myext.get"]; ok {
		t.Error("a valid application override did not clear the lower live tombstone")
	}
}

func TestCommandMetadataTombstonedChildKeepsParentShadowed(t *testing.T) {
	view := buildCommandMetadataViewForServer(map[string]*CommandInfo{
		"container": {
			Name: "container", Flags: []string{"readonly"},
			FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{
				Flags: []string{"RO"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
		"container|child": nil,
	}, nil, "8.10.0")

	cmd := makeCmd("container", "child", "key")
	if _, ok := cscLookupMeta(view, cmd); ok {
		t.Fatal("tombstoned child fell back to the bare parent for CSC")
	}
	if _, ok := routingLookupMeta(view, cmd); ok {
		t.Fatal("tombstoned child fell back to the bare parent for routing")
	}
	if _, ok := view.shadowedParents["container"]; !ok {
		t.Fatal("container parent was not kept explicitly shadowed")
	}
	if _, ok := view.tombstones["container|child"]; !ok {
		t.Fatal("the normalized child tombstone was not preserved")
	}
}

func TestCommandMetadataNestedContainerPrefixesFailClosed(t *testing.T) {
	keyed := func(name string) *CommandInfo {
		return &CommandInfo{
			Name: name, Flags: []string{"readonly"},
			KeySpecs: []KeySpec{{
				Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		}
	}
	view := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"future":          keyed("future"),
		"future|nested":   keyed("future|nested"),
		"future|nested|x": keyed("future|nested|x"),
	})
	if _, ok := view.shadowedParents["future|nested"]; !ok {
		t.Fatal("intermediate nested container was not shadowed")
	}
	cmd := makeCmd("future", "nested", "x", "key")
	if _, ok := cscLookupMeta(view, cmd); ok {
		t.Fatal("unsupported nested invocation used an intermediate CSC record")
	}
	if _, ok := routingLookupMeta(view, cmd); ok {
		t.Fatal("unsupported nested invocation used an intermediate routing record")
	}
}

func TestCommandMetadataNormalizedLiveCollisionFailsClosed(t *testing.T) {
	keyed := &CommandInfo{
		Name: "get", Flags: []string{"readonly"},
		FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{
			Flags: []string{"RO"}, BeginSearch: "index", Index: 1,
			FindKeys: "range", KeyStep: 1,
		}},
	}
	view := buildCommandMetadataViewForServer(map[string]*CommandInfo{
		"GET": nil,
		"get": keyed,
	}, nil, "8.10.0")
	if _, ok := view.records["get"]; ok {
		t.Fatal("case-colliding live tombstone was resurrected")
	}
	if _, ok := view.tombstones["get"]; !ok {
		t.Fatal("case-colliding live metadata was not preserved as a tombstone")
	}
	if isCacheableInView(view, makeCmd("get", "key")) {
		t.Fatal("case-colliding live metadata enabled CSC")
	}
	if _, ok := routingLookupMeta(view, makeCmd("get", "key")); ok {
		t.Fatal("case-colliding live metadata enabled routing")
	}
}

func TestCommandMetadataNormalizedOverrideCollisionFailsClosed(t *testing.T) {
	keyed := &CommandInfo{
		Name: "get", Flags: []string{"readonly"},
		FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{
			Flags: []string{"RO"}, BeginSearch: "index", Index: 1,
			FindKeys: "range", KeyStep: 1,
		}},
	}
	view := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"GET": keyed,
		"get": keyed,
	})
	if _, ok := view.records["get"]; ok {
		t.Fatal("case-colliding application override exposed a record")
	}
	if _, ok := view.tombstones["get"]; !ok {
		t.Fatal("case-colliding application override was not preserved as a tombstone")
	}

	view = buildCommandMetadataView(nil, map[string]*CommandInfo{"GET": nil})
	if _, ok := view.tombstones["get"]; !ok {
		t.Fatal("nil application override was not preserved as a normalized tombstone")
	}
}

func TestCommandMetadataBareParentOverrideIsInert(t *testing.T) {
	// A bare parent override is pruned without hiding its subcommands.
	view := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"memory": {
			Name: "memory", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
	})
	if _, ok := view.cscTable["memory"]; ok {
		t.Error("bare container-parent override must be pruned from the table")
	}
	if isCacheableInView(view, makeCmd("memory", "usage", "k")) {
		t.Error("memory|usage must stay non-cacheable (dont_cache correction)")
	}
	meta, ok := cscLookupMeta(view, makeCmd("memory", "usage", "k"))
	if !ok || meta.bits&cscTipDontCache == 0 {
		t.Error("memory|usage must still resolve through the parent set")
	}
}

func TestCommandMetadataNormalizesOverrideParentNames(t *testing.T) {
	s := newCommandMetadataStore(&CommandMetadataConfig{Overrides: map[string]*CommandInfo{
		"MEMORY": {
			Name: "memory", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
	}}, nil)
	defer s.stopAndJoin()
	if _, ok := s.overrides["MEMORY"]; ok {
		t.Error("override retained its non-normalized key")
	}
	if _, ok := s.static.shadowedParents["memory"]; !ok {
		t.Error("normalized bare-parent override was not recorded as shadowed")
	}
}

func TestCommandMetadataStopBeforeStart(t *testing.T) {
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			t.Error("must not fetch after stop")
			return commandMetadataFetchResult{}, nil
		})
	s.stopAndJoin()
	s.requestRefresh() // must not start a worker after stop
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		t.Error("worker started after stop")
	}
}

func TestCommandMetadataConcurrentRefreshAndStop(t *testing.T) {
	for i := 0; i < 20; i++ {
		s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
			func(context.Context) (commandMetadataFetchResult, error) {
				return testCommandMetadataFetchResult(testTrustedLiveRecords(nil)), nil
			})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.onConnInit()
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.stopAndJoin()
		}()
		wg.Wait()
		s.stopAndJoin()
	}
}

func TestCommandMetadataRedis810LiveRecordIsAuthoritative(t *testing.T) {
	// Redis 8.10+ live records must not inherit snapshot fields.
	flipped := &CommandInfo{
		Name: "set", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
	view := buildCommandMetadataView(testTrustedLiveRecords(map[string]*CommandInfo{"set": flipped}), nil)
	if !isCacheableInView(view, makeCmd("set", "k", "v")) {
		t.Error("Redis 8.10 live record was not authoritative")
	}
	if !commandRecordHas(view.records["set"], "readonly", false) {
		t.Error("resolution rewrote the Redis 8.10 live record")
	}
	if got, want := view.cscTable["set"], cscDeriveMeta(view.records["set"]); got != want {
		t.Fatalf("CSC metadata was not derived solely from the resolved record: got %+v, want %+v", got, want)
	}
}

func TestCommandMetadataCSCTableIsPureResolvedRecordDerivation(t *testing.T) {
	view := buildCommandMetadataViewForServer(map[string]*CommandInfo{
		"TS.INFO": {
			Name: "TS.INFO", Flags: []string{"readonly"},
			FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{
				Flags: []string{"RO"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
		"oldext.get": {
			Name: "OLDEXT.GET", Flags: []string{"readonly"},
			FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{
				Flags: []string{"RO"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
	}, nil, "7.4.0")

	for name, record := range view.records {
		if _, shadowed := view.shadowedParents[name]; shadowed {
			if _, ok := view.cscTable[name]; ok {
				t.Errorf("shadowed parent %q was emitted into the CSC table", name)
			}
			continue
		}
		got, ok := view.cscTable[name]
		if !ok {
			t.Errorf("resolved record %q has no CSC derivation", name)
			continue
		}
		if want := cscDeriveMeta(record); got != want {
			t.Errorf("CSC metadata for %q depends on provenance: got %+v, want %+v", name, got, want)
		}
	}
}

func TestCommandMetadataPre810RefreshKeepsSharedLiveView(t *testing.T) {
	// Older records remain usable outside CSC.
	keyed := &CommandInfo{
		Name: "myext.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
	var pre810 atomic.Bool
	s := newCommandMetadataStore(&CommandMetadataConfig{
		Mode:            CommandMetadataPreferLive,
		RefreshInterval: 5 * time.Millisecond,
	}, func(context.Context) (commandMetadataFetchResult, error) {
		version := "8.10.0"
		if pre810.Load() {
			version = "7.4.0"
		}
		return testCommandMetadataFetchResultFor(
			map[string]*CommandInfo{"myext.get": keyed}, version, "same-server",
		), nil
	})
	defer s.stopAndJoin()

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool {
		return s.view().live && isCacheableInView(s.view(), makeCmd("myext.get", "k"))
	}) {
		t.Fatal("8.10 live view never published")
	}
	pre810.Store(true)
	if !waitForCondition(t, 5*time.Second, func() bool {
		v := s.view()
		return v.live && v.records["myext.get"] != nil &&
			!isCacheableInView(v, makeCmd("myext.get", "k"))
	}) {
		t.Fatal("pre-8.10 refresh did not publish records with CSC restriction")
	}
}

func TestCommandMetadataStopCancelsInflightFetch(t *testing.T) {
	// Close must not wait for a fetch that ignores its context.
	release := make(chan struct{})
	defer close(release)
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			<-release
			return commandMetadataFetchResult{}, errors.New("released late")
		})
	s.onConnInit()
	time.Sleep(20 * time.Millisecond) // let the worker enter the fetch
	done := make(chan struct{})
	go func() {
		s.stopAndJoin()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stopAndJoin blocked on an in-flight fetch")
	}
}

func TestCommandMetadataFetchHonorsContextWithTimeoutsDisabled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	release := make(chan struct{})
	commandSeen := make(chan struct{}, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		serveTestRESPConn(conn, func(command string) string {
			switch command {
			case "hello":
				return "%0\r\n"
			case "command":
				select {
				case commandSeen <- struct{}{}:
				default:
				}
				<-release // simulate a server that never answers COMMAND
				return ""
			default:
				return "+OK\r\n"
			}
		})
	}()

	client := NewClient(&Options{
		Addr:                  ln.Addr().String(),
		Protocol:              3,
		ReadTimeout:           -1,
		WriteTimeout:          -1,
		ContextTimeoutEnabled: false,
		DisableIdentity:       true,
		MaxRetries:            0,
	})
	t.Cleanup(func() {
		_ = client.Close()
		close(release)
		_ = ln.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, fetchErr := client.baseClient.fetchCommandMetadata(ctx)
		done <- fetchErr
	}()

	select {
	case <-commandSeen:
	case <-time.After(time.Second):
		t.Fatal("metadata fetch never reached COMMAND")
	}
	select {
	case fetchErr := <-done:
		if fetchErr == nil {
			t.Fatal("stalled metadata fetch returned nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("metadata fetch ignored its context deadline")
	}
}

func TestCommandMetadataFetchRejectsAndAdoptsDifferentServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		serveTestRESPConn(conn, func(command string) string {
			switch command {
			case "hello":
				return "%1\r\n+version\r\n+8.10.0-A\r\n"
			case "command":
				return "*0\r\n"
			default:
				return "+OK\r\n"
			}
		})
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		DisableIdentity: true,
		MaxRetries:      0,
	})
	store := newCommandMetadataStore(
		&CommandMetadataConfig{Mode: CommandMetadataPreferLive}, nil,
	)
	store.onServerHello("8.10.0-B")
	client.baseClient.cmdMeta = store
	t.Cleanup(func() {
		store.stopAndJoin()
		_ = client.Close()
		_ = ln.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.baseClient.fetchCommandMetadata(ctx); err == nil {
		t.Fatal("metadata fetched from a different server identity was accepted")
	}
	if got := store.serverFingerprint(); got != "8.10.0-A" {
		t.Fatalf("metadata fetch did not record the newly observed identity: %q", got)
	}

	metadata, err := client.baseClient.fetchCommandMetadata(ctx)
	if err != nil {
		t.Fatalf("metadata fetch from the target server failed: %v", err)
	}
	if len(metadata.records) != 0 {
		t.Fatalf("metadata fetch returned %d records, want 0", len(metadata.records))
	}
	if metadata.serverVersion != "8.10.0-A" {
		t.Fatalf("metadata server version = %q, want 8.10.0-A", metadata.serverVersion)
	}
	if metadata.serverFingerprint != "8.10.0-A" {
		t.Fatalf("metadata server fingerprint = %q, want 8.10.0-A", metadata.serverFingerprint)
	}
}

func TestCommandMetadataFetchPreservesCSCEntries(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go serveTestRESPConn(conn, func(command string) string {
				switch command {
				case "hello":
					return "%1\r\n+version\r\n+8.10.0\r\n"
				case "command":
					return "*0\r\n"
				case "get":
					return "$1\r\nv\r\n"
				default:
					return "+OK\r\n"
				}
			})
		}
	}()

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		ClientSideCache: cache,
	})
	t.Cleanup(func() {
		_ = client.Close()
		_ = ln.Close()
	})

	ctx := context.Background()
	if got, getErr := client.Get(ctx, "k").Result(); getErr != nil || got != "v" {
		t.Fatalf("GET: got %q, %v; want %q, nil", got, getErr, "v")
	}
	if cache.Len() != 1 {
		t.Fatalf("GET did not populate CSC: Len=%d", cache.Len())
	}
	if _, fetchErr := client.baseClient.fetchCommandMetadata(ctx); fetchErr != nil {
		t.Fatalf("fetchCommandMetadata: %v", fetchErr)
	}
	if cache.Len() != 1 {
		t.Fatalf("metadata fetch evicted an unchanged CSC entry: Len=%d", cache.Len())
	}
}

func TestCommandMetadataServerChangeRefreshesLiveView(t *testing.T) {
	keyed := func(name string) *CommandInfo {
		return &CommandInfo{
			Name: name, Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		}
	}
	var phase atomic.Int32
	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			calls.Add(1)
			if phase.Load() == 0 {
				return testCommandMetadataFetchResultFor(
					testTrustedLiveRecords(map[string]*CommandInfo{"srva.get": keyed("srva.get")}),
					"8.10.0", "8.10.0|srvA",
				), nil
			}
			return testCommandMetadataFetchResultFor(
				testTrustedLiveRecords(map[string]*CommandInfo{"srvb.get": keyed("srvb.get")}),
				"8.11.0", "8.11.0|srvB",
			), nil
		})
	defer s.stopAndJoin()

	s.onServerHello("8.10.0|srvA")
	if !waitForCondition(t, 5*time.Second, func() bool {
		return s.view().live && isCacheableInView(s.view(), makeCmd("srva.get", "k"))
	}) {
		t.Fatal("first server's live view never published")
	}
	// The same identity must not refetch.
	settled := calls.Load()
	s.onServerHello("8.10.0|srvA")
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != settled {
		t.Errorf("unchanged server identity refetched: %d -> %d", settled, calls.Load())
	}
	// A changed identity must retire and replace the live view.
	phase.Store(1)
	s.onServerHello("8.11.0|srvB")
	if !waitForCondition(t, 5*time.Second, func() bool {
		v := s.view()
		return v.live && isCacheableInView(v, makeCmd("srvb.get", "k")) &&
			!isCacheableInView(v, makeCmd("srva.get", "k"))
	}) {
		t.Fatal("server change did not refresh the live view")
	}
}

func TestCommandMetadataServerUpgradeEnablesLiveOnlyCSC(t *testing.T) {
	keyed := &CommandInfo{
		Name: "myext.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
	var upgraded atomic.Bool
	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			calls.Add(1)
			version := "7.4.0"
			if upgraded.Load() {
				version = "8.10.0"
			}
			return testCommandMetadataFetchResultFor(
				map[string]*CommandInfo{"myext.get": keyed}, version, version,
			), nil
		})
	defer s.stopAndJoin()

	s.onServerHello("7.4.0")
	if !waitForCondition(t, 5*time.Second, func() bool {
		return s.view().live && !isCacheableInView(s.view(), makeCmd("myext.get", "k"))
	}) {
		t.Fatal("pre-8.10 live view was not published fail-closed")
	}
	// Connection churn must not refetch the same identity.
	settled := calls.Load()
	s.onConnInit()
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != settled {
		t.Errorf("live store refetched without a server change: %d -> %d", settled, calls.Load())
	}
	// An upgrade can enable a live-only command after refresh.
	upgraded.Store(true)
	s.onServerHello("8.10.0")
	if !waitForCondition(t, 5*time.Second, func() bool {
		return s.view().live && isCacheableInView(s.view(), makeCmd("myext.get", "k"))
	}) {
		t.Fatal("upgrade did not enable the live-only CSC record")
	}
}

func TestCommandMetadataStraddledFetchNotPublished(t *testing.T) {
	// An old-server fetch must not publish after an identity change.
	keyed := &CommandInfo{
		Name: "old.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var phase atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			if phase.Load() == 0 {
				started <- struct{}{}
				<-release
				return testCommandMetadataFetchResultFor(
					testTrustedLiveRecords(map[string]*CommandInfo{"old.get": keyed}),
					"8.10.0", "srvOld",
				), nil
			}
			return testCommandMetadataFetchResultFor(
				testTrustedLiveRecords(nil), "8.10.0", "srvNew",
			), nil
		})
	defer s.stopAndJoin()

	s.onServerHello("srvOld")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first fetch never started")
	}
	phase.Store(1)
	s.onServerHello("srvNew") // identity changes while fetch #1 is in flight
	close(release)            // fetch #1 completes with the OLD server's data

	if !waitForCondition(t, 5*time.Second, func() bool { return s.view().live }) {
		t.Fatal("new server's view never published")
	}
	if isCacheableInView(s.view(), makeCmd("old.get", "k")) {
		t.Fatal("straddled fetch published the old server's metadata")
	}
}

func TestCommandMetadataPublishRechecksServerFingerprint(t *testing.T) {
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive}, nil)
	defer s.stopAndJoin()
	s.mu.Lock()
	s.serverFp = "srvNew"
	s.mu.Unlock()

	view := buildCommandMetadataView(testTrustedLiveRecords(nil), nil)
	view.live = true
	if s.publishLiveView("srvOld", 0, "srvOld", view) {
		t.Fatal("publish succeeded for a stale server fingerprint")
	}
	if s.view().live {
		t.Fatal("metadata fetched for an old fingerprint was published")
	}
}

func TestCommandMetadataPublishRejectsInvalidationABA(t *testing.T) {
	s := newCommandMetadataStoreForLive(nil, nil)
	defer s.stopAndJoin()

	fp, epoch := s.serverIdentity()
	s.invalidateLiveAndRequestRefresh()

	view := buildCommandMetadataView(testTrustedLiveRecords(nil), nil)
	view.live = true
	if s.publishLiveView(fp, epoch, "srvOld", view) {
		t.Fatal("publish succeeded after an invalidate/reset fingerprint ABA")
	}
	if s.view().live {
		t.Fatal("metadata fetched before invalidation was published")
	}
}

func TestCommandMetadataPublishRejectsMissingIdentity(t *testing.T) {
	s := newCommandMetadataStoreForLive(nil, nil)
	defer s.stopAndJoin()

	fp, epoch := s.serverIdentity()
	view := buildCommandMetadataView(testTrustedLiveRecords(nil), nil)
	view.live = true
	if s.publishLiveView(fp, epoch, "", view) {
		t.Fatal("metadata without a server identity was published")
	}
	if s.view().live {
		t.Fatal("missing identity replaced the static view")
	}
}

func TestCSCPre810CompatibilityCorrectionKeepsSnapshotNegatives(t *testing.T) {
	// Pre-8.10 exclusions belong in the shared record, not only the CSC table.
	live := testTrustedLiveRecords(map[string]*CommandInfo{
		"ts.info": {
			Name: "ts.info", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
		"eval_ro": {
			Name: "eval_ro", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
		"ttl": {
			Name: "ttl", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
	})
	redis810 := buildCommandMetadataViewForServer(live, nil, "8.10.0")
	if !isCacheableInView(redis810, makeCmd("ts.info", "k")) {
		t.Error("Redis 8.10+ live record did not remain authoritative")
	}
	view := buildCommandMetadataViewForServer(live, nil, "7.4.0")
	if isCacheableInView(view, makeCmd("ts.info", "k")) {
		t.Error("pre-8.10 live record cleared the snapshot's dont_cache signal")
	}
	if !commandRecordHas(view.records["ts.info"], "dont_cache", true) {
		t.Error("pre-8.10 compatibility correction was not written to the shared record")
	}
	if !commandRecordHas(view.records["eval_ro"], "script_runner", false) {
		t.Error("pre-8.10 script_runner correction was not written to the shared record")
	}
	if !commandRecordHas(view.records["ttl"], "nondeterministic_output", true) {
		t.Error("pre-8.10 correction did not retain the snapshot's nondeterministic signal")
	}
	if isCacheableInView(view, makeCmd("ttl", "k")) {
		t.Error("pre-8.10 live TTL record lost its snapshot exclusion")
	}
	if got, want := view.cscTable["ts.info"], cscDeriveMeta(view.records["ts.info"]); got != want {
		t.Fatalf("CSC metadata was not derived solely from the resolved record: got %+v, want %+v", got, want)
	}
	if !commandRecordHas(commandInfoSnapshot["ts.info"], "dont_cache", true) {
		t.Fatal("test premise: snapshot ts.info must carry dont_cache")
	}
	// Application overrides may replace the correction.
	view = buildCommandMetadataView(nil, map[string]*CommandInfo{"ts.info": live["ts.info"]})
	if !isCacheableInView(view, makeCmd("ts.info", "k")) {
		t.Error("an explicit application override must not be clamped")
	}
}

func TestCSCDeriveMetaRejectsOverflowingKeynum(t *testing.T) {
	// Overflow must not wrap onto a valid position.
	info := &CommandInfo{
		Name:  "evil",
		Flags: []string{"readonly"},
		KeySpecs: []KeySpec{{
			BeginSearch: "index", Index: 32760, FindKeys: "keynum",
			KeyNumIdx: 10, FirstKey: 11, KeyStep: 1,
		}},
	}
	if m := cscDeriveMeta(info); m.extract != cscKeyExtractNone {
		t.Errorf("overflowing keynum positions must derive no extraction, got %+v", m)
	}

	// Invalid offsets must not cancel into a plausible position.
	info.KeySpecs[0] = KeySpec{
		BeginSearch: "index", Index: math.MaxInt, FindKeys: "keynum",
		KeyNumIdx: 1 - math.MaxInt, FirstKey: 2 - math.MaxInt, KeyStep: 1,
	}
	if m := cscDeriveMeta(info); m.extract != cscKeyExtractNone {
		t.Errorf("canceling malformed keynum positions must derive no extraction, got %+v", m)
	}
}

func TestCommandsInfoMalformedKeyPositionsFailClosed(t *testing.T) {
	// firstkey 257 must not wrap into int8 position 1.
	raw := "*2\r\n" +
		"*6\r\n$3\r\nbad\r\n:-1\r\n*1\r\n$8\r\nreadonly\r\n:257\r\n:257\r\n:1\r\n" +
		"*6\r\n$4\r\ngood\r\n:2\r\n*1\r\n$8\r\nreadonly\r\n:1\r\n:1\r\n:1\r\n"
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	bad, exists := cmd.Val()["bad"]
	if !exists || bad != nil {
		t.Errorf("out-of-range key positions must tombstone the record, got %+v (exists=%v)", bad, exists)
	}
	if good := cmd.Val()["good"]; good == nil || good.FirstKeyPos != 1 || good.StepCount != 1 {
		t.Errorf("in-range key positions must parse, got %+v", good)
	}

	badArity := "*2\r\n" +
		"*6\r\n$3\r\nbad\r\n:128\r\n*1\r\n$8\r\nreadonly\r\n:1\r\n:1\r\n:1\r\n" +
		"*6\r\n$4\r\ngood\r\n:2\r\n*1\r\n$8\r\nreadonly\r\n:1\r\n:1\r\n:1\r\n"
	cmd = NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(badArity))); err != nil {
		t.Fatalf("out-of-range command arity aborted the reply: %v", err)
	}
	if bad, exists := cmd.Val()["bad"]; !exists || bad != nil {
		t.Fatalf("out-of-range command arity must tombstone the record, got %+v (exists=%v)", bad, exists)
	}
	if cmd.Val()["good"] == nil {
		t.Fatal("out-of-range command arity discarded the following valid record")
	}

	overflowKeySpec := "*2\r\n$4\r\nspec\r\n" +
		"*2\r\n$5\r\nindex\r\n:2147483648\r\n"
	if ok, err := readKeySpecSectionChecked(
		proto.NewReader(strings.NewReader(overflowKeySpec)), &KeySpec{}, true,
	); err != nil || ok {
		t.Fatal("out-of-range key-spec position must fail instead of narrowing")
	}
}

func TestCommandsInfoRejectsExcessiveSubcommandDepth(t *testing.T) {
	entry := "*10\r\n$3\r\ncmd\r\n:-1\r\n*0\r\n:0\r\n:0\r\n:0\r\n" +
		"*0\r\n*0\r\n*0\r\n*0\r\n"
	for range maxCommandInfoDepth + 1 {
		entry = "*10\r\n$3\r\ncmd\r\n:-1\r\n*0\r\n:0\r\n:0\r\n:0\r\n" +
			"*0\r\n*0\r\n*0\r\n*1\r\n" + entry
	}

	cmd := NewCommandsInfoCmd(context.Background(), "command")
	err := cmd.readReply(proto.NewReader(strings.NewReader("*1\r\n" + entry)))
	if err == nil || !strings.Contains(err.Error(), "maximum depth") {
		t.Fatalf("deeply nested subcommands returned %v, want maximum-depth error", err)
	}
}

func TestHelloServerFingerprint(t *testing.T) {
	fp := helloServerFingerprint(map[string]interface{}{
		"version": "8.10.0",
		"modules": []interface{}{
			map[interface{}]interface{}{"name": "timeseries", "ver": int64(81000)},
			map[string]interface{}{"name": "bf", "ver": int64(81000)},
		},
	})
	if fp != "8.10.0|bf:81000|timeseries:81000" {
		t.Errorf("fingerprint = %q", fp)
	}
	if helloServerFingerprint(map[string]interface{}{}) != "" {
		t.Error("empty reply must produce an empty fingerprint")
	}
}

func TestCommandMetadataRetryCapStopsSelfRetry(t *testing.T) {
	oldMin, oldCap := cmdMetaBackoffMin, cmdMetaRetryCap
	cmdMetaBackoffMin, cmdMetaRetryCap = time.Millisecond, 3
	defer func() { cmdMetaBackoffMin, cmdMetaRetryCap = oldMin, oldCap }()

	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			calls.Add(1)
			return commandMetadataFetchResult{}, errors.New("NOPERM this user has no permissions to run the 'command' command")
		})
	defer s.stopAndJoin()

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool { return calls.Load() >= 3 }) {
		t.Fatal("retries never ran")
	}
	settled := calls.Load()
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != settled {
		t.Errorf("worker kept self-retrying past the cap: %d -> %d", settled, calls.Load())
	}
	// An external trigger permits one fresh attempt.
	s.onConnInit()
	if !waitForCondition(t, 2*time.Second, func() bool { return calls.Load() == settled+1 }) {
		t.Errorf("external trigger after the cap did not attempt: %d -> %d", settled, calls.Load())
	}
}

func TestCommandMetadataViewChangeCancelsFulfill(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	s := newCommandMetadataStore(&CommandMetadataConfig{
		Overrides: map[string]*CommandInfo{"get": nil},
	}, nil)
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cmdMeta: s}

	view := c.metadataView()
	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	// Change the decision during the fetch.
	s.current.Store(buildCommandMetadataView(nil, nil))
	if c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v")}, view) {
		t.Fatal("a fetch from a retired metadata generation must not publish")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("retired-generation entry must not be cached")
	}

	// An unchanged refresh must not cancel fulfillment.
	view = c.metadataView()
	tok, sf = cache.Reserve("get:k2", []string{"k2"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	s.current.Store(buildCommandMetadataView(nil, nil)) // same decisions, new pointer
	if !c.fulfillCached("get:k2", tok, &cscFetchCapture{raw: []byte("v")}, view) {
		t.Fatal("an equivalent refresh must not suppress publication")
	}
}

// TestCommandMetadataPreferLiveE2E runs the dynamic path against Redis.
func TestCommandMetadataPreferLiveE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a running Redis server")
	}
	addr := "localhost:6379"
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		addr = v
	} else if p := os.Getenv("REDIS_PORT"); p != "" {
		addr = "localhost:" + p
	}

	cache := NewLocalCache(CacheConfig{MaxEntries: 32})
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        3,
		ClientSideCache: cache,
		CommandMetadata: &CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		PoolSize:        1,
		MaxRetries:      -1,
	})
	defer client.Close()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no server at %s: %v", addr, err)
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatalf("INFO server: %v", err)
	}
	version := ""
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			version = v
			break
		}
	}
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		t.Fatalf("could not parse Redis version from INFO server: %q", version)
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil {
		t.Fatalf("could not parse Redis version from INFO server: %q", version)
	}
	if major < 8 || major == 8 && minor < 10 {
		t.Skipf("live command metadata requires Redis 8.10 or newer (server is %s)", version)
	}

	if client.baseClient.cmdMeta == nil {
		t.Fatal("PreferLive client must carry a metadata store")
	}
	if !waitForCondition(t, 10*time.Second, func() bool { return client.baseClient.metadataView().live }) {
		t.Fatal("live view never published against a real 8.10 server")
	}
	live := client.baseClient.metadataView()

	// The live view must reproduce normative decisions.
	for cmd, want := range map[Cmder]bool{
		makeCmd("get", "k"):             true,
		makeCmd("mget", "a", "b"):       true,
		makeCmd("touch", "k"):           false,
		makeCmd("json.mget", "a", "$"):  false,
		makeCmd("memory", "usage", "k"): false,
		makeCmd("blpop", "k", "0"):      false,
	} {
		if got := isCacheableInView(live, cmd); got != want {
			t.Errorf("live view: isCacheable(%v) = %v, want %v", cmd.Args(), got, want)
		}
	}

	// Caching must work with the live fingerprint.
	mutator := NewClient(&Options{Addr: addr})
	defer mutator.Close()
	key := "cmdmeta:e2e:k"
	if err := mutator.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	defer mutator.Del(ctx, key)

	deadline := time.Now().Add(3 * time.Second)
	for cache.Len() < 1 && time.Now().Before(deadline) {
		if err := client.Get(ctx, key).Err(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if cache.Len() < 1 {
		t.Fatal("entry never cached under the live view")
	}

	if err := mutator.Set(ctx, key, "v2", 0).Err(); err != nil {
		t.Fatal(err)
	}
	fresh := waitForCondition(t, 5*time.Second, func() bool {
		v, err := client.Get(ctx, key).Result()
		return err == nil && v == "v2"
	})
	if !fresh {
		t.Fatal("invalidation did not reach the fingerprinted entry")
	}
}
