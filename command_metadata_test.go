package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/routing"
)

func testReadOnlyCommandInfo(name string) *CommandInfo {
	return &CommandInfo{
		Name: name, Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
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
			return testCommandMetadataFetchResult(nil), nil
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
			return testCommandMetadataFetchResult(nil), nil
		case 2:
			close(periodicFailed)
			return commandMetadataFetchResult{}, errors.New("transient periodic refresh failure")
		default:
			return testCommandMetadataFetchResult(nil), nil
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

func TestCommandMetadataRefreshReusesUnchangedView(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*commandMetadataFetchResult)
	}{
		{name: "unchanged"},
		{name: "routing only", change: func(m *commandMetadataFetchResult) {
			m.records["dbsize"].Tips = []string{"request_policy:all_shards", "response_policy:agg_sum"}
		}},
		{name: "key spec", change: func(m *commandMetadataFetchResult) {
			m.records["get"].KeySpecs[0].Index = 2
		}},
		{name: "tombstone", change: func(m *commandMetadataFetchResult) {
			m.records["get"] = nil
		}},
		{name: "removed record", change: func(m *commandMetadataFetchResult) {
			delete(m.records, "get")
		}},
		{name: "legacy provenance", change: func(m *commandMetadataFetchResult) {
			m.legacyRecords = map[string]struct{}{"dbsize": {}}
		}},
		{name: "server version", change: func(m *commandMetadataFetchResult) {
			m.serverVersion = "7.4.0"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			s := newCommandMetadataStoreForLive(nil, func(context.Context) (commandMetadataFetchResult, error) {
				// Each fetch owns distinct records, as separately parsed replies do.
				m := testCommandMetadataFetchResult(map[string]*CommandInfo{
					"get":    testReadOnlyCommandInfo("get"),
					"dbsize": {Name: "dbsize", Arity: 1, Flags: []string{"readonly"}},
				})
				if calls > 0 && tc.change != nil {
					tc.change(&m)
				}
				calls++
				return m, nil
			})
			defer s.stopAndJoin()
			if err := s.ensureLive(context.Background()); err != nil {
				t.Fatal(err)
			}
			first := s.view()
			if err := s.refreshOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			second := s.view()
			if changed := second != first; changed != (tc.change != nil) {
				t.Fatalf("view changed = %v, want %v", changed, tc.change != nil)
			}
			if tc.name == "routing only" && first.cscFingerprint != second.cscFingerprint {
				t.Fatal("routing-only fixture unexpectedly changed CSC eligibility")
			}
			if err := s.refreshOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if s.view() != second {
				t.Fatal("identical refresh rebuilt the live view")
			}
		})
	}
}

func TestCommandMetadataUnchangedRefreshValidatesIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*commandMetadataStore, *commandMetadataFetchResult)
	}{
		{"missing identity", func(_ *commandMetadataStore, m *commandMetadataFetchResult) {
			m.serverFingerprint = ""
		}},
		{"different identity", func(_ *commandMetadataStore, m *commandMetadataFetchResult) {
			m.serverFingerprint = "other-server"
		}},
		{"invalidated during fetch", func(s *commandMetadataStore, _ *commandMetadataFetchResult) {
			s.invalidateLive()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var duringFetch func(*commandMetadataFetchResult)
			s := newCommandMetadataStoreForLive(nil, func(context.Context) (commandMetadataFetchResult, error) {
				m := testCommandMetadataFetchResult(map[string]*CommandInfo{"get": testReadOnlyCommandInfo("get")})
				if duringFetch != nil {
					duringFetch(&m)
				}
				return m, nil
			})
			defer s.stopAndJoin()
			if err := s.ensureLive(context.Background()); err != nil {
				t.Fatal(err)
			}
			duringFetch = func(m *commandMetadataFetchResult) { tc.change(s, m) }
			if err := s.refreshOnce(context.Background()); err == nil {
				t.Fatal("unchanged records bypassed identity validation")
			}
			previous := s.view()
			duringFetch = nil
			if err := s.refreshOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !s.view().live {
				t.Fatal("valid refresh did not restore live metadata")
			}
			if previous.live && s.view() != previous {
				t.Fatal("rejected fetch replaced the last successful comparison input")
			}
		})
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

func TestCommandMetadataLegacyRoutingPolicies(t *testing.T) {
	for _, fields := range []int{6, 7, 10} {
		t.Run(fmt.Sprint(fields), func(t *testing.T) {
			var entries []string
			for _, name := range []string{"dbsize", "flushall", "mget"} {
				info := commandInfoSnapshot[name]
				first, last, step := info.FirstKeyPos, info.LastKeyPos, info.StepCount
				if name == "mget" {
					first = 2 // Live positions must win over the snapshot's position 1.
				}
				entry := []string{
					commandInfoTestBulk(name), commandInfoTestInt(int64(info.Arity)),
					commandInfoTestArray(commandInfoTestBulk(info.Flags[0])),
					commandInfoTestInt(int64(first)), commandInfoTestInt(int64(last)), commandInfoTestInt(int64(step)),
				}
				for len(entry) < fields {
					entry = append(entry, commandInfoTestArray())
				}
				entries = append(entries, commandInfoTestArray(entry...))
			}
			parsed := commandInfoTestReadReply(t, commandInfoTestArray(entries...))
			view := buildCommandMetadataViewForServerWithLegacy(parsed.Val(), nil, "6.2.0", parsed.legacyRecords)
			for _, tc := range []struct {
				name     string
				request  routing.RequestPolicy
				response routing.ResponsePolicy
			}{
				{"dbsize", routing.ReqAllShards, routing.RespAggSum},
				{"flushall", routing.ReqAllShards, routing.RespAllSucceeded},
				{"mget", routing.ReqMultiShard, routing.RespDefaultHashSlot},
			} {
				policy := view.routingTable[tc.name].policy
				if fields == 10 {
					// Modern records explicitly supply tips, even when empty.
					if policy != nil && policy.Request != routing.ReqDefault {
						t.Errorf("%s: snapshot policy replaced modern live tips: %+v", tc.name, policy)
					}
				} else if policy == nil || policy.Request != tc.request || policy.Response != tc.response {
					t.Errorf("%s: policy=%+v, want %v/%v", tc.name, policy, tc.request, tc.response)
				}
			}
			plan, ok := routingResolveKeyPlan(view.routingTable["mget"], makeCmd("mget", "prefix", "{a}k", "{b}k"))
			if !ok || len(plan.positions) != 2 || plan.positions[0] != 2 || plan.positions[1] != 3 {
				t.Fatalf("live key plan=%+v, ok=%v", plan, ok)
			}
		})
	}
}

func TestCommandMetadataViewCopiesSourceRecords(t *testing.T) {
	for _, source := range []string{"live", "override"} {
		t.Run(source, func(t *testing.T) {
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
			if source == "override" {
				view = buildCommandMetadataView(nil, map[string]*CommandInfo{"module.read": live})
			}
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
		})
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

	keyedGet := testReadOnlyCommandInfo("myext.get")
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
		"container":       testReadOnlyCommandInfo("container"),
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

func TestCommandMetadataNormalizedCollisionsFailClosed(t *testing.T) {
	keyed := commandInfoSnapshot["get"]
	for _, tc := range []struct {
		name            string
		live, overrides map[string]*CommandInfo
	}{
		{"live tombstone", map[string]*CommandInfo{"GET": nil, "get": keyed}, nil},
		{"live records", map[string]*CommandInfo{"GET": keyed, "get": keyed}, nil},
		{"overrides", nil, map[string]*CommandInfo{"GET": keyed, "get": keyed}},
		{"override tombstone", nil, map[string]*CommandInfo{"GET": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := buildCommandMetadataView(tc.live, tc.overrides)
			if _, ok := view.records["get"]; ok {
				t.Fatal("ambiguous or tombstoned command exposed a record")
			}
			if _, ok := view.tombstones["get"]; !ok {
				t.Fatal("normalized tombstone was not preserved")
			}
			if isCacheableInView(view, makeCmd("get", "key")) {
				t.Fatal("ambiguous or tombstoned command enabled CSC")
			}
			if _, ok := routingLookupMeta(view, makeCmd("get", "key")); ok {
				t.Fatal("ambiguous or tombstoned command enabled routing")
			}
		})
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

func TestCommandMetadataConcurrentRefreshAndStop(t *testing.T) {
	for i := 0; i < 20; i++ {
		s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
			func(context.Context) (commandMetadataFetchResult, error) {
				return testCommandMetadataFetchResult(nil), nil
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

func TestCommandMetadataServerChangeRefreshesLiveView(t *testing.T) {
	var phase atomic.Int32
	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			calls.Add(1)
			if phase.Load() == 0 {
				return testCommandMetadataFetchResultFor(
					map[string]*CommandInfo{"srva.get": testReadOnlyCommandInfo("srva.get")},
					"8.10.0", "8.10.0|srvA",
				), nil
			}
			return testCommandMetadataFetchResultFor(
				map[string]*CommandInfo{"srvb.get": testReadOnlyCommandInfo("srvb.get")},
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
	keyed := testReadOnlyCommandInfo("myext.get")
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
	meta, ok := routingLookupMeta(s.view(), makeCmd("myext.get", "k"))
	if !ok || !meta.readOnly {
		t.Fatal("pre-8.10 record was lost to the routing consumer")
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
	keyed := testReadOnlyCommandInfo("old.get")
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var phase atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (commandMetadataFetchResult, error) {
			if phase.Load() == 0 {
				started <- struct{}{}
				<-release
				return testCommandMetadataFetchResultFor(
					map[string]*CommandInfo{"old.get": keyed},
					"8.10.0", "srvOld",
				), nil
			}
			return testCommandMetadataFetchResultFor(
				nil, "8.10.0", "srvNew",
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

func TestCSCPre810CompatibilityCorrectionKeepsSnapshotNegatives(t *testing.T) {
	// Pre-8.10 exclusions belong in the shared record, not only the CSC table.
	live := map[string]*CommandInfo{
		"ts.info": testReadOnlyCommandInfo("ts.info"),
		"eval_ro": testReadOnlyCommandInfo("eval_ro"),
		"ttl":     testReadOnlyCommandInfo("ttl"),
	}
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
			[]interface{}{"name", "json", "ver", int64(81000)}, // RESP2
		},
	})
	if fp != "8.10.0|bf:81000|json:81000|timeseries:81000" {
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
	for _, coalesced := range []bool{false, true} {
		t.Run(fmt.Sprintf("coalesced=%v", coalesced), func(t *testing.T) {
			cache := NewLocalCache(CacheConfig{MaxEntries: 16})
			s := newCommandMetadataStoreForLive(nil, nil)
			c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cmdMeta: s}
			view := c.metadataView()
			for _, changed := range []bool{true, false} {
				key := "get:k"
				token, fetch := cache.Reserve(key, []string{"k"})
				if !fetch {
					t.Fatal("Reserve should fetch")
				}
				var overrides map[string]*CommandInfo
				if changed {
					overrides = map[string]*CommandInfo{"get": nil}
				}
				// A fresh pointer with the same decisions still permits publication.
				s.current.Store(buildCommandMetadataView(nil, overrides))
				raw := []byte("$1\r\nv\r\n")
				if coalesced {
					cmd := NewStringCmd(context.Background(), "get", "k")
					req := &cscMissReq{cmd: cmd, cacheKey: key, token: token, view: view, done: make(chan error, 1)}
					mc := &cscMissCoalescer{c: c}
					mc.applyAndSettle(req, raw, 0, 0)
					if err := <-req.done; err != nil || cmd.Val() != "v" {
						t.Fatalf("fetched reply = %q, %v", cmd.Val(), err)
					}
				} else {
					c.fulfillCached(key, token, &cscFetchCapture{raw: raw}, view)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, cached := cache.Get(ctx, key)
				cancel()
				if cached == changed {
					t.Fatalf("changed=%v: cached=%v, want %v", changed, cached, !changed)
				}
			}
		})
	}
}

func TestCommandMetadataRefreshSkipsRetiredEntries(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	s := newCommandMetadataStore(&CommandMetadataConfig{Overrides: map[string]*CommandInfo{"get": nil}}, nil)
	pooler := &erroringPooler{}
	c := &baseClient{opt: &Options{}, csc: cache, cmdMeta: s, connPool: pooler, cscKeyPrefix: "p:"}
	rawKey, _ := buildCacheKey(makeCmd("get", "k"))
	key := cscEntryKey(c.cscKeyPrefix, defaultCommandMetadataView.cscFingerprint, rawKey)
	n, err := c.refreshInvalidatedBatch(context.Background(), []cscRefreshTarget{{cacheKey: key, redisKeys: []string{"p:k"}}})
	if err != nil || n != 0 || pooler.gets.Load() != 0 || cache.Len() != 0 {
		t.Fatalf("retired refresh: published=%d err=%v pool gets=%d entries=%d", n, err, pooler.gets.Load(), cache.Len())
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
	if version == "" {
		t.Fatal("INFO server did not report a version")
	}
	if !commandMetadataSupportsCSC(version) {
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

	// An unchanged metadata fetch must preserve already cached replies.
	entries := cache.Len()
	if _, err := client.baseClient.fetchCommandMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.Len() != entries {
		t.Fatal("unchanged metadata fetch evicted cached entries")
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
