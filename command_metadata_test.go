package redis

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// testTrustedLiveRecords returns live COMMAND output carrying the >= 8.10
// trust canaries, plus any extra records.
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

func TestCommandMetadataStaticStoreNeverStartsWorker(t *testing.T) {
	s := newCommandMetadataStore(&CommandMetadataConfig{
		Overrides: map[string]*CommandInfo{"get": nil},
	}, func(context.Context) (map[string]*CommandInfo, error) {
		t.Error("static mode must never fetch")
		return nil, nil
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

func TestCommandMetadataPreferLivePublishes(t *testing.T) {
	live := testTrustedLiveRecords(map[string]*CommandInfo{
		"myext.get": {
			Name: "myext.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
	})
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (map[string]*CommandInfo, error) { return live, nil })
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
	// Snapshot fills what the live output does not mention; corrections stick.
	if !isCacheableInView(upgraded, makeCmd("get", "k")) {
		t.Error("snapshot GET must remain cacheable under the live view")
	}
	if isCacheableInView(upgraded, makeCmd("touch", "k")) {
		t.Error("built-in corrections must survive the live upgrade")
	}
	if upgraded.cscFingerprint == static.cscFingerprint {
		t.Error("a decision change must change the fingerprint")
	}
	// Once live, connection churn stops re-requesting.
	s.onConnInit()
	select {
	case <-s.refresh:
		t.Error("onConnInit must not re-request after a live view is published")
	default:
	}
}

func TestCommandMetadataUntrustedLiveRejected(t *testing.T) {
	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (map[string]*CommandInfo, error) {
			calls.Add(1)
			// No 8.10 canaries: e.g. a Redis 7.x server.
			return map[string]*CommandInfo{"get": {Name: "get", Flags: []string{"readonly"}}}, nil
		})
	defer s.stopAndJoin()

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool { return s.untrusted.Load() }) {
		t.Fatal("untrusted live output was never flagged")
	}
	if s.view().live {
		t.Fatal("untrusted output must not be published")
	}
	first := calls.Load()
	// Connection churn must not hammer an untrusted server.
	s.onConnInit()
	s.onConnInit()
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != first {
		t.Errorf("onConnInit re-fetched from an untrusted server: %d -> %d", first, calls.Load())
	}
}

func TestCommandMetadataFetchErrorRetries(t *testing.T) {
	oldMin := cmdMetaBackoffMin
	cmdMetaBackoffMin = time.Millisecond
	defer func() { cmdMetaBackoffMin = oldMin }()

	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (map[string]*CommandInfo, error) {
			if calls.Add(1) < 3 {
				return nil, errors.New("transient dial failure")
			}
			return testTrustedLiveRecords(nil), nil
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
	}, func(context.Context) (map[string]*CommandInfo, error) {
		switch calls.Add(1) {
		case 1:
			return testTrustedLiveRecords(nil), nil
		case 2:
			close(periodicFailed)
			return nil, errors.New("transient periodic refresh failure")
		default:
			return testTrustedLiveRecords(nil), nil
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

func TestCommandMetadataBareParentOverrideIsInert(t *testing.T) {
	// An override keyed "memory" is pruned by the bare-parent rule: the
	// subcommand entries keep resolving, and nothing becomes cacheable.
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

func TestCommandMetadataStopBeforeStart(t *testing.T) {
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (map[string]*CommandInfo, error) {
			t.Error("must not fetch after stop")
			return nil, nil
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
			func(context.Context) (map[string]*CommandInfo, error) {
				return testTrustedLiveRecords(nil), nil
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

func TestCommandMetadataLiveCannotFlipSnapshotWrites(t *testing.T) {
	// A malicious/buggy live record must not make a command cacheable that
	// the shipped snapshot knows as non-readonly (e.g. SET): the client would
	// serve repeats of a write from cache instead of sending them.
	flipped := &CommandInfo{
		Name: "set", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
	view := buildCommandMetadataView(testTrustedLiveRecords(map[string]*CommandInfo{"set": flipped}), nil)
	if isCacheableInView(view, makeCmd("set", "k", "v")) {
		t.Error("live metadata flipped snapshot-known write SET into the cached path")
	}
	// records stays truthful (the floor applies to the caching table only)...
	if !commandRecordHas(view.records["set"], "readonly", false) {
		t.Error("the floor must not rewrite the resolved record")
	}
	// ...and an explicit application override still wins (documented risk).
	view = buildCommandMetadataView(nil, map[string]*CommandInfo{"set": flipped})
	if !isCacheableInView(view, makeCmd("set", "k", "v")) {
		t.Error("an explicit application override must not be floored")
	}
}

func TestCommandMetadataUntrustedRefreshRevertsLiveView(t *testing.T) {
	// A periodic refresh reaching a downgraded endpoint (failover, LB swap)
	// must retire the previous server's live view.
	var trusted atomic.Bool
	trusted.Store(true)
	s := newCommandMetadataStore(&CommandMetadataConfig{
		Mode:            CommandMetadataPreferLive,
		RefreshInterval: 5 * time.Millisecond,
	}, func(context.Context) (map[string]*CommandInfo, error) {
		if trusted.Load() {
			return testTrustedLiveRecords(nil), nil
		}
		return map[string]*CommandInfo{"get": {Name: "get"}}, nil
	})
	defer s.stopAndJoin()

	s.onConnInit()
	if !waitForCondition(t, 5*time.Second, func() bool { return s.view().live }) {
		t.Fatal("live view never published")
	}
	trusted.Store(false)
	if !waitForCondition(t, 5*time.Second, func() bool { return !s.view().live }) {
		t.Fatal("live view was not retired after an untrusted refresh")
	}
	if s.view() != s.static {
		t.Error("the retired view must revert to the static view")
	}
}

func TestCommandMetadataStopCancelsInflightFetch(t *testing.T) {
	// Close must not wait out a hung fetch — even one that never observes
	// its context (the command path honors ctx deadlines only with
	// ContextTimeoutEnabled).
	release := make(chan struct{})
	defer close(release)
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (map[string]*CommandInfo, error) {
			<-release
			return nil, errors.New("released late")
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

func TestCommandMetadataFetchRejectsDifferentServer(t *testing.T) {
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
	if got := store.serverFingerprint(); got != "8.10.0-B" {
		t.Fatalf("metadata fetch changed the target server identity to %q", got)
	}

	store.onServerHello("8.10.0-A")
	records, err := client.baseClient.fetchCommandMetadata(ctx)
	if err != nil {
		t.Fatalf("metadata fetch from the target server failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("metadata fetch returned %d records, want 0", len(records))
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
		func(context.Context) (map[string]*CommandInfo, error) {
			calls.Add(1)
			if phase.Load() == 0 {
				return testTrustedLiveRecords(map[string]*CommandInfo{"srva.get": keyed("srva.get")}), nil
			}
			return testTrustedLiveRecords(map[string]*CommandInfo{"srvb.get": keyed("srvb.get")}), nil
		})
	defer s.stopAndJoin()

	s.onServerHello("8.10.0|srvA")
	if !waitForCondition(t, 5*time.Second, func() bool {
		return s.view().live && isCacheableInView(s.view(), makeCmd("srva.get", "k"))
	}) {
		t.Fatal("first server's live view never published")
	}
	// Same identity: connection churn must not refetch.
	settled := calls.Load()
	s.onServerHello("8.10.0|srvA")
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != settled {
		t.Errorf("unchanged server identity refetched: %d -> %d", settled, calls.Load())
	}
	// Changed identity (failover/upgrade): the old live view must be retired
	// and the new server's metadata fetched, with no periodic refresh set.
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

func TestCommandMetadataServerChangeRecoversFromUntrusted(t *testing.T) {
	var trusted atomic.Bool
	var calls atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (map[string]*CommandInfo, error) {
			calls.Add(1)
			if trusted.Load() {
				return testTrustedLiveRecords(nil), nil
			}
			return map[string]*CommandInfo{"get": {Name: "get"}}, nil
		})
	defer s.stopAndJoin()

	s.onServerHello("7.4.0")
	if !waitForCondition(t, 5*time.Second, func() bool { return s.untrusted.Load() }) {
		t.Fatal("untrusted server was never latched")
	}
	// The latch holds for the SAME server...
	trusted.Store(true)
	settled := calls.Load()
	s.onConnInit()
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != settled {
		t.Errorf("latched store refetched without a server change: %d -> %d", settled, calls.Load())
	}
	// ...but a server change (upgrade) recovers without periodic refresh.
	s.onServerHello("8.10.0")
	if !waitForCondition(t, 5*time.Second, func() bool { return s.view().live }) {
		t.Fatal("untrusted->trusted recovery never happened after the server change")
	}
}

func TestCommandMetadataStraddledFetchNotPublished(t *testing.T) {
	// A fetch that started against the old server must not publish (or
	// distrust) after the identity changed mid-flight.
	keyed := &CommandInfo{
		Name: "old.get", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
		KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
	}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var phase atomic.Int32
	s := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		func(context.Context) (map[string]*CommandInfo, error) {
			if phase.Load() == 0 {
				started <- struct{}{}
				<-release
				return testTrustedLiveRecords(map[string]*CommandInfo{"old.get": keyed}), nil
			}
			return testTrustedLiveRecords(nil), nil
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
	if s.publishLiveView("srvOld", view) {
		t.Fatal("publish succeeded for a stale server fingerprint")
	}
	if s.view().live {
		t.Fatal("metadata fetched for an old fingerprint was published")
	}
}

func TestCSCLiveCannotClearSnapshotNegatives(t *testing.T) {
	// An older module's live record may predate its dont_cache tip (e.g.
	// TS.INFO); the snapshot's negative signal must stick.
	live := testTrustedLiveRecords(map[string]*CommandInfo{
		"ts.info": {
			Name: "ts.info", Flags: []string{"readonly"}, FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1,
			KeySpecs: []KeySpec{{Flags: []string{"RO"}, BeginSearch: "index", Index: 1, FindKeys: "range", KeyStep: 1}},
		},
	})
	view := buildCommandMetadataView(live, nil)
	if isCacheableInView(view, makeCmd("ts.info", "k")) {
		t.Error("live record cleared the snapshot's dont_cache signal")
	}
	if !commandRecordHas(commandInfoSnapshot["ts.info"], "dont_cache", true) {
		t.Fatal("test premise: snapshot ts.info must carry dont_cache")
	}
	// An explicit application override remains exempt (documented risk).
	view = buildCommandMetadataView(nil, map[string]*CommandInfo{"ts.info": live["ts.info"]})
	if !isCacheableInView(view, makeCmd("ts.info", "k")) {
		t.Error("an explicit application override must not be clamped")
	}
}

func TestCSCDeriveMetaRejectsOverflowingKeynum(t *testing.T) {
	// Positions summing past int16 would wrap onto a different valid
	// position and pass the per-call bounds checks.
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

	// Individually nonsensical offsets must not be allowed to cancel into
	// plausible positions (MaxInt + (1-MaxInt) == 1).
	info.KeySpecs[0] = KeySpec{
		BeginSearch: "index", Index: math.MaxInt, FindKeys: "keynum",
		KeyNumIdx: 1 - math.MaxInt, FirstKey: 2 - math.MaxInt, KeyStep: 1,
	}
	if m := cscDeriveMeta(info); m.extract != cscKeyExtractNone {
		t.Errorf("canceling malformed keynum positions must derive no extraction, got %+v", m)
	}
}

func TestCommandsInfoMalformedKeyPositionsFailClosed(t *testing.T) {
	// firstkey 257 must not wrap into int8 position 1; the triple is zeroed
	// ("positions unknown"), which fails closed for eligibility.
	raw := "*2\r\n" +
		"*6\r\n$3\r\nbad\r\n:-1\r\n*1\r\n$8\r\nreadonly\r\n:257\r\n:257\r\n:1\r\n" +
		"*6\r\n$4\r\ngood\r\n:2\r\n*1\r\n$8\r\nreadonly\r\n:1\r\n:1\r\n:1\r\n"
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	bad := cmd.Val()["bad"]
	if bad == nil || bad.FirstKeyPos != 0 || bad.LastKeyPos != 0 || bad.StepCount != 0 {
		t.Errorf("out-of-range key positions must zero the triple, got %+v", bad)
	}
	if good := cmd.Val()["good"]; good == nil || good.FirstKeyPos != 1 || good.StepCount != 1 {
		t.Errorf("in-range key positions must parse, got %+v", good)
	}
	if cscIsClientSideCacheable(cscDeriveMeta(bad)) {
		t.Error("record with wrapped positions must not be cacheable")
	}

	badArity := "*1\r\n" +
		"*6\r\n$3\r\nbad\r\n:128\r\n*1\r\n$8\r\nreadonly\r\n:1\r\n:1\r\n:1\r\n"
	cmd = NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(badArity))); err == nil {
		t.Fatal("out-of-range command arity must fail instead of wrapping")
	}

	overflowKeySpec := "*2\r\n$4\r\nspec\r\n" +
		"*2\r\n$5\r\nindex\r\n:2147483648\r\n"
	if err := readKeySpecSection(
		proto.NewReader(strings.NewReader(overflowKeySpec)), &KeySpec{}, true,
	); err == nil {
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
		func(context.Context) (map[string]*CommandInfo, error) {
			calls.Add(1)
			return nil, errors.New("NOPERM this user has no permissions to run the 'command' command")
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
	// An external trigger still gets one fresh attempt.
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
	// A decision change lands while the fetch is in flight.
	s.current.Store(buildCommandMetadataView(nil, nil))
	if c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v")}, view) {
		t.Fatal("a fetch from a retired metadata generation must not publish")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("retired-generation entry must not be cached")
	}

	// A refresh that changed no decisions must NOT cancel the fulfill.
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

// TestCommandMetadataPreferLiveE2E runs the full dynamic path against a real
// server when one is available.
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

	// The live view must reproduce the normative decisions.
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

	// Caching still works end to end under the live fingerprint.
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
