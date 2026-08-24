package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
)

// Per-client command-metadata resolution. A client resolves COMMAND metadata
// records into an immutable commandMetadataView (app overrides > built-in
// corrections > live COMMAND output > shipped snapshot) and publishes it
// through an atomic pointer. Every command invocation reads exactly one view.
// In PreferLive mode a background worker upgrades the static view with the
// server's live COMMAND output.

// CommandMetadataMode selects how a client resolves the COMMAND metadata that
// drives client-side-cache eligibility.
//
// Experimental: this API may change in a minor release.
type CommandMetadataMode int

const (
	// CommandMetadataStatic (default) resolves metadata from the shipped
	// snapshot and configured overrides only: no network traffic, no
	// goroutines.
	CommandMetadataStatic CommandMetadataMode = iota

	// CommandMetadataPreferLive starts from the static tables and upgrades
	// them in the background with the server's live COMMAND output after the
	// first successful connection. Live output is accepted only from servers
	// that expose the Redis >= 8.10 eligibility signals (a version check, not
	// an integrity check); older servers stay on static metadata.
	//
	// Security: live metadata is authenticated only by the transport. A
	// compromised server or man-in-the-middle can shape caching decisions —
	// including serving repeated commands from the local cache instead of
	// sending them — and a poisoned view persists until the next refresh.
	// Use TLS when enabling this mode. Commands the shipped snapshot knows as
	// non-readonly can never be made cacheable by live metadata (only by an
	// explicit application override).
	CommandMetadataPreferLive
)

// CommandMetadataConfig configures per-client command-metadata resolution,
// set via Options.CommandMetadata. The zero value (or a nil config) is the
// static default.
//
// Experimental: this API may change in a minor release.
type CommandMetadataConfig struct {
	Mode CommandMetadataMode

	// Overrides take precedence over every other metadata source. Keys are
	// lowercase command names, "parent|child" for subcommands. A nil record
	// marks the command unknown: it is never cached. A record that makes a
	// command look cacheable can serve stale data if the server never
	// invalidates it. The map and its records are deep-copied at client
	// creation; later mutations have no effect.
	Overrides map[string]*CommandInfo

	// RefreshInterval re-fetches live metadata periodically in PreferLive
	// mode (jittered). Zero means the live upgrade happens once, with no
	// periodic refresh.
	RefreshInterval time.Duration
}

// commandMetadataView is one immutable resolution of command metadata.
type commandMetadataView struct {
	// records holds the resolved CommandInfo per lowercase name, kept for
	// consumers beyond CSC (e.g. future routing convergence).
	records map[string]*CommandInfo

	cscTable   map[string]cscCommandMeta
	cscParents map[string]struct{}

	// cscFingerprint identifies the eligibility decisions. It is part of
	// every cache-entry key — never of the Redis-key invalidation index — so
	// entries cached under different metadata never mix, while invalidations
	// keep reaching every generation.
	cscFingerprint string

	// live is true when the view was built from trusted live COMMAND output.
	live bool
}

// defaultCommandMetadataView is shared by every client without overrides or
// live mode.
var defaultCommandMetadataView = buildCommandMetadataView(nil, nil)

// cscSnapshotNonReadonly lists snapshot commands without the readonly flag.
// Live metadata can never make them cacheable (see the floor in
// buildCommandMetadataView).
var cscSnapshotNonReadonly = func() map[string]struct{} {
	floor := make(map[string]struct{})
	for name, info := range commandInfoSnapshot {
		if !commandRecordHas(info, "readonly", false) {
			floor[name] = struct{}{}
		}
	}
	return floor
}()

// cscSnapshotNegativeBits holds the negative eligibility bits the snapshot
// asserts per command. A live record cannot clear them: a server passing the
// core trust canaries can still run an older module whose records predate
// their dont_cache/nondeterministic tips (e.g. TS.INFO), and dropping the
// snapshot's negative signal would make such commands cacheable.
var cscSnapshotNegativeBits = func() map[string]cscCmdBits {
	m := make(map[string]cscCmdBits)
	for name, info := range commandInfoSnapshot {
		if bits := cscDeriveMeta(info).bits & cscNegativeBits; bits != 0 {
			m[name] = bits
		}
	}
	return m
}()

// buildCommandMetadataView resolves records and derives the CSC lookup
// tables. live and overrides may be nil.
func buildCommandMetadataView(live, overrides map[string]*CommandInfo) *commandMetadataView {
	records := make(map[string]*CommandInfo, len(commandInfoSnapshot)+len(overrides))
	for name, info := range commandInfoSnapshot {
		records[name] = info
	}
	var fromLive map[string]struct{}
	if len(live) > 0 {
		fromLive = make(map[string]struct{}, len(live))
	}
	for name, info := range live {
		if info != nil {
			lower := internal.ToLower(name)
			records[lower] = info
			fromLive[lower] = struct{}{}
		}
	}
	// Built-in corrections are additive: extra tips on top of the resolved
	// record, so a dont_cache correction never discards the record's flags,
	// key specs, or routing tips.
	for name, tips := range cscMetadataCorrections {
		rec := &CommandInfo{Name: name, Tips: tips}
		if base := records[name]; base != nil {
			cp := *base
			cp.Tips = append(append([]string(nil), base.Tips...), tips...)
			rec = &cp
		}
		records[name] = rec
	}
	for name, info := range overrides {
		lower := internal.ToLower(name)
		// An explicit application override is not subject to the live floor.
		delete(fromLive, lower)
		if info == nil {
			// nil means "unknown": fail closed, don't expose lower layers.
			delete(records, lower)
			continue
		}
		records[lower] = cloneCommandInfo(info)
	}

	table := make(map[string]cscCommandMeta, len(records))
	for name, info := range records {
		table[name] = cscDeriveMeta(info)
	}
	// Safety clamps on live-sourced entries (approved deviation from the pure
	// shared-record model: records stays truthful for other consumers, the
	// clamps apply only to the caching table; explicit application overrides
	// are exempt):
	//  - floor: a live record cannot make a command cacheable that the
	//    snapshot knows as non-readonly (a compromised server could otherwise
	//    flip a write into the cached path and suppress repeats of it);
	//  - sticky negatives: a live record cannot clear the snapshot's
	//    dont_cache/nondeterministic signals (older modules predate them).
	for name := range fromLive {
		meta, ok := table[name]
		if !ok {
			continue
		}
		if _, nonRO := cscSnapshotNonReadonly[name]; nonRO {
			meta.bits &^= cscFlagReadonly
		}
		meta.bits |= cscSnapshotNegativeBits[name]
		table[name] = meta
	}
	// Bare parents with subcommand entries are dropped: a leftover "xinfo"
	// would shadow "xinfo|stream" on direct lookup.
	for name := range table {
		if i := strings.IndexByte(name, '|'); i > 0 {
			delete(table, name[:i])
		}
	}
	parents := make(map[string]struct{})
	for name := range table {
		if i := strings.IndexByte(name, '|'); i > 0 {
			parents[name[:i]] = struct{}{}
		}
	}
	return &commandMetadataView{
		records:        records,
		cscTable:       table,
		cscParents:     parents,
		cscFingerprint: cscTableFingerprint(table),
	}
}

// cloneCommandInfo deep-copies a record so the copy can be mutated or
// published independently of the source.
func cloneCommandInfo(info *CommandInfo) *CommandInfo {
	cp := *info
	cp.Flags = append([]string(nil), info.Flags...)
	cp.ACLFlags = append([]string(nil), info.ACLFlags...)
	cp.Tips = append([]string(nil), info.Tips...)
	if info.KeySpecs != nil {
		cp.KeySpecs = make([]KeySpec, len(info.KeySpecs))
		for i, ks := range info.KeySpecs {
			cp.KeySpecs[i] = ks
			cp.KeySpecs[i].Flags = append([]string(nil), ks.Flags...)
		}
	}
	return &cp
}

// cscTableFingerprint hashes the derived eligibility table into a short
// stable token. Cryptographic (truncated SHA-256): in PreferLive mode the
// input is server-influenced, and a forged collision with an honest view's
// fingerprint would let a poisoned generation's entries survive a refresh.
func cscTableFingerprint(table map[string]cscCommandMeta) string {
	names := make([]string, 0, len(table))
	for name := range table {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, name := range names {
		m := table[name]
		fmt.Fprintf(h, "%s\x00%d %d %d %d %d %d\n",
			name, m.bits, m.extract, m.firstKey, m.lastKey, m.step, m.numkeysAt)
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// commandMetadataStore publishes metadata views for one client. It exists
// only when the client's config differs from the shared static default. The
// pointer is shared with clones; the worker is owned (started, stopped) by
// the client that attached it.
type commandMetadataStore struct {
	current atomic.Pointer[commandMetadataView]

	mode            CommandMetadataMode
	overrides       map[string]*CommandInfo
	refreshInterval time.Duration
	fetch           func(context.Context) (map[string]*CommandInfo, error)

	// static is the view built without live input; published initially and
	// again if a later refresh finds the endpoint downgraded.
	static *commandMetadataView

	// untrusted is set when the server's COMMAND output lacks the >= 8.10
	// eligibility signals; connection churn then stops re-requesting.
	untrusted atomic.Bool

	// fetchCtx is cancelled by signalStop so Close never waits out an
	// in-flight fetch's own timeout.
	fetchCtx    context.Context
	fetchCancel context.CancelFunc

	mu       sync.Mutex
	started  bool
	refresh  chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	// serverFp (guarded by mu) identifies the server the client last spoke
	// to (HELLO version + modules). A change retires a live view and forces
	// a refresh; refreshOnce discards a fetch that straddled a change.
	serverFp string
}

// newCommandMetadataStore returns nil for configs equivalent to the static
// default, letting those clients share defaultCommandMetadataView with zero
// allocations.
func newCommandMetadataStore(
	cfg *CommandMetadataConfig,
	fetch func(context.Context) (map[string]*CommandInfo, error),
) *commandMetadataStore {
	if cfg == nil || (cfg.Mode == CommandMetadataStatic && len(cfg.Overrides) == 0) {
		return nil
	}
	// Deep-copy the overrides: refreshOnce re-reads them on every rebuild,
	// and an application mutating its config map would race the worker.
	var overrides map[string]*CommandInfo
	if len(cfg.Overrides) > 0 {
		overrides = make(map[string]*CommandInfo, len(cfg.Overrides))
		for name, info := range cfg.Overrides {
			if info == nil {
				overrides[name] = nil
				continue
			}
			overrides[name] = cloneCommandInfo(info)
		}
	}
	s := &commandMetadataStore{
		mode:            cfg.Mode,
		overrides:       overrides,
		refreshInterval: cfg.RefreshInterval,
		fetch:           fetch,
		refresh:         make(chan struct{}, 1),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
	}
	s.fetchCtx, s.fetchCancel = context.WithCancel(context.Background())
	s.static = buildCommandMetadataView(nil, overrides)
	s.current.Store(s.static)
	// An override keyed by a bare container name ("memory") is pruned in
	// favor of its subcommand entries and would otherwise vanish silently.
	for name, info := range overrides {
		if info == nil {
			continue
		}
		if _, isParent := s.static.cscParents[name]; isParent {
			if _, inTable := s.static.cscTable[name]; !inTable {
				internal.Logger.Printf(context.Background(),
					"redis: CommandMetadata override %q targets a container command and has no effect; key it \"parent|child\"", name)
			}
		}
	}
	return s
}

func (s *commandMetadataStore) view() *commandMetadataView {
	return s.current.Load()
}

// onConnInit runs after each successful connection initialization: in
// PreferLive mode it schedules the live upgrade until one succeeds. Later
// drift (module loads, upgrades) is covered by RefreshInterval.
func (s *commandMetadataStore) onConnInit() {
	if s == nil || s.mode != CommandMetadataPreferLive || s.untrusted.Load() {
		return
	}
	if v := s.current.Load(); v != nil && v.live {
		return
	}
	s.requestRefresh()
}

// onServerHello runs after each successful HELLO with the server's identity
// (version + modules). An identity change — failover, maintenance handoff,
// upgrade, module load — retires a live view, clears the untrusted latch,
// and forces a fresh fetch; an unchanged identity behaves like onConnInit.
func (s *commandMetadataStore) onServerHello(fp string) {
	if s == nil || s.mode != CommandMetadataPreferLive {
		return
	}
	s.mu.Lock()
	changed := fp != s.serverFp
	if changed {
		s.serverFp = fp
		s.untrusted.Store(false)
		if v := s.current.Load(); v != nil && v.live {
			// Decide on static metadata until the new server's arrives.
			s.current.Store(s.static)
		}
	}
	s.mu.Unlock()
	if !changed {
		s.onConnInit()
		return
	}
	s.requestRefresh()
}

func (s *commandMetadataStore) serverFingerprint() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serverFp
}

// publishLiveView publishes only if the fetched server is still current.
// Holding mu across the recheck and store closes the failover race with
// onServerHello: either the old view lands first and is retired, or it never
// lands at all.
func (s *commandMetadataStore) publishLiveView(fp string, view *commandMetadataView) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverFp != fp {
		return false
	}
	s.untrusted.Store(false)
	s.current.Store(view)
	return true
}

// requestRefresh schedules one background refresh; pending requests coalesce.
func (s *commandMetadataStore) requestRefresh() {
	if s == nil || s.mode != CommandMetadataPreferLive || s.fetch == nil {
		return
	}
	s.startWorker()
	select {
	case s.refresh <- struct{}{}:
	default:
	}
}

func (s *commandMetadataStore) startWorker() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	select {
	case <-s.stop:
		return // already stopped; never start after Close
	default:
	}
	s.started = true
	go s.run()
}

// cmdMetaBackoffMin/Max bound the retry backoff after a failed refresh, and
// cmdMetaRetryCap bounds automatic retries (later external triggers —
// conn init, periodic refresh — still attempt once each). Vars so tests can
// shrink them.
var (
	cmdMetaBackoffMin = time.Second
	cmdMetaBackoffMax = 30 * time.Second
	cmdMetaRetryCap   = 5
)

const cmdMetaFetchTimeout = 30 * time.Second

func (s *commandMetadataStore) run() {
	defer close(s.done)
	var periodicC <-chan time.Time
	var periodic *time.Timer
	if s.refreshInterval > 0 {
		periodic = time.NewTimer(cmdMetaJitter(s.refreshInterval))
		defer periodic.Stop()
		periodicC = periodic.C
	}
	backoff := cmdMetaBackoffMin
	consecFail := 0
	attempt := func() {
		for {
			err := s.refreshOnce()
			if err == nil {
				backoff = cmdMetaBackoffMin
				consecFail = 0
				return
			}
			consecFail++
			// Damp the log for persistent failures (e.g. COMMAND denied by ACL).
			if consecFail <= cmdMetaRetryCap || consecFail%10 == 0 {
				internal.Logger.Printf(context.Background(),
					"redis: command metadata refresh failed (attempt %d): %v", consecFail, err)
			}
			if consecFail >= cmdMetaRetryCap {
				// Stop self-retrying; the next external trigger tries again.
				return
			}
			select {
			case <-s.stop:
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > cmdMetaBackoffMax {
				backoff = cmdMetaBackoffMax
			}
		}
	}
	for {
		select {
		case <-s.stop:
			return
		case <-periodicC:
			periodic.Reset(cmdMetaJitter(s.refreshInterval))
			attempt()
		case <-s.refresh:
			// Stop wins over a pending token, so Close never runs one more fetch.
			select {
			case <-s.stop:
				return
			default:
			}
			// Requests only drive the initial upgrade (a token queued by a
			// conn init during the first fetch would otherwise fetch twice);
			// drift after that is the periodic refresh's job.
			if v := s.current.Load(); v != nil && v.live {
				continue
			}
			attempt()
		}
	}
}

func (s *commandMetadataStore) refreshOnce() (err error) {
	fpStart := s.serverFingerprint()
	ctx, cancel := context.WithTimeout(s.fetchCtx, cmdMetaFetchTimeout)
	defer cancel()

	// The fetch runs in its own goroutine: the command path only honors ctx
	// deadlines with ContextTimeoutEnabled, and a stalled reply must not
	// block stop/join. An abandoned attempt ends when its connection does.
	type fetchResult struct {
		records map[string]*CommandInfo
		err     error
	}
	ch := make(chan fetchResult, 1)
	go func() {
		// A hostile or corrupt COMMAND reply must fail the refresh, not kill
		// the process from a goroutine the application cannot wrap.
		defer func() {
			if r := recover(); r != nil {
				ch <- fetchResult{err: fmt.Errorf("panic during command metadata refresh: %v", r)}
			}
		}()
		records, err := s.fetch(ctx)
		ch <- fetchResult{records: records, err: err}
	}()
	var records map[string]*CommandInfo
	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		records = res.records
	}
	if s.serverFingerprint() != fpStart {
		// The server changed under this fetch; onServerHello already queued
		// a fresh one. Don't publish (or distrust) the wrong server's data.
		return nil
	}
	if !commandMetadataTrusted(records) {
		// Pre-8.10 output lacks the negative signals eligibility depends on;
		// publishing it would make commands look safer than they are. Not an
		// error: the server will not change until restarted or upgraded, so
		// only a periodic refresh rechecks.
		s.mu.Lock()
		if s.serverFp != fpStart {
			s.mu.Unlock()
			return nil
		}
		firstUntrusted := s.untrusted.CompareAndSwap(false, true)
		if v := s.current.Load(); v != nil && v.live {
			// The endpoint downgraded (failover, LB swap): retire the previous
			// server's live view rather than keep deciding on its metadata.
			s.current.Store(s.static)
		}
		s.mu.Unlock()
		if firstUntrusted {
			internal.Logger.Printf(context.Background(),
				"redis: ignoring live command metadata: the server does not expose the Redis >= 8.10 eligibility signals")
		}
		return nil
	}
	view := buildCommandMetadataView(records, s.overrides)
	view.live = true
	s.publishLiveView(fpStart, view)
	return nil
}

// commandMetadataTrusted checks two signals introduced in Redis 8.10 —
// script_runner on EVAL_RO and the nondeterministic tip on TTL — whose
// absence means the output cannot carry the negative eligibility signals.
func commandMetadataTrusted(records map[string]*CommandInfo) bool {
	return commandRecordHas(records["eval_ro"], "script_runner", false) &&
		commandRecordHas(records["ttl"], "nondeterministic_output", true)
}

func commandRecordHas(info *CommandInfo, token string, inTips bool) bool {
	if info == nil {
		return false
	}
	list := info.Flags
	if inTips {
		list = info.Tips
	}
	for _, v := range list {
		if v == token {
			return true
		}
	}
	return false
}

// cmdMetaJitter spreads periodic refreshes by +-10% to avoid synchronized
// fetches across a fleet.
func cmdMetaJitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int63n(int64(d)/5+1)) - d/10
}

// signalStop makes the worker exit without joining; safe from a GC cleanup.
// It also cancels any in-flight fetch so a join never waits out the fetch
// timeout.
func (s *commandMetadataStore) signalStop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stop)
		s.fetchCancel()
	})
}

// stopAndJoin stops the worker and waits for it to exit. Called by the owning
// client's Close before the pools the fetch path uses are torn down; clones
// never call it.
func (s *commandMetadataStore) stopAndJoin() {
	if s == nil {
		return
	}
	s.signalStop()
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		<-s.done
	}
}

// helloServerFingerprint condenses a HELLO reply into a server-identity
// token (version plus sorted module name:ver pairs) for change detection.
func helloServerFingerprint(reply map[string]interface{}) string {
	var b strings.Builder
	if v, ok := reply["version"].(string); ok {
		b.WriteString(v)
	}
	if mods, ok := reply["modules"].([]interface{}); ok {
		names := make([]string, 0, len(mods))
		for _, m := range mods {
			switch mm := m.(type) {
			case map[interface{}]interface{}:
				names = append(names, fmt.Sprintf("%v:%v", mm["name"], mm["ver"]))
			case map[string]interface{}:
				names = append(names, fmt.Sprintf("%v:%v", mm["name"], mm["ver"]))
			}
		}
		sort.Strings(names)
		for _, n := range names {
			b.WriteString("|")
			b.WriteString(n)
		}
	}
	return b.String()
}

// metadataView returns the client's current command-metadata view; clients
// without a store share the static default.
func (c *baseClient) metadataView() *commandMetadataView {
	if s := c.cmdMeta; s != nil {
		return s.view()
	}
	return defaultCommandMetadataView
}

// fetchCommandMetadata retrieves live COMMAND output through the retry path,
// bypassing application hooks and the cache.
func (c *baseClient) fetchCommandMetadata(ctx context.Context) (map[string]*CommandInfo, error) {
	// Metadata refresh owns a bounded internal context independently of the
	// application's ContextTimeoutEnabled and socket-timeout choices. Run the
	// command through a lightweight clone whose zero socket timeouts defer to
	// that context deadline; otherwise ReadTimeout=-1 would leave a stalled
	// COMMAND holding a pooled connection after refreshOnce stopped waiting.
	fetchClient := c.withTimeout(0)
	fetchClient.hooksMixin = c.hooksMixin.clone()
	fetchClient.opt.ContextTimeoutEnabled = true

	// Pin HELLO and COMMAND to one physical connection. During a maintenance
	// handoff the parent pool can temporarily contain connections to both the
	// old and new endpoints; comparing a global HELLO fingerprint around an
	// arbitrary pooled COMMAND cannot prove which server supplied the reply.
	expectedFp := ""
	if c.cmdMeta != nil {
		expectedFp = c.cmdMeta.serverFingerprint()
	}
	sticky := c.newStickyConnPool()
	defer func() { _ = sticky.Close() }()
	fetchClient.connPool = sticky
	fetchClient.pipelinePool = nil
	// Initialization of the borrowed connection must not change the target
	// identity while this fetch is validating it; the parent connection init
	// already owns onServerHello notifications.
	fetchClient.cmdMeta = nil

	helloCmd := NewMapStringInterfaceCmd(ctx, "hello")
	infoCmd := NewCommandsInfoCmd(ctx, "command")
	if err := fetchClient.processPipeline(ctx, []Cmder{helloCmd, infoCmd}); err != nil {
		return nil, err
	}
	hello, err := helloCmd.Result()
	if err != nil {
		return nil, err
	}
	actualFp := helloServerFingerprint(hello)
	if expectedFp != "" && actualFp != expectedFp {
		return nil, fmt.Errorf("command metadata server changed: got %q, want %q", actualFp, expectedFp)
	}
	return infoCmd.Result()
}
