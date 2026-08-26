package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
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

// CommandMetadataMode selects metadata resolution for CSC and Cluster routing.
//
// Experimental: this API may change in a minor release.
type CommandMetadataMode int

const (
	// CommandMetadataStatic uses the snapshot and overrides without network
	// calls or goroutines.
	CommandMetadataStatic CommandMetadataMode = iota

	// CommandMetadataPreferLive starts static and fetches live metadata after
	// the first connection. Pre-8.10 records get conservative CSC exclusions.
	//
	// Security: live metadata controls routing and caching. Use TLS because a
	// compromised source can misroute commands, serve stale data, or suppress a
	// write by falsely classifying it as cacheable. Redis 8.10+ records are
	// authoritative except for built-in corrections.
	CommandMetadataPreferLive
)

// CommandMetadataConfig configures metadata for Options and ClusterOptions.
// Its zero value is static.
//
// Experimental: this API may change in a minor release.
type CommandMetadataConfig struct {
	Mode CommandMetadataMode

	// Overrides take precedence over every other metadata source. Keys are
	// case-insensitive; use "parent|child" for subcommands. Nil marks a command
	// unknown. Unsafe cacheable overrides can serve stale data. The map and
	// records are copied at client creation.
	Overrides map[string]*CommandInfo

	// RefreshInterval periodically refreshes live metadata with jitter.
	// Zero fetches it only once.
	RefreshInterval time.Duration
}

// commandMetadataView is one immutable resolution of command metadata.
type commandMetadataView struct {
	// generation identifies this view across clients and refreshes.
	generation uint64

	// records holds resolved metadata by lowercase command name.
	records map[string]*CommandInfo

	// tombstones block fallback for malformed or explicitly disabled commands.
	tombstones map[string]struct{}

	// shadowedParents are bare records pruned to expose their subcommands.
	subcommandParents map[string]struct{}
	shadowedParents   map[string]struct{}

	// CSC and routing derive separate decisions from the shared records.
	cscTable     map[string]cscCommandMeta
	cscParents   map[string]struct{}
	routingTable map[string]routingCommandMeta

	// cscFingerprint identifies the eligibility decisions. It is part of
	// every cache-entry key — never of the Redis-key invalidation index — so
	// entries cached under different metadata never mix, while invalidations
	// keep reaching every generation.
	cscFingerprint string

	// live is true when the view was built from live COMMAND output.
	live          bool
	serverVersion string
}

// defaultCommandMetadataView is shared by every client without overrides or
// live mode.
var defaultCommandMetadataView = buildCommandMetadataView(nil, nil)

var commandMetadataGeneration atomic.Uint64

// commandMetadataCompatibilityCorrection adds CSC exclusions missing before
// Redis 8.10.
type commandMetadataCompatibilityCorrection struct {
	flags []string
	tips  []string
}

// commandMetadataPre810Corrections contains only pre-8.10 CSC exclusions.
// Redis 8.10+ metadata stays authoritative except for built-in corrections.
var commandMetadataPre810Corrections = func() map[string]commandMetadataCompatibilityCorrection {
	corrections := make(map[string]commandMetadataCompatibilityCorrection)
	for name, info := range commandInfoSnapshot {
		var correction commandMetadataCompatibilityCorrection
		if !commandRecordHas(info, "readonly", false) {
			// Do not let old metadata make a known write command cacheable.
			correction.tips = append(correction.tips, "dont_cache")
		}
		for _, flag := range info.Flags {
			switch flag {
			case "script_runner", "blocking":
				correction.flags = append(correction.flags, flag)
			}
		}
		for _, tip := range info.Tips {
			switch tip {
			case "dont_cache", "nondeterministic_output":
				correction.tips = append(correction.tips, tip)
			}
		}
		if len(correction.flags) > 0 || len(correction.tips) > 0 {
			corrections[name] = correction
		}
	}
	return corrections
}()

// buildCommandMetadataView builds a view, treating live input as Redis 8.10.
func buildCommandMetadataView(live, overrides map[string]*CommandInfo) *commandMetadataView {
	return buildCommandMetadataViewForServer(live, overrides, "8.10")
}

// buildCommandMetadataViewForServer resolves records and derived decisions.
func buildCommandMetadataViewForServer(
	live, overrides map[string]*CommandInfo,
	serverVersion string,
) *commandMetadataView {
	return buildCommandMetadataViewForServerWithLegacy(
		live,
		overrides,
		serverVersion,
		nil,
	)
}

func buildCommandMetadataViewForServerWithLegacy(
	live, overrides map[string]*CommandInfo,
	serverVersion string,
	liveLegacyRecords map[string]struct{},
) *commandMetadataView {
	records := make(map[string]*CommandInfo, len(commandInfoSnapshot)+len(overrides))
	tombstones := make(map[string]struct{})
	parents := make(map[string]struct{})
	markParent := func(name string) {
		for offset := 0; ; {
			i := strings.IndexByte(name[offset:], '|')
			if i < 0 {
				break
			}
			offset += i
			if offset > 0 {
				parents[name[:offset]] = struct{}{}
			}
			offset++
		}
	}
	for name, info := range commandInfoSnapshot {
		lower := internal.ToLower(name)
		records[lower] = cloneCommandInfoForName(lower, info)
		markParent(lower)
	}

	legacyLive := make(map[string]struct{}, len(liveLegacyRecords))
	for name := range liveLegacyRecords {
		legacyLive[internal.ToLower(name)] = struct{}{}
	}
	liveSupportsCSC := commandMetadataSupportsCSC(serverVersion)
	liveNames := make([]string, 0, len(live))
	for name := range live {
		liveNames = append(liveNames, name)
	}
	sort.Strings(liveNames)
	seenLive := make(map[string]struct{}, len(liveNames))
	for _, name := range liveNames {
		info := live[name]
		lower := internal.ToLower(name)
		// Tombstoned children still prevent their parent from shadowing them.
		markParent(lower)
		if _, duplicate := seenLive[lower]; duplicate {
			delete(records, lower)
			tombstones[lower] = struct{}{}
			continue
		}
		seenLive[lower] = struct{}{}
		if info == nil {
			// Live nil explicitly blocks snapshot fallback and corrections.
			delete(records, lower)
			tombstones[lower] = struct{}{}
			continue
		}
		_, legacy := legacyLive[lower]
		records[lower] = resolveLiveCommandMetadata(
			lower,
			info,
			liveSupportsCSC,
			legacy,
		)
	}
	// Apply built-in corrections before deriving CSC and routing decisions.
	for name, correction := range commandMetadataCorrections {
		markParent(name)
		if _, tombstoned := tombstones[name]; tombstoned {
			continue
		}
		rec := &CommandInfo{Name: name}
		if base := records[name]; base != nil {
			rec = cloneCommandInfo(base)
		}
		rec.Tips = appendCommandMetadataTokens(rec.Tips, correction.tips...)
		rec.Flags = removeCommandMetadataTokens(rec.Flags, correction.removeFlags...)
		if correction.keySpecs != nil {
			rec.KeySpecs = cloneCommandInfo(&CommandInfo{KeySpecs: correction.keySpecs}).KeySpecs
			rec.FirstKeyPos = correction.firstKeyPos
			rec.LastKeyPos = correction.lastKeyPos
			rec.StepCount = correction.stepCount
		}
		rec.ReadOnly = commandRecordHas(rec, "readonly", false)
		records[name] = rec
	}

	overrideNames := make([]string, 0, len(overrides))
	for name := range overrides {
		overrideNames = append(overrideNames, name)
	}
	sort.Strings(overrideNames)
	seenOverrides := make(map[string]struct{}, len(overrideNames))
	for _, name := range overrideNames {
		info := overrides[name]
		lower := internal.ToLower(name)
		markParent(lower)
		if _, duplicate := seenOverrides[lower]; duplicate {
			delete(records, lower)
			tombstones[lower] = struct{}{}
			continue
		}
		seenOverrides[lower] = struct{}{}
		if info == nil {
			// nil means "unknown": fail closed, don't expose lower layers.
			delete(records, lower)
			tombstones[lower] = struct{}{}
			continue
		}
		// Overrides replace all lower-priority records and corrections.
		delete(tombstones, lower)
		records[lower] = cloneCommandInfoForName(lower, info)
	}

	table := make(map[string]cscCommandMeta, len(records))
	for name, info := range records {
		table[name] = cscDeriveMeta(info)
	}
	// Keep only parents whose arity permits a bare invocation. Lookup still
	// checks subcommands first, so a parent cannot shadow its children.
	shadowedParents := make(map[string]struct{})
	for parent := range parents {
		if _, shadowed := table[parent]; shadowed && !commandMetadataAllowsBareInvocation(records[parent]) {
			delete(table, parent)
			shadowedParents[parent] = struct{}{}
		}
	}
	return &commandMetadataView{
		generation:        commandMetadataGeneration.Add(1),
		records:           records,
		tombstones:        tombstones,
		subcommandParents: parents,
		shadowedParents:   shadowedParents,
		cscTable:          table,
		cscParents:        parents,
		routingTable:      deriveRoutingTable(records, shadowedParents),
		cscFingerprint:    cscTableFingerprint(table),
		serverVersion:     serverVersion,
	}
}

func commandMetadataAllowsBareInvocation(info *CommandInfo) bool {
	return info != nil && (info.Arity == 1 || info.Arity == -1)
}

// commandMetadataSupportsCSC reports whether live metadata has all CSC
// exclusion signals.
func commandMetadataSupportsCSC(version string) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return false
	}
	return major > 8 || major == 8 && minor >= 10
}

// resolveLiveCommandMetadata applies version-specific restrictions to the
// shared record before consumers derive decisions.
func resolveLiveCommandMetadata(
	name string,
	info *CommandInfo,
	liveSupportsCSC bool,
	legacy bool,
) *CommandInfo {
	resolved := cloneCommandInfoForName(name, info)
	_, snapshotKnown := commandInfoSnapshot[name]
	if !liveSupportsCSC {
		if correction, ok := commandMetadataPre810Corrections[name]; ok {
			resolved.Flags = appendCommandMetadataTokens(resolved.Flags, correction.flags...)
			resolved.Tips = appendCommandMetadataTokens(resolved.Tips, correction.tips...)
		}
	}

	// Preserve legacy records for routing, but exclude unprovable records from
	// CSC. Snapshot-known pre-8.10 records keep their known exclusions above.
	if (!liveSupportsCSC && !snapshotKnown) || (liveSupportsCSC && legacy) {
		resolved.Tips = appendCommandMetadataTokens(resolved.Tips, "dont_cache")
	}
	return resolved
}

func cloneCommandInfoForName(name string, info *CommandInfo) *CommandInfo {
	cloned := cloneCommandInfo(info)
	cloned.Name = internal.ToLower(name)
	for i, flag := range cloned.Flags {
		cloned.Flags[i] = normalizeCommandMetadataFlag(flag)
	}
	for i, tip := range cloned.Tips {
		cloned.Tips[i] = normalizeCommandMetadataTip(tip)
	}
	for i := range cloned.KeySpecs {
		spec := &cloned.KeySpecs[i]
		spec.BeginSearch = normalizeCommandMetadataEnum(
			spec.BeginSearch,
			"index", "keyword", "unknown",
		)
		spec.FindKeys = normalizeCommandMetadataEnum(
			spec.FindKeys,
			"range", "keynum", "unknown",
		)
		for j, flag := range spec.Flags {
			spec.Flags[j] = normalizeCommandMetadataKeyFlag(flag)
		}
	}
	cloned.ReadOnly = commandRecordHas(cloned, "readonly", false)
	return cloned
}

func normalizeCommandMetadataFlag(flag string) string {
	key, _, hasValue := strings.Cut(flag, ":")
	lowerKey := internal.ToLower(key)
	switch lowerKey {
	case "blocking", "script_runner":
		// Preserve malformed negative signals conservatively.
		return lowerKey
	case "readonly":
		if !hasValue {
			return lowerKey
		}
		return flag
	default:
		return flag
	}
}

func normalizeCommandMetadataTip(tip string) string {
	key, value, hasValue := strings.Cut(tip, ":")
	lowerKey := internal.ToLower(key)
	switch lowerKey {
	case "dont_cache", "nondeterministic_output":
		// Preserve malformed negative signals conservatively.
		return lowerKey
	case requestPolicy:
		if hasValue {
			return lowerKey + ":" + normalizeCommandMetadataEnum(
				value,
				"", "default", "none", "all_nodes", "all_shards", "multi_shard", "special",
			)
		}
		return lowerKey
	case responsePolicy:
		if hasValue {
			return lowerKey + ":" + normalizeCommandMetadataEnum(
				value,
				"default(keyless)", "default(hashslot)", "all_succeeded", "one_succeeded",
				"agg_sum", "agg_min", "agg_max", "agg_logical_and", "agg_logical_or", "special",
			)
		}
		return lowerKey
	}
	return tip
}

func normalizeCommandMetadataEnum(value string, known ...string) string {
	lower := internal.ToLower(value)
	for _, candidate := range known {
		if lower == candidate {
			return candidate
		}
	}
	return value
}

func normalizeCommandMetadataKeyFlag(flag string) string {
	upper := strings.ToUpper(flag)
	switch upper {
	case "RO", "RW", "OW", "RM":
		return upper
	}
	lower := internal.ToLower(flag)
	switch lower {
	case "access", "update", "insert", "delete", "not_key", "incomplete", "variable_flags", "prefix":
		return lower
	default:
		return flag
	}
}

// commandMetadataKeySpecFlags is the validated key-spec meaning shared by CSC
// and Cluster routing. Unknown and prefix flags fail closed.
type commandMetadataKeySpecFlags struct {
	routingUsable bool
	cscComplete   bool
	planComplete  bool
}

func classifyCommandMetadataKeySpecFlags(flags []string) commandMetadataKeySpecFlags {
	seen := make(map[string]struct{}, len(flags))
	accessModes := 0
	actions := 0
	incomplete := false
	notKey := false
	for _, flag := range flags {
		if _, duplicate := seen[flag]; duplicate {
			return commandMetadataKeySpecFlags{}
		}
		seen[flag] = struct{}{}
		switch flag {
		case "RO", "RW", "OW", "RM":
			accessModes++
		case "access", "update", "insert", "delete":
			actions++
		case "variable_flags":
		case "incomplete":
			incomplete = true
		case "not_key":
			notKey = true
		case "prefix":
			return commandMetadataKeySpecFlags{}
		default:
			return commandMetadataKeySpecFlags{}
		}
	}

	// not_key arguments can route by slot but cannot prove CSC invalidation.
	if notKey {
		if accessModes != 0 || actions != 0 {
			return commandMetadataKeySpecFlags{}
		}
		return commandMetadataKeySpecFlags{
			routingUsable: true,
			planComplete:  !incomplete,
		}
	}

	// Ordinary key specs require exactly one access mode.
	if accessModes != 1 {
		return commandMetadataKeySpecFlags{}
	}
	return commandMetadataKeySpecFlags{
		routingUsable: true,
		cscComplete:   !incomplete,
		planComplete:  !incomplete,
	}
}

func appendCommandMetadataTokens(tokens []string, additions ...string) []string {
	for _, addition := range additions {
		found := false
		for _, token := range tokens {
			if token == addition {
				found = true
				break
			}
		}
		if !found {
			tokens = append(tokens, addition)
		}
	}
	return tokens
}

func removeCommandMetadataTokens(tokens []string, removals ...string) []string {
	if len(removals) == 0 {
		return tokens
	}
	kept := tokens[:0]
	for _, token := range tokens {
		remove := false
		for _, candidate := range removals {
			if token == candidate {
				remove = true
				break
			}
		}
		if !remove {
			kept = append(kept, token)
		}
	}
	return kept
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
	if info.CommandPolicy != nil {
		policy := *info.CommandPolicy
		if info.CommandPolicy.Tips != nil {
			policy.Tips = make(map[string]string, len(info.CommandPolicy.Tips))
			for key, value := range info.CommandPolicy.Tips {
				policy.Tips[key] = value
			}
		}
		cp.CommandPolicy = &policy
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
		fmt.Fprintf(h, "%s\x00%d %d %d %d %d %d %d\n",
			name, m.bits, m.extract, m.guard, m.firstKey, m.lastKey, m.step, m.numkeysAt)
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// commandMetadataFetchResult separates publication identity from the version
// used for compatibility rules.
type commandMetadataFetchResult struct {
	records           map[string]*CommandInfo
	legacyRecords     map[string]struct{}
	serverVersion     string
	serverFingerprint string
}

type commandMetadataFetchFunc func(context.Context) (commandMetadataFetchResult, error)

// commandMetadataStore publishes immutable views for one client. Its pointer
// is shared with clones; the attaching client owns the worker.
type commandMetadataStore struct {
	current atomic.Pointer[commandMetadataView]

	mode            CommandMetadataMode
	overrides       map[string]*CommandInfo
	refreshInterval time.Duration
	fetch           commandMetadataFetchFunc

	// static is the initial and fallback view.
	static *commandMetadataView

	// fetchCtx is cancelled by signalStop so Close never waits out an
	// in-flight fetch's own timeout.
	fetchCtx    context.Context
	fetchCancel context.CancelFunc

	mu        sync.Mutex
	refreshMu sync.Mutex
	started   bool
	refresh   chan struct{}
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once

	// serverFp identifies the current server. serverEpoch also rejects ABA
	// changes during a refresh. Both are guarded by mu.
	serverFp    string
	serverEpoch uint64
}

// newCommandMetadataStore returns nil for configs equivalent to the static
// default, letting those clients share defaultCommandMetadataView with zero
// allocations.
func newCommandMetadataStore(
	cfg *CommandMetadataConfig,
	fetch commandMetadataFetchFunc,
) *commandMetadataStore {
	return newCommandMetadataStoreWithLiveRequirement(cfg, fetch, false)
}

// newCommandMetadataStoreForLive creates a store even for static configuration.
func newCommandMetadataStoreForLive(
	cfg *CommandMetadataConfig,
	fetch commandMetadataFetchFunc,
) *commandMetadataStore {
	return newCommandMetadataStoreWithLiveRequirement(cfg, fetch, true)
}

func newCommandMetadataStoreWithLiveRequirement(
	cfg *CommandMetadataConfig,
	fetch commandMetadataFetchFunc,
	requireLive bool,
) *commandMetadataStore {
	if cfg == nil {
		cfg = &CommandMetadataConfig{}
	}
	if !requireLive && cfg.Mode == CommandMetadataStatic && len(cfg.Overrides) == 0 {
		return nil
	}
	// Deep-copy the overrides: refreshOnce re-reads them on every rebuild,
	// and an application mutating its config map would race the worker.
	var overrides map[string]*CommandInfo
	if len(cfg.Overrides) > 0 {
		overrides = make(map[string]*CommandInfo, len(cfg.Overrides))
		// Sort before normalization for deterministic case-collision handling.
		names := make([]string, 0, len(cfg.Overrides))
		for name := range cfg.Overrides {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			info := cfg.Overrides[name]
			name = internal.ToLower(name)
			if _, duplicate := overrides[name]; duplicate {
				// Tombstone ambiguous case-colliding overrides.
				overrides[name] = nil
				continue
			}
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
	if len(overrides) == 0 {
		s.static = defaultCommandMetadataView
	} else {
		s.static = buildCommandMetadataView(nil, overrides)
	}
	s.current.Store(s.static)
	// An override keyed by a bare container name ("memory") is pruned in
	// favor of its subcommand entries and would otherwise vanish silently.
	for name, info := range overrides {
		if info == nil {
			continue
		}
		if _, shadowed := s.static.shadowedParents[name]; shadowed {
			internal.Logger.Printf(context.Background(),
				"redis: CommandMetadata override %q targets a container command and has no effect; key it \"parent|child\"", name)
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
	if s == nil || s.mode != CommandMetadataPreferLive {
		return
	}
	if v := s.current.Load(); v != nil && v.live {
		return
	}
	s.requestRefresh()
}

// ensureLive synchronously fetches metadata until a live view is published.
func (s *commandMetadataStore) ensureLive(ctx context.Context) error {
	if s == nil || s.fetch == nil {
		return fmt.Errorf("redis: live command metadata fetch is unavailable")
	}
	if v := s.current.Load(); v != nil && v.live {
		return nil
	}
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if v := s.current.Load(); v != nil && v.live {
		return nil
	}
	return s.refreshOnceLocked(ctx)
}

// onServerHello retires live metadata when the server identity changes.
func (s *commandMetadataStore) onServerHello(fp string) {
	if s == nil || s.mode != CommandMetadataPreferLive {
		return
	}
	s.mu.Lock()
	changed := fp != s.serverFp
	if changed {
		s.serverFp = fp
		s.serverEpoch++
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
	fp, _ := s.serverIdentity()
	return fp
}

func (s *commandMetadataStore) serverIdentity() (string, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serverFp, s.serverEpoch
}

// publishLiveView atomically validates the server identity and publishes the
// fetched view. It adopts the fetched identity when none was known.
func (s *commandMetadataStore) publishLiveView(
	expectedFp string,
	expectedEpoch uint64,
	fetchedFp string,
	view *commandMetadataView,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverFp != expectedFp || s.serverEpoch != expectedEpoch {
		return false
	}
	// Never publish metadata without a verifiable source identity.
	if fetchedFp == "" || expectedFp != "" && expectedFp != fetchedFp {
		return false
	}
	if expectedFp == "" && fetchedFp != "" {
		s.serverFp = fetchedFp
	}
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

// invalidateLiveAndRequestRefresh retires old metadata and schedules a refresh.
func (s *commandMetadataStore) invalidateLiveAndRequestRefresh() {
	s.invalidateLive()
	if s != nil && s.mode == CommandMetadataPreferLive {
		s.requestRefresh()
	}
}

// beginParentSourceChange blocks refresh publication across a topology change.
func (s *commandMetadataStore) beginParentSourceChange() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.invalidateLiveLocked()
}

func (s *commandMetadataStore) finishParentSourceChange() {
	if s == nil {
		return
	}
	s.mu.Unlock()
	if s.mode == CommandMetadataPreferLive {
		s.requestRefresh()
	}
}

// invalidateLive retires the live view without starting a fetch.
func (s *commandMetadataStore) invalidateLive() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.invalidateLiveLocked()
	s.mu.Unlock()
}

func (s *commandMetadataStore) invalidateLiveLocked() {
	s.serverFp = ""
	s.serverEpoch++
	if view := s.current.Load(); view != nil && view.live {
		s.current.Store(s.static)
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
			err := s.refreshOnce(s.fetchCtx)
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

func (s *commandMetadataStore) refreshOnce(parent context.Context) (err error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	return s.refreshOnceLocked(parent)
}

func (s *commandMetadataStore) refreshOnceLocked(parent context.Context) (err error) {
	fpStart, epochStart := s.serverIdentity()
	ctx, cancel := context.WithTimeout(parent, cmdMetaFetchTimeout)
	stopFetchCancel := context.AfterFunc(s.fetchCtx, cancel)
	defer stopFetchCancel()
	defer cancel()

	// The fetch runs in its own goroutine: the command path only honors ctx
	// deadlines with ContextTimeoutEnabled, and a stalled reply must not
	// block stop/join. An abandoned attempt ends when its connection does.
	type fetchResult struct {
		metadata commandMetadataFetchResult
		err      error
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
		metadata, err := s.fetch(ctx)
		ch <- fetchResult{metadata: metadata, err: err}
	}()
	var metadata commandMetadataFetchResult
	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		metadata = res.metadata
	}
	if fp, epoch := s.serverIdentity(); fp != fpStart || epoch != epochStart {
		// Do not publish across a server change.
		return fmt.Errorf("command metadata server changed during refresh")
	}
	view := buildCommandMetadataViewForServerWithLegacy(
		metadata.records,
		s.overrides,
		metadata.serverVersion,
		metadata.legacyRecords,
	)
	view.live = true
	if !s.publishLiveView(fpStart, epochStart, metadata.serverFingerprint, view) {
		return fmt.Errorf("command metadata server changed during publication")
	}
	return nil
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
	b.WriteString(helloServerVersion(reply))
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

func helloServerVersion(reply map[string]interface{}) string {
	version, _ := reply["version"].(string)
	return version
}

// metadataView returns the client's current command-metadata view; clients
// without a store share the static default.
func (c *baseClient) metadataView() *commandMetadataView {
	if s := c.cmdMeta; s != nil {
		return s.view()
	}
	return defaultCommandMetadataView
}

// fetchCommandMetadata retrieves HELLO and COMMAND on one connection. It
// bypasses process hooks and CSC, but retains connection initialization hooks.
func (c *baseClient) fetchCommandMetadata(ctx context.Context) (commandMetadataFetchResult, error) {
	// Metadata refresh owns a bounded internal context independently of the
	// application's ContextTimeoutEnabled and socket-timeout choices. Run the
	// command through a lightweight clone whose zero socket timeouts defer to
	// that context deadline; otherwise ReadTimeout=-1 would leave a stalled
	// COMMAND holding a pooled connection after refreshOnce stopped waiting.
	fetchClient := c.withTimeout(0)
	// TODO: make baseClient.withTimeout preserve its hook snapshot.
	fetchClient.hooksMixin = c.hooksMixin.clone()
	fetchClient.opt.ContextTimeoutEnabled = true

	// Send HELLO and COMMAND in one pipeline. Each pipeline attempt holds one
	// physical connection for its entire write/read cycle, which proves which
	// server supplied the COMMAND reply even while a maintenance handoff leaves
	// connections to both endpoints in the pool. Do not wrap the pool in a
	// StickyConnPool: claiming a tracked connection there revokes its parent CSC
	// coverage and unnecessarily evicts that connection's cache entries on every
	// refresh.
	expectedFp := ""
	if c.cmdMeta != nil {
		expectedFp = c.cmdMeta.serverFingerprint()
	}
	fetchClient.pipelinePool = nil
	// Initialization of the borrowed connection must not change the target
	// identity while this fetch is validating it; the parent connection init
	// already owns onServerHello notifications.
	fetchClient.cmdMeta = nil

	helloCmd := NewMapStringInterfaceCmd(ctx, "hello")
	infoCmd := NewCommandsInfoCmd(ctx, "command")
	if err := fetchClient.processPipeline(ctx, []Cmder{helloCmd, infoCmd}); err != nil {
		return commandMetadataFetchResult{}, err
	}
	hello, err := helloCmd.Result()
	if err != nil {
		return commandMetadataFetchResult{}, err
	}
	serverVersion := helloServerVersion(hello)
	if serverVersion == "" {
		return commandMetadataFetchResult{}, fmt.Errorf("command metadata HELLO reply has no server version")
	}
	actualFp := helloServerFingerprint(hello)
	if expectedFp != "" && actualFp != expectedFp {
		// Notify the owner when this fetch first observes a server change.
		if c.cmdMeta != nil {
			c.cmdMeta.onServerHello(actualFp)
		}
		return commandMetadataFetchResult{}, fmt.Errorf(
			"command metadata server changed: got %q, want %q", actualFp, expectedFp,
		)
	}
	records, err := infoCmd.Result()
	if err != nil {
		return commandMetadataFetchResult{}, err
	}
	return commandMetadataFetchResult{
		records:           records,
		legacyRecords:     infoCmd.legacyRecords,
		serverVersion:     serverVersion,
		serverFingerprint: actualFp,
	}, nil
}
