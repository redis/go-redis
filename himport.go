package redis

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// himportFieldset is the client-side record of a fieldset registered with
// HImportPrepare.
type himportFieldset struct {
	fields  []string
	version uint64
}

// himportRegistry remembers fieldsets registered through a client so HIMPORT
// SET can lazily prepare them on whichever pooled connection it executes.
// Versions increase monotonically and start at 1; re-registering a name under
// a new version invalidates every connection's prepared flag for it, so a
// replaced fieldset is re-prepared before its next use.
//
// Discards propagate lazily as well: a discarded name is kept as a tombstone
// and the discard-all counter as an epoch, and connections whose sessions
// still hold discarded fieldsets replay HIMPORT DISCARD/DISCARDALL before
// their next HIMPORT command (see baseClient.himportInjectedCmds).
type himportRegistry struct {
	mu          sync.RWMutex
	nextVersion uint64
	fieldsets   map[string]himportFieldset
	// tombstones holds names discarded through this client whose server-side
	// copies may survive on pooled connections that prepared them. An entry
	// is removed when the name is registered again (the new version replaces
	// the fieldset on the server, so no discard is needed) or by discardAll.
	// Known limitation: a workload discarding many uniquely-named fieldsets
	// grows this map for the client's lifetime and pays an O(tombstones)
	// snapshot per HIMPORT round trip; HImportDiscardAll resets it.
	tombstones map[string]struct{}
	// discardAllEpoch increments on every successful HImportDiscardAll.
	discardAllEpoch uint64
}

func newHImportRegistry() *himportRegistry {
	return &himportRegistry{}
}

// register stores the fieldset and returns its new version together with the
// current discard-all epoch.
func (r *himportRegistry) register(name string, fields []string) (version, epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fieldsets == nil {
		r.fieldsets = make(map[string]himportFieldset)
	}
	delete(r.tombstones, name)
	r.nextVersion++
	r.fieldsets[name] = himportFieldset{
		fields:  append([]string(nil), fields...),
		version: r.nextVersion,
	}
	return r.nextVersion, r.discardAllEpoch
}

func (r *himportRegistry) lookup(name string) (himportFieldset, bool) {
	if r == nil {
		return himportFieldset{}, false
	}
	r.mu.RLock()
	fs, ok := r.fieldsets[name]
	r.mu.RUnlock()
	return fs, ok
}

// discard removes the fieldset and leaves a tombstone so connections whose
// sessions still hold it replay the DISCARD before their next HIMPORT
// command. It reports whether the fieldset was registered.
func (r *himportRegistry) discard(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.fieldsets[name]; !ok {
		return false
	}
	delete(r.fieldsets, name)
	if r.tombstones == nil {
		r.tombstones = make(map[string]struct{})
	}
	r.tombstones[name] = struct{}{}
	return true
}

// discardAll drops every fieldset and tombstone and moves to a new epoch;
// connections prepared under an older epoch replay HIMPORT DISCARDALL before
// their next HIMPORT command. It returns the new epoch and the number of
// fieldsets that were registered.
func (r *himportRegistry) discardAll() (epoch uint64, removed int) {
	r.mu.Lock()
	removed = len(r.fieldsets)
	r.fieldsets = nil
	r.tombstones = nil
	r.discardAllEpoch++
	epoch = r.discardAllEpoch
	r.mu.Unlock()
	return epoch, removed
}

// discardVersion withdraws a registration whose fan-out PREPARE was rejected
// by a server — but only while the entry is still at that version, so a
// concurrent re-registration is not clobbered. A tombstone is left: the
// fan-out may have succeeded on some masters before another rejected it
// (per-node ACLs, rolling upgrades), and those sessions hold the withdrawn
// fieldset; the tombstone makes their next HIMPORT command discard it
// instead of leaving a fieldset the client can no longer address.
func (r *himportRegistry) discardVersion(name string, version uint64) {
	r.mu.Lock()
	if fs, ok := r.fieldsets[name]; ok && fs.version == version {
		delete(r.fieldsets, name)
		if r.tombstones == nil {
			r.tombstones = make(map[string]struct{})
		}
		r.tombstones[name] = struct{}{}
	}
	r.mu.Unlock()
}

// refreshVersion bumps a registered fieldset to a new version, keeping its
// fields — but only while the entry is still at the given version, so a
// concurrent re-registration is not disturbed. Every connection's prepared
// flag becomes stale, forcing a re-prepare before the fieldset's next use on
// each of them. Used when a "no such fieldset" reply signals session loss
// that may have hit more connections than the one that reported it (failover,
// cross-region switch, reset storms).
func (r *himportRegistry) refreshVersion(name string, version uint64) {
	r.mu.Lock()
	if fs, ok := r.fieldsets[name]; ok && fs.version == version {
		r.nextVersion++
		fs.version = r.nextVersion
		r.fieldsets[name] = fs
	}
	r.mu.Unlock()
}

// idle reports whether the registry implies no injection work at all: no
// fieldsets to replay, no tombstones to discard, and no discard-all epoch a
// session could be behind.
func (r *himportRegistry) idle() bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	idle := len(r.fieldsets) == 0 && len(r.tombstones) == 0 && r.discardAllEpoch == 0
	r.mu.RUnlock()
	return idle
}

// cleanupSnapshot returns the current epoch and the tombstoned names.
func (r *himportRegistry) cleanupSnapshot() (epoch uint64, tombstones []string) {
	r.mu.RLock()
	epoch = r.discardAllEpoch
	if len(r.tombstones) > 0 {
		tombstones = make([]string, 0, len(r.tombstones))
		for name := range r.tombstones {
			tombstones = append(tombstones, name)
		}
	}
	r.mu.RUnlock()
	return epoch, tombstones
}

// himportNoSuchFieldset reports whether err is the server's "no such
// fieldset" reply, i.e. an HIMPORT SET executed on a connection whose session
// does not hold the referenced fieldset.
func himportNoSuchFieldset(err error) bool {
	return isRedisError(err) && strings.Contains(err.Error(), "no such fieldset")
}

// himportInjectedCmds returns the HIMPORT commands to write to cn ahead of a
// batch, in order:
//
//  1. HIMPORT DISCARDALL when cn's session was prepared under an older
//     discard-all epoch;
//  2. HIMPORT DISCARD for each discarded fieldset the session still holds;
//  3. HIMPORT PREPARE for each registered fieldset referenced by an HIMPORT
//     SET in the batch that the session lacks at the current version.
//
// A fieldset covered by a user-issued PREPARE earlier in the batch needs no
// injection — the server session holds it by the time the SET runs. Returns
// nil when the batch contains no HIMPORT commands: sessions holding only
// discarded fieldsets are cleaned up on their next HIMPORT use, not on
// unrelated traffic.
func (c *baseClient) himportInjectedCmds(ctx context.Context, cn *pool.Conn, cmds []Cmder) []Cmder {
	if c.himport.idle() {
		return nil
	}
	hasHImport := false
	for _, cmd := range cmds {
		if _, ok := cmd.(himportCmder); ok {
			hasHImport = true
			break
		}
	}
	if !hasHImport {
		return nil
	}

	var injected []Cmder

	// Discards first: a session behind the discard-all epoch is wiped
	// entirely; otherwise individual tombstoned fieldsets it still holds are
	// discarded.
	epoch, tombstones := c.himport.cleanupSnapshot()
	sessionWiped := false
	if cn.HasPreparedFieldsets() && cn.FieldsetEpoch() != epoch {
		da := NewHImportDiscardAllCmd(ctx)
		da.registryEpoch = epoch
		injected = append(injected, da)
		sessionWiped = true
	} else {
		for _, name := range tombstones {
			if cn.FieldsetPreparedVersion(name) != 0 {
				injected = append(injected, NewHImportDiscardCmd(ctx, name))
			}
		}
	}

	// Prepares for registered fieldsets the batch's SETs reference.
	var covered map[string]struct{}
	cover := func(name string) {
		if covered == nil {
			covered = make(map[string]struct{})
		}
		covered[name] = struct{}{}
	}
	// batchSchemas holds the fieldset a user-issued PREPARE earlier in
	// this batch registered, so a SET after it is stamped with the
	// schema it was issued under rather than the registry's older one.
	var batchSchemas map[string]himportFieldset
	for _, cmd := range cmds {
		switch hc := cmd.(type) {
		case *HImportPrepareCmd:
			if batchSchemas == nil {
				batchSchemas = make(map[string]himportFieldset)
			}
			batchSchemas[hc.fieldsetName] = himportFieldset{fields: hc.fields, version: hc.registryVersion}
			cover(hc.fieldsetName)
		case *HImportSetCmd:
			// Stamp the SET with the fieldset schema in force when
			// it was first dispatched: a later answered PREPARE in
			// the same batch may replace the registry entry before
			// the retry runs, and re-preparing from the registry
			// would write the SET's values under the new field
			// names. The stamp is kept only while this invocation
			// still owns the dispatch: a reused SET in a later
			// invocation must be re-stamped from the schema in
			// force then, or it keeps preparing the first
			// invocation's fields after the fieldset was replaced.
			// "This invocation" is the per-invocation answered
			// set for pipelines — a SET answered in this
			// invocation is in it — or, on paths without one (a
			// single command, a cluster node batch, a direct
			// call), a non-stale error: an error this invocation
			// recorded on the SET. A reused SET carrying a stale
			// error from an earlier invocation on a path without
			// either record still keeps its stamp; that case
			// needs a reset at the invocation entry, not here.
			answered := pipelineAnsweredFromCtx(ctx)
			stale := pipelineStaleErrsFromCtx(ctx)
			dispatched := answered[hc.base()] || (pipelineCmdErr(hc) != nil && !stale[hc.base()])
			if !hc.issuedSet || !dispatched {
				if fs, ok := batchSchemas[hc.fieldsetName]; ok {
					hc.issuedFields = append([]string(nil), fs.fields...)
					hc.issuedVersion = fs.version
					hc.issuedSet = true
				} else if fs, ok := c.himport.lookup(hc.fieldsetName); ok {
					hc.issuedFields = append([]string(nil), fs.fields...)
					hc.issuedVersion = fs.version
					hc.issuedSet = true
				}
			}
			if _, ok := covered[hc.fieldsetName]; ok {
				continue
			}
			fs, ok := c.himport.lookup(hc.fieldsetName)
			if !ok {
				continue
			}
			// A stamped SET runs under its issued schema, not the
			// registry's current one: a later answered PREPARE
			// replaced the fieldset after the SET was issued, and
			// the retry must re-prepare the old fields before the
			// SET. When the registry no longer holds the name at
			// all (a discard), there is nothing to re-prepare, as
			// before. A stamp whose fields match the registry is
			// the same schema, so the normal path below applies.
			if hc.issuedSet && !slices.Equal(hc.issuedFields, fs.fields) {
				fs = himportFieldset{fields: hc.issuedFields, version: hc.issuedVersion}
			}
			preparedVersion := cn.FieldsetPreparedVersion(hc.fieldsetName)
			// A stamp from a user-issued PREPARE carries version 0:
			// the session holds nothing at version 0, so the
			// prepared-version equality below must not skip the
			// injection for it — a fresh session needs the PREPARE.
			if !sessionWiped && fs.version != 0 && preparedVersion == fs.version {
				continue
			}
			// The session holds an older version. Discard it before the
			// re-prepare: the SET behind it is already on the wire, and if
			// the re-prepare fails the SET must answer "no such fieldset"
			// rather than silently writing the old version's field names.
			if !sessionWiped && preparedVersion != 0 {
				injected = append(injected, NewHImportDiscardCmd(ctx, hc.fieldsetName))
			}
			prep := NewHImportPrepareCmd(ctx, hc.fieldsetName, fs.fields...)
			prep.registryVersion = fs.version
			prep.registryEpoch = epoch
			injected = append(injected, prep)
			cover(hc.fieldsetName)
		}
	}
	return injected
}

// himportReadInjectedReplies consumes the replies of injected HIMPORT
// commands. Server errors are recorded on the command and the connection is
// left readable; transport errors are returned. Successful commands apply
// their prepared-flag bookkeeping on cn.
func (c *baseClient) himportReadInjectedReplies(ctx context.Context, cn *pool.Conn, rd *proto.Reader, injected []Cmder) error {
	for _, cmd := range injected {
		if err := c.processPendingPushNotificationWithReader(ctx, cn, rd); err != nil {
			internal.Logger.Printf(ctx, "push: error processing pending notifications before reading reply: %v", err)
		}
		err := cmd.readReply(rd)
		cmd.SetErr(err)
		if err != nil {
			if !isRedisError(err) {
				return err
			}
			// A failed injected PREPARE becomes the root cause of the
			// dependent SETs' errors downstream; a failed injected discard
			// only delays cleanup until the next HIMPORT command.
			internal.Logger.Printf(ctx, "himport: injected %s failed: %v", cmd.Name(), err)
			continue
		}
		switch hc := cmd.(type) {
		case *HImportPrepareCmd:
			cn.MarkFieldsetPrepared(hc.fieldsetName, hc.registryVersion, hc.registryEpoch)
		case *HImportDiscardCmd:
			cn.UnmarkFieldsetPrepared(hc.fieldsetName)
		case *HImportDiscardAllCmd:
			cn.ClearPreparedFieldsets(hc.registryEpoch)
		}
	}
	return nil
}

// himportAfterCmd applies registry and prepared-flag updates after a
// user-issued HIMPORT command completed successfully on cn.
func (c *baseClient) himportAfterCmd(cn *pool.Conn, hc himportCmder) {
	if c.himport == nil {
		return
	}
	switch cmd := hc.(type) {
	case *HImportPrepareCmd:
		version, epoch := cmd.registryVersion, cmd.registryEpoch
		if version == 0 {
			version, epoch = c.himport.register(cmd.fieldsetName, cmd.fields)
		}
		// A pre-assigned version marks a fan-out copy: the fieldset was
		// registered once at the cluster/ring level; only mark the
		// executing connection.
		cn.MarkFieldsetPrepared(cmd.fieldsetName, version, epoch)
	case *HImportDiscardCmd:
		registered := c.himport.discard(cmd.fieldsetName)
		cn.UnmarkFieldsetPrepared(cmd.fieldsetName)
		// The managed API reports the registry lifecycle: 1 when the
		// fieldset was registered on this client and is now removed. The
		// executing connection's session count stands only for fieldsets
		// the registry never knew (raw usage).
		if registered {
			cmd.SetVal(1)
		}
	case *HImportDiscardAllCmd:
		// A pre-assigned epoch marks a fan-out copy: the registry was
		// already wiped at the cluster/ring level; only move the executing
		// connection to that epoch.
		if cmd.registryEpoch != 0 {
			cn.ClearPreparedFieldsets(cmd.registryEpoch)
			return
		}
		epoch, removed := c.himport.discardAll()
		cn.ClearPreparedFieldsets(epoch)
		// Same registry semantics: report how many registered fieldsets
		// were removed, not how many the executing session happened to
		// hold.
		if removed > 0 {
			cmd.SetVal(int64(removed))
		}
	}
}

// himportAfterBatch runs after all replies of a batch were read: it surfaces
// an injected PREPARE failure as the root cause on the HIMPORT SET commands
// that depended on it (their own reply is the secondary "no such fieldset"
// error), invalidates stale prepared flags for SETs that found their
// registered fieldset missing server-side, and applies registry updates for
// user-issued HIMPORT commands that succeeded in the batch.
// rawErr throughout: this runs on the execution path, before an async
// autopipeline batch completes (its ready channel closes only after the
// pipeline hook chain returns) — Err() on a user command would await and
// self-deadlock the dispatcher.
func (c *baseClient) himportAfterBatch(cn *pool.Conn, injected []Cmder, cmds []Cmder) {
	var failed map[string]error
	var refreshed map[string]struct{}
	for _, cmd := range injected {
		if prep, ok := cmd.(*HImportPrepareCmd); ok {
			if err := prep.Err(); err != nil {
				if failed == nil {
					failed = make(map[string]error)
				}
				failed[prep.fieldsetName] = err
			}
		}
	}
	// A queued HIMPORT command only executed when its transaction's
	// EXEC committed it: +QUEUED merely acknowledges the queueing, so
	// the frame outcomes are precomputed once for the guard below —
	// but only when the batch actually holds a queued HIMPORT command,
	// so batches without one pay no extra scans.
	var discardedTx, abortedTx map[*baseCmd]bool
	var txBodyNeedsRetry []bool
	var queuedExecErrs map[int]error
	queuedGuardReady := false
	queuedGuard := func() {
		if queuedGuardReady {
			return
		}
		discardedTx = pipelineDiscardedTxCmds(cmds)
		abortedTx = pipelineAbortedTxCmds(cmds)
		txBodyNeedsRetry = pipelineTxBodyNeedsRetryMap(cmds)
		queuedExecErrs = pipelineQueuedExecErrMap(cmds)
		queuedGuardReady = true
	}
	for idx, cmd := range cmds {
		hc, ok := cmd.(himportCmder)
		if !ok {
			continue
		}
		if set, ok := hc.(*HImportSetCmd); ok {
			if rootCause, ok := failed[set.fieldsetName]; ok && himportNoSuchFieldset(set.rawErr()) {
				set.SetErr(rootCause)
				continue
			}
			// The session lost a fieldset the flags claim is prepared (e.g.
			// RESET) — and the same event may have wiped other sessions
			// whose flags also still look current. Bump the fieldset
			// version once so the SET's re-issue, the cluster re-queue on
			// whichever connection it lands, or the caller's transaction
			// retry replays the PREPARE.
			if himportNoSuchFieldset(set.rawErr()) {
				if _, done := refreshed[set.fieldsetName]; !done {
					if refreshed == nil {
						refreshed = make(map[string]struct{})
					}
					refreshed[set.fieldsetName] = struct{}{}
					if fs, registered := c.himport.lookup(set.fieldsetName); registered {
						c.himport.refreshVersion(set.fieldsetName, fs.version)
					}
				}
			}
			continue
		}
		if hc.rawErr() == nil {
			// A queued HIMPORT command never executed on its own:
			// it runs only when its transaction's EXEC commits it.
			// A PREPARE in a frame DISCARD dropped, the server
			// aborted, or still open registered nothing — applying
			// its bookkeeping would mark a fieldset prepared that
			// the server never saw, and a later SET would write
			// values under the wrong field names. Only a queued
			// command whose EXEC element answered without error
			// counts as executed.
			if pipelineCmdQueued(cmd) {
				queuedGuard()
				if discardedTx[cmd.base()] || abortedTx[cmd.base()] || txBodyNeedsRetry[idx] {
					continue
				}
				if elemErr, ok := queuedExecErrs[idx]; !ok || elemErr != nil {
					continue
				}
			}
			c.himportAfterCmd(cn, hc)
		}
	}
}

// himportRetryFailedSets re-issues, once, the HIMPORT SET commands of a
// pipeline batch that failed with "no such fieldset" while their fieldset is
// registered — the error must not surface for managed fieldsets (NF.4). Only
// the SETs are re-sent: HIMPORT SET is a full replace, so re-execution is
// idempotent, and no other command of the batch runs again. Their prepared
// flags were invalidated by himportAfterBatch, so himportInjectedCmds
// regenerates the PREPAREs for this connection. Transport errors are
// returned; server errors stay recorded on the commands.
// (The retry does not carry an ASKING prefix. A redirected [ASKING, SET]
// pair whose injected PREPARE failed is excluded by the root-cause swap in
// himportAfterBatch; one that lost its session without an injection can be
// re-issued here, and the bare SET then draws a fresh MOVED/ASK that the
// outer cluster redirect handling resolves.)
func (c *baseClient) himportRetryFailedSets(ctx context.Context, cn *pool.Conn, cmds []Cmder) error {
	if c.himport.idle() {
		return nil
	}
	var retry []Cmder
	for _, cmd := range cmds {
		// rawErr: execution path, same self-deadlock rule as himportAfterBatch.
		if set, ok := cmd.(*HImportSetCmd); ok && himportNoSuchFieldset(set.rawErr()) {
			if _, registered := c.himport.lookup(set.fieldsetName); registered {
				retry = append(retry, set)
			}
		}
	}
	if len(retry) == 0 {
		return nil
	}

	injected := c.himportInjectedCmds(ctx, cn, retry)
	if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, retry)
	}); err != nil {
		return err
	}
	return cn.WithReader(c.context(ctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
		if err := c.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		err := c.pipelineReadCmds(ctx, cn, rd, retry)
		if err != nil && !isRedisError(err) {
			return err
		}
		// Server errors (including a repeated failure) stay on the
		// individual commands; the batch as a whole is done.
		c.himportAfterBatch(cn, injected, retry)
		return nil
	})
}

// himportSetNeedsRecovery reports whether cmd is an HIMPORT SET whose
// "no such fieldset" reply means its write never happened and the managed
// recovery (a re-PREPARE plus a re-issued SET, see
// himportRetryFailedSets) has not run yet: that recovery runs only after
// a fully read attempt, so a transport failure partway through the replies
// returns before it. Such a SET must stay pending in a retry — trimming
// it as settled would keep the fieldset error while the write never
// happens. The registry is not consulted here: the retry re-prepares
// only a registered fieldset, so an unregistered one simply fails the
// same way again, with the same terminal error.
func himportSetNeedsRecovery(cmd Cmder) bool {
	_, ok := cmd.(*HImportSetCmd)
	return ok && himportNoSuchFieldset(pipelineCmdErr(cmd))
}

// himportShouldRetrySet reports whether a retry of cmd may succeed after it
// failed with "no such fieldset": true when the fieldset is registered
// client-side — the executing connection lost its server session state (for
// example a RESET, or a concurrent discard). The connection's prepared flag
// was already invalidated inside _process, while that goroutine still owned
// the connection, so the retry re-prepares lazily wherever it lands.
func (c *baseClient) himportShouldRetrySet(cmd Cmder, err error) bool {
	set, ok := cmd.(*HImportSetCmd)
	if !ok || !himportNoSuchFieldset(err) {
		return false
	}
	_, registered := c.himport.lookup(set.fieldsetName)
	return registered
}
