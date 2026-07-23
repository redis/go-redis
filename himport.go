package redis

import (
	"context"
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
	// the fieldset on the server, so no discard is needed).
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
// command.
func (r *himportRegistry) discard(name string) {
	r.mu.Lock()
	if _, ok := r.fieldsets[name]; ok {
		delete(r.fieldsets, name)
		if r.tombstones == nil {
			r.tombstones = make(map[string]struct{})
		}
		r.tombstones[name] = struct{}{}
	}
	r.mu.Unlock()
}

// discardAll drops every fieldset and tombstone and moves to a new epoch;
// connections prepared under an older epoch replay HIMPORT DISCARDALL before
// their next HIMPORT command.
func (r *himportRegistry) discardAll() (epoch uint64) {
	r.mu.Lock()
	r.fieldsets = nil
	r.tombstones = nil
	r.discardAllEpoch++
	epoch = r.discardAllEpoch
	r.mu.Unlock()
	return epoch
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
	for _, cmd := range cmds {
		switch hc := cmd.(type) {
		case *HImportPrepareCmd:
			cover(hc.fieldsetName)
		case *HImportSetCmd:
			if _, ok := covered[hc.fieldsetName]; ok {
				continue
			}
			fs, ok := c.himport.lookup(hc.fieldsetName)
			if !ok {
				continue
			}
			if !sessionWiped && cn.FieldsetPreparedVersion(hc.fieldsetName) == fs.version {
				continue
			}
			// The session holds an older version. Discard it before the
			// re-prepare: the SET behind it is already on the wire, and if
			// the re-prepare fails the SET must answer "no such fieldset"
			// rather than silently writing the old version's field names.
			if !sessionWiped && cn.FieldsetPreparedVersion(hc.fieldsetName) != 0 {
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
		version, epoch := c.himport.register(cmd.fieldsetName, cmd.fields)
		cn.MarkFieldsetPrepared(cmd.fieldsetName, version, epoch)
	case *HImportDiscardCmd:
		c.himport.discard(cmd.fieldsetName)
		cn.UnmarkFieldsetPrepared(cmd.fieldsetName)
	case *HImportDiscardAllCmd:
		epoch := c.himport.discardAll()
		cn.ClearPreparedFieldsets(epoch)
	}
}

// himportAfterBatch runs after all replies of a batch were read: it surfaces
// an injected PREPARE failure as the root cause on the HIMPORT SET commands
// that depended on it (their own reply is the secondary "no such fieldset"
// error), invalidates stale prepared flags for SETs that found their
// registered fieldset missing server-side, and applies registry updates for
// user-issued HIMPORT commands that succeeded in the batch.
func (c *baseClient) himportAfterBatch(cn *pool.Conn, injected []Cmder, cmds []Cmder) {
	var failed map[string]error
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
	for _, cmd := range cmds {
		hc, ok := cmd.(himportCmder)
		if !ok {
			continue
		}
		if set, ok := hc.(*HImportSetCmd); ok {
			if rootCause, ok := failed[set.fieldsetName]; ok && himportNoSuchFieldset(set.Err()) {
				set.SetErr(rootCause)
				continue
			}
			// The session lost a fieldset the flags claim is prepared (e.g.
			// RESET): invalidate the flag so a caller-initiated retry of the
			// pipeline replays the PREPARE. Pipelines are not auto-retried:
			// re-running the whole batch would repeat the side effects of
			// commands that already succeeded.
			if himportNoSuchFieldset(set.Err()) {
				if _, registered := c.himport.lookup(set.fieldsetName); registered {
					cn.UnmarkFieldsetPrepared(set.fieldsetName)
				}
			}
			continue
		}
		if hc.Err() == nil {
			c.himportAfterCmd(cn, hc)
		}
	}
}

// himportRecover prepares a retry after cmd failed with "no such fieldset":
// if the fieldset is registered client-side, the executing connection lost
// its server session state (for example a RESET, or a concurrent discard),
// so its prepared flag is invalidated and the retry re-prepares lazily. It
// reports whether such a retry may succeed.
func (c *baseClient) himportRecover(cmd Cmder, err error, cn *pool.Conn) bool {
	set, ok := cmd.(*HImportSetCmd)
	if !ok || !himportNoSuchFieldset(err) {
		return false
	}
	if _, registered := c.himport.lookup(set.fieldsetName); !registered {
		return false
	}
	if cn != nil {
		cn.UnmarkFieldsetPrepared(set.fieldsetName)
	}
	return true
}
