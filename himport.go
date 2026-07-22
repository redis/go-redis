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
type himportRegistry struct {
	mu          sync.RWMutex
	nextVersion uint64
	fieldsets   map[string]himportFieldset
}

func newHImportRegistry() *himportRegistry {
	return &himportRegistry{}
}

func (r *himportRegistry) register(name string, fields []string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fieldsets == nil {
		r.fieldsets = make(map[string]himportFieldset)
	}
	r.nextVersion++
	r.fieldsets[name] = himportFieldset{
		fields:  append([]string(nil), fields...),
		version: r.nextVersion,
	}
	return r.nextVersion
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

func (r *himportRegistry) unregister(name string) {
	r.mu.Lock()
	delete(r.fieldsets, name)
	r.mu.Unlock()
}

func (r *himportRegistry) clear() {
	r.mu.Lock()
	r.fieldsets = nil
	r.mu.Unlock()
}

func (r *himportRegistry) empty() bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	n := len(r.fieldsets)
	r.mu.RUnlock()
	return n == 0
}

// himportNoSuchFieldset reports whether err is the server's "no such
// fieldset" reply, i.e. an HIMPORT SET executed on a connection whose session
// does not hold the referenced fieldset.
func himportNoSuchFieldset(err error) bool {
	return isRedisError(err) && strings.Contains(err.Error(), "no such fieldset")
}

// himportEnsureCmd returns an HIMPORT PREPARE command to write ahead of cmd
// on cn, or nil if none is needed. A PREPARE is needed when cmd is an HIMPORT
// SET whose fieldset is registered client-side and cn's session was not
// prepared at the current registry version.
func (c *baseClient) himportEnsureCmd(ctx context.Context, cn *pool.Conn, cmd Cmder) *HImportPrepareCmd {
	set, ok := cmd.(*HImportSetCmd)
	if !ok {
		return nil
	}
	fs, ok := c.himport.lookup(set.fieldsetName)
	if !ok || cn.FieldsetPreparedVersion(set.fieldsetName) == fs.version {
		return nil
	}
	prep := NewHImportPrepareCmd(ctx, set.fieldsetName, fs.fields...)
	prep.registryVersion = fs.version
	return prep
}

// himportPrepareCmdsForBatch collects the HIMPORT PREPARE commands to write
// ahead of a pipeline batch on cn: one per registered fieldset referenced by
// an HIMPORT SET in the batch that cn's session lacks at the current registry
// version. A fieldset covered by a user-issued PREPARE earlier in the batch
// needs no injection — the server session holds it by the time the SET runs.
func (c *baseClient) himportPrepareCmdsForBatch(ctx context.Context, cn *pool.Conn, cmds []Cmder) []*HImportPrepareCmd {
	if c.himport.empty() {
		return nil
	}
	var (
		prepCmds []*HImportPrepareCmd
		covered  map[string]struct{}
	)
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
			if !ok || cn.FieldsetPreparedVersion(hc.fieldsetName) == fs.version {
				continue
			}
			prep := NewHImportPrepareCmd(ctx, hc.fieldsetName, fs.fields...)
			prep.registryVersion = fs.version
			prepCmds = append(prepCmds, prep)
			cover(hc.fieldsetName)
		}
	}
	return prepCmds
}

// himportReadPrepareReplies consumes the replies of injected HIMPORT PREPARE
// commands. Server errors are recorded on the command and the connection is
// left readable; transport errors are returned. On success the connection is
// marked as prepared at the version the command was injected for.
func (c *baseClient) himportReadPrepareReplies(ctx context.Context, cn *pool.Conn, rd *proto.Reader, prepCmds []*HImportPrepareCmd) error {
	for _, prep := range prepCmds {
		if err := c.processPendingPushNotificationWithReader(ctx, cn, rd); err != nil {
			internal.Logger.Printf(ctx, "push: error processing pending notifications before reading reply: %v", err)
		}
		err := prep.readReply(rd)
		prep.SetErr(err)
		if err != nil {
			if !isRedisError(err) {
				return err
			}
			continue
		}
		cn.MarkFieldsetPrepared(prep.fieldsetName, prep.registryVersion)
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
		version := c.himport.register(cmd.fieldsetName, cmd.fields)
		cn.MarkFieldsetPrepared(cmd.fieldsetName, version)
	case *HImportDiscardCmd:
		c.himport.unregister(cmd.fieldsetName)
		cn.UnmarkFieldsetPrepared(cmd.fieldsetName)
	case *HImportDiscardAllCmd:
		c.himport.clear()
		cn.ClearPreparedFieldsets()
	}
}

// himportAfterBatch runs after all replies of a pipeline batch were read: it
// surfaces an injected PREPARE failure as the root cause on the HIMPORT SET
// commands that depended on it (their own reply is the secondary "no such
// fieldset" error), and applies registry updates for user-issued HIMPORT
// commands that succeeded in the batch.
func (c *baseClient) himportAfterBatch(cn *pool.Conn, prepCmds []*HImportPrepareCmd, cmds []Cmder) {
	var failed map[string]error
	for _, prep := range prepCmds {
		if err := prep.Err(); err != nil {
			if failed == nil {
				failed = make(map[string]error)
			}
			failed[prep.fieldsetName] = err
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
