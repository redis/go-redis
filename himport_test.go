package redis

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sort"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// epoch and empty are test-only registry accessors; the linter runs with
// tests excluded, so keeping them in himport.go would flag them as unused.
func (r *himportRegistry) epoch() uint64 {
	r.mu.RLock()
	e := r.discardAllEpoch
	r.mu.RUnlock()
	return e
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

func TestHImportCmdArgs(t *testing.T) {
	ctx := context.Background()

	prep := NewHImportPrepareCmd(ctx, "fs", "name", "email", "age")
	wantPrep := []interface{}{"himport", "prepare", "fs", "name", "email", "age"}
	if !reflect.DeepEqual(prep.Args(), wantPrep) {
		t.Errorf("prepare args = %v, want %v", prep.Args(), wantPrep)
	}

	set := NewHImportSetCmd(ctx, "key1", "fs", "alice", "a@example.com", 25)
	wantSet := []interface{}{"himport", "set", "key1", "fs", "alice", "a@example.com", 25}
	if !reflect.DeepEqual(set.Args(), wantSet) {
		t.Errorf("set args = %v, want %v", set.Args(), wantSet)
	}
	if pos := set.firstKeyPos(); pos != 2 {
		t.Errorf("set firstKeyPos = %d, want 2", pos)
	}

	discard := NewHImportDiscardCmd(ctx, "fs")
	wantDiscard := []interface{}{"himport", "discard", "fs"}
	if !reflect.DeepEqual(discard.Args(), wantDiscard) {
		t.Errorf("discard args = %v, want %v", discard.Args(), wantDiscard)
	}

	discardAll := NewHImportDiscardAllCmd(ctx)
	wantDiscardAll := []interface{}{"himport", "discardall"}
	if !reflect.DeepEqual(discardAll.Args(), wantDiscardAll) {
		t.Errorf("discardall args = %v, want %v", discardAll.Args(), wantDiscardAll)
	}
}

func TestHImportRegistry(t *testing.T) {
	var nilRegistry *himportRegistry
	if _, ok := nilRegistry.lookup("fs"); ok {
		t.Error("nil registry lookup should miss")
	}
	if !nilRegistry.empty() || !nilRegistry.idle() {
		t.Error("nil registry should be empty and idle")
	}

	r := newHImportRegistry()
	if !r.empty() || !r.idle() {
		t.Error("new registry should be empty and idle")
	}

	v1, e1 := r.register("fs", []string{"a", "b"})
	if v1 == 0 {
		t.Error("versions must start above the 0 sentinel")
	}
	if e1 != 0 {
		t.Errorf("initial epoch = %d, want 0", e1)
	}
	fs, ok := r.lookup("fs")
	if !ok || fs.version != v1 || !reflect.DeepEqual(fs.fields, []string{"a", "b"}) {
		t.Errorf("lookup = %+v, %v; want fields [a b] at version %d", fs, ok, v1)
	}

	// Re-registering the same name must bump the version so stale
	// per-connection flags are invalidated.
	v2, _ := r.register("fs", []string{"c"})
	if v2 <= v1 {
		t.Errorf("re-register version = %d, want > %d", v2, v1)
	}

	// Discard leaves a tombstone; re-registering clears it (the new version
	// replaces the fieldset server-side, no discard needed).
	r.discard("fs")
	if _, ok := r.lookup("fs"); ok {
		t.Error("lookup after discard should miss")
	}
	if _, tombs := r.cleanupSnapshot(); len(tombs) != 1 || tombs[0] != "fs" {
		t.Errorf("tombstones after discard = %v, want [fs]", tombs)
	}
	if r.idle() {
		t.Error("registry with tombstones is not idle")
	}
	r.register("fs", []string{"d"})
	if _, tombs := r.cleanupSnapshot(); len(tombs) != 0 {
		t.Errorf("tombstones after re-register = %v, want none", tombs)
	}

	// Discarding an unknown name leaves no tombstone: the client never
	// prepared it anywhere.
	r.discard("never-registered")
	if _, tombs := r.cleanupSnapshot(); len(tombs) != 0 {
		t.Errorf("tombstones after unknown discard = %v, want none", tombs)
	}

	// Discard-all moves to a new epoch and drops fieldsets and tombstones.
	r.discard("fs")
	epoch := r.discardAll()
	if epoch != 1 || r.epoch() != 1 {
		t.Errorf("epoch after discardAll = %d/%d, want 1", epoch, r.epoch())
	}
	if !r.empty() {
		t.Error("registry should be empty after discardAll")
	}
	if _, tombs := r.cleanupSnapshot(); len(tombs) != 0 {
		t.Errorf("tombstones after discardAll = %v, want none", tombs)
	}
	if r.idle() {
		t.Error("registry with a non-zero epoch is not idle: sessions may predate it")
	}
}

func TestHImportConnPreparedFlags(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	cn := pool.NewConn(client)
	if v := cn.FieldsetPreparedVersion("fs"); v != 0 {
		t.Errorf("unprepared fieldset version = %d, want 0", v)
	}
	if cn.HasPreparedFieldsets() {
		t.Error("fresh connection should have no prepared fieldsets")
	}

	// The first mark stamps the session's epoch; later marks keep it.
	cn.MarkFieldsetPrepared("fs", 7, 3)
	if v := cn.FieldsetPreparedVersion("fs"); v != 7 {
		t.Errorf("prepared fieldset version = %d, want 7", v)
	}
	if e := cn.FieldsetEpoch(); e != 3 {
		t.Errorf("epoch after first mark = %d, want 3", e)
	}
	cn.MarkFieldsetPrepared("fs2", 8, 9)
	if e := cn.FieldsetEpoch(); e != 3 {
		t.Errorf("epoch after second mark = %d, want 3 (unchanged)", e)
	}
	names := cn.PreparedFieldsetNames()
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"fs", "fs2"}) {
		t.Errorf("prepared names = %v, want [fs fs2]", names)
	}

	cn.UnmarkFieldsetPrepared("fs")
	if v := cn.FieldsetPreparedVersion("fs"); v != 0 {
		t.Errorf("unmarked fieldset version = %d, want 0", v)
	}

	// Clearing wipes the flags and adopts the given epoch.
	cn.ClearPreparedFieldsets(5)
	if cn.HasPreparedFieldsets() || cn.FieldsetEpoch() != 5 {
		t.Errorf("after clear: has=%v epoch=%d, want none/5",
			cn.HasPreparedFieldsets(), cn.FieldsetEpoch())
	}

	// A session cleared to epoch 5 that prepares again keeps... the epoch of
	// that prepare (empty -> non-empty stamps anew).
	cn.MarkFieldsetPrepared("fs", 9, 6)
	if e := cn.FieldsetEpoch(); e != 6 {
		t.Errorf("epoch after re-mark = %d, want 6", e)
	}

	// Replacing the network connection is a new server session: flags and
	// epoch reset.
	client2, server2 := net.Pipe()
	defer client2.Close()
	defer server2.Close()
	cn.SetNetConn(client2)
	if cn.HasPreparedFieldsets() || cn.FieldsetEpoch() != 0 {
		t.Errorf("after SetNetConn: has=%v epoch=%d, want none/0",
			cn.HasPreparedFieldsets(), cn.FieldsetEpoch())
	}
}

func injectedKinds(cmds []Cmder) []string {
	kinds := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		switch cmd.(type) {
		case *HImportPrepareCmd:
			kinds = append(kinds, "prepare")
		case *HImportDiscardCmd:
			kinds = append(kinds, "discard")
		case *HImportDiscardAllCmd:
			kinds = append(kinds, "discardall")
		default:
			kinds = append(kinds, "other")
		}
	}
	return kinds
}

func TestHImportInjectedPrepare(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)
	set := NewHImportSetCmd(ctx, "k", "fs", "v1", "v2")

	// Not an HIMPORT command: nothing to inject.
	if inj := c.himportInjectedCmds(ctx, cn, []Cmder{NewStatusCmd(ctx, "ping")}); inj != nil {
		t.Errorf("injected for PING = %v, want nil", injectedKinds(inj))
	}

	// Unregistered fieldset: raw pass-through, nothing to inject.
	if inj := c.himportInjectedCmds(ctx, cn, []Cmder{set}); inj != nil {
		t.Errorf("injected for unregistered fieldset = %v, want nil", injectedKinds(inj))
	}

	// Registered but not prepared on this connection: inject PREPARE.
	version, _ := c.himport.register("fs", []string{"f1", "f2"})
	inj := c.himportInjectedCmds(ctx, cn, []Cmder{set})
	if len(inj) != 1 {
		t.Fatalf("injected = %v, want one PREPARE", injectedKinds(inj))
	}
	prep, ok := inj[0].(*HImportPrepareCmd)
	if !ok {
		t.Fatalf("injected[0] = %T, want *HImportPrepareCmd", inj[0])
	}
	wantArgs := []interface{}{"himport", "prepare", "fs", "f1", "f2"}
	if !reflect.DeepEqual(prep.Args(), wantArgs) {
		t.Errorf("injected prepare args = %v, want %v", prep.Args(), wantArgs)
	}
	if prep.registryVersion != version {
		t.Errorf("injected prepare version = %d, want %d", prep.registryVersion, version)
	}

	// Prepared at the current version: nothing to inject.
	cn.MarkFieldsetPrepared("fs", version, 0)
	if inj := c.himportInjectedCmds(ctx, cn, []Cmder{set}); len(inj) != 0 {
		t.Errorf("injected for prepared connection = %v, want none", injectedKinds(inj))
	}

	// Fieldset replaced under a new version: the session's old version is
	// discarded before the replay, so a failed re-prepare leaves the SET
	// answering "no such fieldset" instead of silently writing the old
	// version's field names.
	c.himport.register("fs", []string{"f3"})
	inj = c.himportInjectedCmds(ctx, cn, []Cmder{set})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"discard", "prepare"}) {
		t.Errorf("injected after re-register = %v, want [discard prepare]", kinds)
	}

	// A connection with no version at all needs no discard.
	fresh := pool.NewConn(nil)
	inj = c.himportInjectedCmds(ctx, fresh, []Cmder{set})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"prepare"}) {
		t.Errorf("injected for fresh connection = %v, want [prepare]", kinds)
	}
}

func TestHImportInjectedBatch(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)

	c.himport.register("fs", []string{"f1"})
	c.himport.register("fs2", []string{"g1"})

	// Two SETs on the same fieldset need a single PREPARE; a user-issued
	// PREPARE earlier in the batch covers its fieldset.
	batch := []Cmder{
		NewStatusCmd(ctx, "ping"),
		NewHImportPrepareCmd(ctx, "fs2", "g1"),
		NewHImportSetCmd(ctx, "k1", "fs", "v"),
		NewHImportSetCmd(ctx, "k2", "fs", "v"),
		NewHImportSetCmd(ctx, "k3", "fs2", "v"),
	}
	inj := c.himportInjectedCmds(ctx, cn, batch)
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"prepare"}) {
		t.Fatalf("injected = %v, want [prepare]", kinds)
	}
	if inj[0].(*HImportPrepareCmd).fieldsetName != "fs" {
		t.Errorf("injected prepare fieldset = %q, want fs", inj[0].(*HImportPrepareCmd).fieldsetName)
	}

	// A connection already prepared at the current version needs nothing.
	fs, _ := c.himport.lookup("fs")
	cn.MarkFieldsetPrepared("fs", fs.version, 0)
	if inj := c.himportInjectedCmds(ctx, cn, []Cmder{NewHImportSetCmd(ctx, "k1", "fs", "v")}); len(inj) != 0 {
		t.Errorf("injected for prepared connection = %v, want none", injectedKinds(inj))
	}
}

func TestHImportInjectedDiscard(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)

	// Prepare fs and fs2 on this connection, then discard fs through the
	// registry (as if the DISCARD executed on another pooled connection).
	v1, e1 := c.himport.register("fs", []string{"f"})
	cn.MarkFieldsetPrepared("fs", v1, e1)
	v2, e2 := c.himport.register("fs2", []string{"g"})
	cn.MarkFieldsetPrepared("fs2", v2, e2)
	c.himport.discard("fs")

	// The session still holds fs: the next HIMPORT command replays the
	// DISCARD ahead of itself.
	inj := c.himportInjectedCmds(ctx, cn, []Cmder{NewHImportSetCmd(ctx, "k", "fs2", "v")})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"discard"}) {
		t.Fatalf("injected = %v, want [discard]", kinds)
	}
	if inj[0].(*HImportDiscardCmd).fieldsetName != "fs" {
		t.Errorf("injected discard fieldset = %q, want fs", inj[0].(*HImportDiscardCmd).fieldsetName)
	}

	// A connection that never prepared fs replays nothing.
	other := pool.NewConn(nil)
	otherFs2, _ := c.himport.lookup("fs2")
	other.MarkFieldsetPrepared("fs2", otherFs2.version, c.himport.epoch())
	if inj := c.himportInjectedCmds(ctx, other, []Cmder{NewHImportSetCmd(ctx, "k", "fs2", "v")}); len(inj) != 0 {
		t.Errorf("injected on unaffected connection = %v, want none", injectedKinds(inj))
	}

	// Cleanup piggybacks on any HIMPORT command, not only SET.
	inj = c.himportInjectedCmds(ctx, cn, []Cmder{NewHImportDiscardCmd(ctx, "fs2")})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"discard"}) {
		t.Errorf("injected before user discard = %v, want [discard]", kinds)
	}

	// Non-HIMPORT traffic never triggers cleanup.
	if inj := c.himportInjectedCmds(ctx, cn, []Cmder{NewStatusCmd(ctx, "ping")}); inj != nil {
		t.Errorf("injected for PING = %v, want nil", injectedKinds(inj))
	}

	// Re-registering the name clears the tombstone. This session still
	// holds the old version (its cleanup discard never ran), so the replay
	// discards it before the PREPARE — a failed re-prepare must not leave
	// the old field names live.
	c.himport.register("fs", []string{"f-new"})
	inj = c.himportInjectedCmds(ctx, cn, []Cmder{NewHImportSetCmd(ctx, "k", "fs", "v")})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"discard", "prepare"}) {
		t.Errorf("injected after re-register = %v, want [discard prepare]", kinds)
	}
}

func TestHImportInjectedDiscardAll(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)

	v1, e1 := c.himport.register("fs", []string{"f"})
	cn.MarkFieldsetPrepared("fs", v1, e1)
	c.himport.discardAll()

	// Session predates the epoch: one DISCARDALL wipes it. The fieldset is
	// gone from the registry, so no PREPARE follows.
	inj := c.himportInjectedCmds(ctx, cn, []Cmder{NewHImportSetCmd(ctx, "k", "fs", "v")})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"discardall"}) {
		t.Fatalf("injected = %v, want [discardall]", kinds)
	}
	if inj[0].(*HImportDiscardAllCmd).registryEpoch != c.himport.epoch() {
		t.Errorf("injected discardall epoch = %d, want %d",
			inj[0].(*HImportDiscardAllCmd).registryEpoch, c.himport.epoch())
	}

	// A fieldset registered after the discard-all is replayed alongside the
	// session wipe, ignoring the connection's stale prepared flag.
	v2, _ := c.himport.register("fs", []string{"f2"})
	cn.MarkFieldsetPrepared("fs", v2, 0) // stale epoch: marked before the wipe
	inj = c.himportInjectedCmds(ctx, cn, []Cmder{NewHImportSetCmd(ctx, "k", "fs", "v")})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"discardall", "prepare"}) {
		t.Fatalf("injected = %v, want [discardall prepare]", kinds)
	}

	// A fresh session (no prepared fieldsets) has nothing to wipe.
	fresh := pool.NewConn(nil)
	inj = c.himportInjectedCmds(ctx, fresh, []Cmder{NewHImportSetCmd(ctx, "k", "fs", "v")})
	if kinds := injectedKinds(inj); !reflect.DeepEqual(kinds, []string{"prepare"}) {
		t.Errorf("injected on fresh connection = %v, want [prepare]", kinds)
	}

	// A session prepared under the current epoch is not wiped again.
	current := pool.NewConn(nil)
	fs, _ := c.himport.lookup("fs")
	current.MarkFieldsetPrepared("fs", fs.version, c.himport.epoch())
	if inj := c.himportInjectedCmds(ctx, current, []Cmder{NewHImportSetCmd(ctx, "k", "fs", "v")}); len(inj) != 0 {
		t.Errorf("injected on current-epoch connection = %v, want none", injectedKinds(inj))
	}
}

func TestHImportAfterCmd(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)

	// A successful user-issued PREPARE registers the fieldset and marks the
	// executing connection.
	prep := NewHImportPrepareCmd(ctx, "fs", "f1", "f2")
	c.himportAfterCmd(cn, prep)
	fs, ok := c.himport.lookup("fs")
	if !ok || !reflect.DeepEqual(fs.fields, []string{"f1", "f2"}) {
		t.Fatalf("registry after prepare = %+v, %v; want fields [f1 f2]", fs, ok)
	}
	if cn.FieldsetPreparedVersion("fs") != fs.version {
		t.Error("executing connection should be marked at the registered version")
	}

	// DISCARD unregisters, tombstones, and unmarks the executing connection.
	c.himportAfterCmd(cn, NewHImportDiscardCmd(ctx, "fs"))
	if _, ok := c.himport.lookup("fs"); ok {
		t.Error("fieldset should be unregistered after discard")
	}
	if cn.FieldsetPreparedVersion("fs") != 0 {
		t.Error("connection flag should be gone after discard")
	}
	if _, tombs := c.himport.cleanupSnapshot(); len(tombs) != 1 {
		t.Errorf("tombstones after discard = %v, want [fs]", tombs)
	}

	// DISCARDALL clears everything and moves the connection to the new
	// epoch.
	c.himportAfterCmd(cn, NewHImportPrepareCmd(ctx, "fs1", "a"))
	c.himportAfterCmd(cn, NewHImportPrepareCmd(ctx, "fs2", "b"))
	c.himportAfterCmd(cn, NewHImportDiscardAllCmd(ctx))
	if !c.himport.empty() {
		t.Error("registry should be empty after discardall")
	}
	if cn.HasPreparedFieldsets() {
		t.Error("connection flags should be gone after discardall")
	}
	if cn.FieldsetEpoch() != c.himport.epoch() {
		t.Errorf("connection epoch = %d, want %d", cn.FieldsetEpoch(), c.himport.epoch())
	}
}

func TestHImportAfterBatch(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)
	c.himport.register("fs", []string{"f1"})

	// An injected PREPARE failure is the root cause of the dependent SET's
	// "no such fieldset" reply.
	prep := NewHImportPrepareCmd(ctx, "fs", "f1")
	prepErr := proto.RedisError("ERR duplicate field name in fieldset")
	prep.SetErr(prepErr)

	set := NewHImportSetCmd(ctx, "k", "fs", "v")
	set.SetErr(proto.RedisError("ERR no such fieldset"))
	otherSet := NewHImportSetCmd(ctx, "k2", "other", "v")
	otherSet.SetErr(proto.RedisError("ERR no such fieldset"))

	c.himportAfterBatch(cn, []Cmder{prep}, []Cmder{set, otherSet})
	if !errors.Is(set.Err(), prepErr) {
		t.Errorf("dependent set error = %v, want the prepare root cause", set.Err())
	}
	if errors.Is(otherSet.Err(), prepErr) {
		t.Error("unrelated set must keep its own error")
	}

	// A registered fieldset that came back "no such fieldset" without a
	// failed injected PREPARE means the session lost it: the stale flag is
	// invalidated so a caller retry re-prepares.
	fs, _ := c.himport.lookup("fs")
	cn.MarkFieldsetPrepared("fs", fs.version, 0)
	lost := NewHImportSetCmd(ctx, "k3", "fs", "v")
	lost.SetErr(proto.RedisError("ERR no such fieldset"))
	c.himportAfterBatch(cn, nil, []Cmder{lost})
	if cn.FieldsetPreparedVersion("fs") != 0 {
		t.Error("stale prepared flag must be invalidated after no-such-fieldset")
	}

	// Successful user-issued commands in the batch update the registry.
	userPrep := NewHImportPrepareCmd(ctx, "fs3", "x")
	c.himportAfterBatch(cn, nil, []Cmder{userPrep})
	if _, ok := c.himport.lookup("fs3"); !ok {
		t.Error("successful in-batch prepare should register the fieldset")
	}

	failedPrep := NewHImportPrepareCmd(ctx, "fs4", "x")
	failedPrep.SetErr(proto.RedisError("ERR duplicate field name in fieldset"))
	c.himportAfterBatch(cn, nil, []Cmder{failedPrep})
	if _, ok := c.himport.lookup("fs4"); ok {
		t.Error("failed in-batch prepare must not register the fieldset")
	}
}

// Every pooled client constructor must wire the HIMPORT registry: without it,
// HImportPrepare silently skips registration and pooled HImportSet replay
// never engages.
func TestHImportRegistryWiring(t *testing.T) {
	client := NewClient(&Options{Addr: "127.0.0.1:0"})
	defer client.Close()
	if client.himport == nil {
		t.Error("NewClient must initialize the HIMPORT registry")
	}

	// Conn and Tx borrow connections from the client's pool and return them:
	// they must share the parent registry so prepared flags stay coherent.
	conn := client.Conn()
	defer conn.Close()
	if conn.himport != client.himport {
		t.Error("Client.Conn() must share the parent HIMPORT registry")
	}
	tx := client.newTx()
	defer tx.Close(context.Background())
	if tx.himport != client.himport {
		t.Error("newTx must share the parent HIMPORT registry")
	}

	failover := NewFailoverClient(&FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:0"},
	})
	defer failover.Close()
	if failover.himport == nil {
		t.Error("NewFailoverClient must initialize the HIMPORT registry")
	}
}

func TestHImportRecover(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)
	noSuchFieldset := proto.RedisError("ERR no such fieldset")

	set := NewHImportSetCmd(ctx, "k", "fs", "v")

	// Unregistered fieldset: a retry would fail identically.
	if c.himportRecover(set, noSuchFieldset, cn) {
		t.Error("recover must refuse unregistered fieldsets")
	}

	version, epoch := c.himport.register("fs", []string{"f1"})
	cn.MarkFieldsetPrepared("fs", version, epoch)

	// Wrong error: nothing to recover.
	if c.himportRecover(set, proto.RedisError("ERR value count does not match fieldset field count"), cn) {
		t.Error("recover must only react to no-such-fieldset errors")
	}
	// Wrong command type: nothing to recover.
	if c.himportRecover(NewStatusCmd(ctx, "ping"), noSuchFieldset, cn) {
		t.Error("recover must only react to HIMPORT SET")
	}

	// Registered fieldset: the stale flag is invalidated and a retry may
	// succeed.
	if !c.himportRecover(set, noSuchFieldset, cn) {
		t.Error("recover should allow a retry for registered fieldsets")
	}
	if cn.FieldsetPreparedVersion("fs") != 0 {
		t.Error("recover must invalidate the connection's prepared flag")
	}
}
