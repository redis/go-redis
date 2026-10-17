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

	// Discard-all moves to a new epoch, drops fieldsets and tombstones, and
	// reports how many fieldsets were registered.
	r.discard("fs")
	epoch, removed := r.discardAll()
	if epoch != 1 || r.epoch() != 1 {
		t.Errorf("epoch after discardAll = %d/%d, want 1", epoch, r.epoch())
	}
	if removed != 0 {
		t.Errorf("discardAll removed = %d, want 0 (fs was already discarded)", removed)
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

	// DISCARD unregisters, tombstones, unmarks the executing connection,
	// and reports the registry lifecycle: 1 for a registered fieldset even
	// when the executing session did not hold it (server replied 0).
	discard := NewHImportDiscardCmd(ctx, "fs")
	discard.SetVal(0)
	c.himportAfterCmd(cn, discard)
	if _, ok := c.himport.lookup("fs"); ok {
		t.Error("fieldset should be unregistered after discard")
	}
	if cn.FieldsetPreparedVersion("fs") != 0 {
		t.Error("connection flag should be gone after discard")
	}
	if _, tombs := c.himport.cleanupSnapshot(); len(tombs) != 1 {
		t.Errorf("tombstones after discard = %v, want [fs]", tombs)
	}
	if discard.Val() != 1 {
		t.Errorf("discard value = %d, want 1 (fieldset was registered)", discard.Val())
	}

	// An unregistered name keeps the server's session reply.
	rawDiscard := NewHImportDiscardCmd(ctx, "raw-only")
	rawDiscard.SetVal(1)
	c.himportAfterCmd(cn, rawDiscard)
	if rawDiscard.Val() != 1 {
		t.Errorf("unregistered discard value = %d, want the server reply 1", rawDiscard.Val())
	}

	// DISCARDALL clears everything, moves the connection to the new epoch,
	// and reports the number of registered fieldsets removed.
	c.himportAfterCmd(cn, NewHImportPrepareCmd(ctx, "fs1", "a"))
	c.himportAfterCmd(cn, NewHImportPrepareCmd(ctx, "fs2", "b"))
	discardAll := NewHImportDiscardAllCmd(ctx)
	discardAll.SetVal(0)
	c.himportAfterCmd(cn, discardAll)
	if !c.himport.empty() {
		t.Error("registry should be empty after discardall")
	}
	if discardAll.Val() != 2 {
		t.Errorf("discardall value = %d, want 2 (registered fieldsets removed)", discardAll.Val())
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
	// failed injected PREPARE means the session lost it — and other
	// sessions may have been wiped by the same event. The fieldset version
	// is bumped once, staling every connection's mark, so any retry
	// re-prepares wherever it lands.
	fs, _ := c.himport.lookup("fs")
	cn.MarkFieldsetPrepared("fs", fs.version, 0)
	lost := NewHImportSetCmd(ctx, "k3", "fs", "v")
	lost.SetErr(proto.RedisError("ERR no such fieldset"))
	lost2 := NewHImportSetCmd(ctx, "k4", "fs", "v")
	lost2.SetErr(proto.RedisError("ERR no such fieldset"))
	c.himportAfterBatch(cn, nil, []Cmder{lost, lost2})
	bumped, _ := c.himport.lookup("fs")
	if bumped.version != fs.version+1 {
		t.Errorf("fieldset version = %d, want %d (bumped exactly once for two failed sets)",
			bumped.version, fs.version+1)
	}
	if cn.FieldsetPreparedVersion("fs") == bumped.version {
		t.Error("the connection's mark must be stale against the bumped version")
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

	// Cluster node clients — replicas included, roles change with the
	// topology — share the cluster-wide registry.
	cluster := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:0"}})
	defer cluster.Close()
	if cluster.himport == nil {
		t.Fatal("NewClusterClient must initialize the HIMPORT registry")
	}
	node, err := cluster.nodes.GetOrCreate("127.0.0.1:0")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if node.Client.himport != cluster.himport {
		t.Error("cluster node clients must share the cluster-wide HIMPORT registry")
	}

	// Ring shard clients share the ring-wide registry.
	ring := NewRing(&RingOptions{Addrs: map[string]string{"shard1": "127.0.0.1:0"}})
	defer ring.Close()
	if ring.opt.himport == nil {
		t.Fatal("NewRing must initialize the HIMPORT registry")
	}
	shards := ring.sharding.List()
	if len(shards) == 0 {
		t.Fatal("ring has no shards")
	}
	if shards[0].Client.himport != ring.opt.himport {
		t.Error("ring shard clients must share the ring-wide HIMPORT registry")
	}

	// NewRing copies the options: rings built from one caller-owned struct
	// must not share or clobber each other's registry.
	sharedOpts := &RingOptions{Addrs: map[string]string{"shard1": "127.0.0.1:0"}}
	ringA := NewRing(sharedOpts)
	defer ringA.Close()
	ringB := NewRing(sharedOpts)
	defer ringB.Close()
	if ringA.opt.himport == ringB.opt.himport {
		t.Error("rings built from the same options must not share a registry")
	}
	if sharedOpts.himport != nil {
		t.Error("NewRing must not write the registry into the caller's options")
	}
}

// Fan-out copies carry a pre-assigned version/epoch: they mark or wipe the
// executing connection but must not touch the shared registry again.
func TestHImportFanOutCopyBookkeeping(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)

	version, epoch := c.himport.register("fs", []string{"f"})

	copyPrep := NewHImportPrepareCmd(ctx, "fs", "f")
	copyPrep.registryVersion = version
	copyPrep.registryEpoch = epoch
	c.himportAfterCmd(cn, copyPrep)
	if fs, _ := c.himport.lookup("fs"); fs.version != version {
		t.Errorf("fan-out prepare copy bumped the registry version to %d", fs.version)
	}
	if cn.FieldsetPreparedVersion("fs") != version {
		t.Error("fan-out prepare copy must mark the executing connection")
	}

	newEpoch, _ := c.himport.discardAll()
	copyDA := NewHImportDiscardAllCmd(ctx)
	copyDA.registryEpoch = newEpoch
	c.himportAfterCmd(cn, copyDA)
	if got := c.himport.epoch(); got != newEpoch {
		t.Errorf("fan-out discardall copy bumped the epoch to %d, want %d", got, newEpoch)
	}
	if cn.FieldsetEpoch() != newEpoch || cn.HasPreparedFieldsets() {
		t.Error("fan-out discardall copy must wipe the executing connection at the given epoch")
	}
}

// A deterministic server rejection withdraws the registration, but only at
// the failed version — a concurrent re-registration survives — and leaves a
// tombstone: the fan-out may have prepared some sessions before another
// master rejected it, and those sessions must be cleaned lazily.
func TestHImportRegistryDiscardVersion(t *testing.T) {
	r := newHImportRegistry()
	v1, _ := r.register("fs", []string{"f"})
	r.discardVersion("fs", v1)
	if _, ok := r.lookup("fs"); ok {
		t.Error("discardVersion must delete the entry at the matching version")
	}
	if _, tombs := r.cleanupSnapshot(); len(tombs) != 1 || tombs[0] != "fs" {
		t.Errorf("discardVersion must leave a tombstone for partial fan-out cleanup, got %v", tombs)
	}

	v2, _ := r.register("fs", []string{"g"})
	r.discardVersion("fs", v2-1) // stale version: no-op
	if fs, ok := r.lookup("fs"); !ok || fs.version != v2 {
		t.Error("discardVersion with a stale version must not clobber the current entry")
	}
	if _, tombs := r.cleanupSnapshot(); len(tombs) != 0 {
		t.Errorf("stale discardVersion must not tombstone (re-register cleared it), got %v", tombs)
	}
}

// refreshVersion invalidates every connection's mark by bumping the version
// while keeping the fields — used on "no such fieldset" so a retry
// re-prepares on whichever connection it lands (mass session loss may have
// staled more marks than the reporting connection's).
func TestHImportRegistryRefreshVersion(t *testing.T) {
	r := newHImportRegistry()
	v1, _ := r.register("fs", []string{"f1", "f2"})

	r.refreshVersion("fs", v1)
	fs, ok := r.lookup("fs")
	if !ok || fs.version <= v1 {
		t.Fatalf("version after refresh = %d, want > %d", fs.version, v1)
	}
	if !reflect.DeepEqual(fs.fields, []string{"f1", "f2"}) {
		t.Errorf("fields after refresh = %v, must be unchanged", fs.fields)
	}

	// A stale expected version must not disturb a concurrent
	// re-registration.
	v2, _ := r.register("fs", []string{"g"})
	r.refreshVersion("fs", v2-1)
	if fs, _ := r.lookup("fs"); fs.version != v2 {
		t.Errorf("version = %d, want %d (stale refresh must no-op)", fs.version, v2)
	}

	// Unregistered names are ignored.
	r.refreshVersion("missing", 1)
	if _, ok := r.lookup("missing"); ok {
		t.Error("refresh must not create entries")
	}
}

func TestHImportShouldRetrySet(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	noSuchFieldset := proto.RedisError("ERR no such fieldset")

	set := NewHImportSetCmd(ctx, "k", "fs", "v")

	// Unregistered fieldset: a retry would fail identically.
	if c.himportShouldRetrySet(set, noSuchFieldset) {
		t.Error("retry must be refused for unregistered fieldsets")
	}

	c.himport.register("fs", []string{"f1"})

	// Wrong error: nothing to recover.
	if c.himportShouldRetrySet(set, proto.RedisError("ERR value count does not match fieldset field count")) {
		t.Error("retry must only react to no-such-fieldset errors")
	}
	// Wrong command type: nothing to recover.
	if c.himportShouldRetrySet(NewStatusCmd(ctx, "ping"), noSuchFieldset) {
		t.Error("retry must only react to HIMPORT SET")
	}

	// Registered fieldset: a retry may succeed (the stale flag is
	// invalidated inside _process while the connection is held; the RESET
	// recovery in TestHImportLazyReplay pins that end to end).
	if !c.himportShouldRetrySet(set, noSuchFieldset) {
		t.Error("retry should be allowed for registered fieldsets")
	}
}
