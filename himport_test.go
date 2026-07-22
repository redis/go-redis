package redis

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

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
	if !nilRegistry.empty() {
		t.Error("nil registry should be empty")
	}

	r := newHImportRegistry()
	if !r.empty() {
		t.Error("new registry should be empty")
	}

	v1 := r.register("fs", []string{"a", "b"})
	if v1 == 0 {
		t.Error("versions must start above the 0 sentinel")
	}
	fs, ok := r.lookup("fs")
	if !ok || fs.version != v1 || !reflect.DeepEqual(fs.fields, []string{"a", "b"}) {
		t.Errorf("lookup = %+v, %v; want fields [a b] at version %d", fs, ok, v1)
	}

	// Re-registering the same name must bump the version so stale
	// per-connection flags are invalidated.
	v2 := r.register("fs", []string{"c"})
	if v2 <= v1 {
		t.Errorf("re-register version = %d, want > %d", v2, v1)
	}
	fs, _ = r.lookup("fs")
	if fs.version != v2 || !reflect.DeepEqual(fs.fields, []string{"c"}) {
		t.Errorf("lookup after re-register = %+v, want fields [c] at version %d", fs, v2)
	}

	r.unregister("fs")
	if _, ok := r.lookup("fs"); ok {
		t.Error("lookup after unregister should miss")
	}

	r.register("fs1", []string{"a"})
	r.register("fs2", []string{"b"})
	r.clear()
	if !r.empty() {
		t.Error("registry should be empty after clear")
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

	cn.MarkFieldsetPrepared("fs", 7)
	if v := cn.FieldsetPreparedVersion("fs"); v != 7 {
		t.Errorf("prepared fieldset version = %d, want 7", v)
	}

	cn.UnmarkFieldsetPrepared("fs")
	if v := cn.FieldsetPreparedVersion("fs"); v != 0 {
		t.Errorf("unmarked fieldset version = %d, want 0", v)
	}

	cn.MarkFieldsetPrepared("fs1", 1)
	cn.MarkFieldsetPrepared("fs2", 2)
	cn.ClearPreparedFieldsets()
	if cn.FieldsetPreparedVersion("fs1") != 0 || cn.FieldsetPreparedVersion("fs2") != 0 {
		t.Error("flags should be gone after ClearPreparedFieldsets")
	}

	// Replacing the network connection is a new server session: all
	// prepared flags must be dropped.
	cn.MarkFieldsetPrepared("fs", 3)
	client2, server2 := net.Pipe()
	defer client2.Close()
	defer server2.Close()
	cn.SetNetConn(client2)
	if v := cn.FieldsetPreparedVersion("fs"); v != 0 {
		t.Errorf("fieldset version after SetNetConn = %d, want 0", v)
	}
}

func TestHImportEnsureCmd(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)

	// Not an HIMPORT SET: nothing to inject.
	if prep := c.himportEnsureCmd(ctx, cn, NewStatusCmd(ctx, "ping")); prep != nil {
		t.Errorf("ensure for PING = %v, want nil", prep.Args())
	}

	// Unregistered fieldset: raw pass-through, nothing to inject.
	set := NewHImportSetCmd(ctx, "k", "fs", "v1", "v2")
	if prep := c.himportEnsureCmd(ctx, cn, set); prep != nil {
		t.Errorf("ensure for unregistered fieldset = %v, want nil", prep.Args())
	}

	// Registered but not prepared on this connection: inject PREPARE.
	version := c.himport.register("fs", []string{"f1", "f2"})
	prep := c.himportEnsureCmd(ctx, cn, set)
	if prep == nil {
		t.Fatal("ensure for registered fieldset = nil, want PREPARE")
	}
	wantArgs := []interface{}{"himport", "prepare", "fs", "f1", "f2"}
	if !reflect.DeepEqual(prep.Args(), wantArgs) {
		t.Errorf("injected prepare args = %v, want %v", prep.Args(), wantArgs)
	}
	if prep.registryVersion != version {
		t.Errorf("injected prepare version = %d, want %d", prep.registryVersion, version)
	}

	// Prepared at the current version: nothing to inject.
	cn.MarkFieldsetPrepared("fs", version)
	if prep := c.himportEnsureCmd(ctx, cn, set); prep != nil {
		t.Errorf("ensure for prepared connection = %v, want nil", prep.Args())
	}

	// Fieldset replaced under a new version: the stale flag must not stop
	// the replay.
	c.himport.register("fs", []string{"f3"})
	if prep := c.himportEnsureCmd(ctx, cn, set); prep == nil {
		t.Error("ensure after re-register = nil, want PREPARE")
	}
}

func TestHImportPrepareCmdsForBatch(t *testing.T) {
	ctx := context.Background()
	c := &baseClient{himport: newHImportRegistry()}
	cn := pool.NewConn(nil)

	// Empty registry: no scan results regardless of batch content.
	batch := []Cmder{NewHImportSetCmd(ctx, "k", "fs", "v")}
	if prepCmds := c.himportPrepareCmdsForBatch(ctx, cn, batch); prepCmds != nil {
		t.Errorf("batch prepares with empty registry = %d, want none", len(prepCmds))
	}

	c.himport.register("fs", []string{"f1"})
	c.himport.register("fs2", []string{"g1"})

	// Two SETs on the same fieldset need a single PREPARE; a user-issued
	// PREPARE earlier in the batch covers its fieldset.
	batch = []Cmder{
		NewStatusCmd(ctx, "ping"),
		NewHImportPrepareCmd(ctx, "fs2", "g1"),
		NewHImportSetCmd(ctx, "k1", "fs", "v"),
		NewHImportSetCmd(ctx, "k2", "fs", "v"),
		NewHImportSetCmd(ctx, "k3", "fs2", "v"),
	}
	prepCmds := c.himportPrepareCmdsForBatch(ctx, cn, batch)
	if len(prepCmds) != 1 {
		t.Fatalf("batch prepares = %d, want 1", len(prepCmds))
	}
	if prepCmds[0].fieldsetName != "fs" {
		t.Errorf("batch prepare fieldset = %q, want fs", prepCmds[0].fieldsetName)
	}

	// A connection already prepared at the current version needs nothing.
	fs, _ := c.himport.lookup("fs")
	cn.MarkFieldsetPrepared("fs", fs.version)
	batch = []Cmder{NewHImportSetCmd(ctx, "k1", "fs", "v")}
	if prepCmds := c.himportPrepareCmdsForBatch(ctx, cn, batch); len(prepCmds) != 0 {
		t.Errorf("batch prepares for prepared connection = %d, want 0", len(prepCmds))
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

	// DISCARD unregisters and unmarks.
	c.himportAfterCmd(cn, NewHImportDiscardCmd(ctx, "fs"))
	if _, ok := c.himport.lookup("fs"); ok {
		t.Error("fieldset should be unregistered after discard")
	}
	if cn.FieldsetPreparedVersion("fs") != 0 {
		t.Error("connection flag should be gone after discard")
	}

	// DISCARDALL clears everything.
	c.himportAfterCmd(cn, NewHImportPrepareCmd(ctx, "fs1", "a"))
	c.himportAfterCmd(cn, NewHImportPrepareCmd(ctx, "fs2", "b"))
	c.himportAfterCmd(cn, NewHImportDiscardAllCmd(ctx))
	if !c.himport.empty() {
		t.Error("registry should be empty after discardall")
	}
	if cn.FieldsetPreparedVersion("fs1") != 0 || cn.FieldsetPreparedVersion("fs2") != 0 {
		t.Error("connection flags should be gone after discardall")
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

	c.himportAfterBatch(cn, []*HImportPrepareCmd{prep}, []Cmder{set, otherSet})
	if !errors.Is(set.Err(), prepErr) {
		t.Errorf("dependent set error = %v, want the prepare root cause", set.Err())
	}
	if errors.Is(otherSet.Err(), prepErr) {
		t.Error("unrelated set must keep its own error")
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

	version := c.himport.register("fs", []string{"f1"})
	cn.MarkFieldsetPrepared("fs", version)

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
