package redis

// FT.AGGREGATE ... WITHCURSOR creates a cursor on the node that runs it, and
// FT.CURSOR READ/DEL must reach that node. In cluster mode both route by the
// slot of the index name and always go to the slot master (#4098).

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/routing"
)

func TestFTCursorIndexPos(t *testing.T) {
	ctx := context.Background()
	var got Cmder
	c := cmdable(func(_ context.Context, cmd Cmder) error { got = cmd; return nil })
	typed := func(fn func()) Cmder { fn(); return got }

	cases := []struct {
		name string
		cmd  Cmder
		want int
	}{
		{"FTAggregateWithArgs WithCursor", typed(func() {
			c.FTAggregateWithArgs(ctx, "idx", "*", &FTAggregateOptions{WithCursor: true})
		}), 1},
		{"FTAggregateWithArgs WithCursor options", typed(func() {
			c.FTAggregateWithArgs(ctx, "idx", "*", &FTAggregateOptions{
				WithCursor: true, WithCursorOptions: &FTAggregateWithCursor{Count: 1},
			})
		}), 1},
		{"FTAggregateWithArgs no cursor", typed(func() {
			c.FTAggregateWithArgs(ctx, "idx", "*", &FTAggregateOptions{LoadAll: true})
		}), 0},
		{"FTAggregate", typed(func() { c.FTAggregate(ctx, "idx", "*") }), 0},
		{"FTCursorRead", typed(func() { c.FTCursorRead(ctx, "idx", 42, 1) }), 2},
		{"FTCursorDel", typed(func() { c.FTCursorDel(ctx, "idx", 42) }), 2},
		{"raw lowercase aggregate", NewCmd(ctx, "ft.aggregate", "idx", "*", "withcursor"), 1},
		{"raw mixed case aggregate", NewCmd(ctx, "Ft.Aggregate", "idx", "*", "WithCursor", "COUNT", 1), 1},
		{"raw aggregate WITHCURSOR as query", NewCmd(ctx, "FT.AGGREGATE", "idx", "WITHCURSOR"), 0},
		{"raw cursor read int64 id", NewCmd(ctx, "FT.CURSOR", "READ", "idx", int64(1<<40)), 2},
		{"raw cursor read string id", NewCmd(ctx, "ft.cursor", "read", "idx", "42"), 2},
		{"raw cursor del bytes id", NewCmd(ctx, "FT.CURSOR", "DEL", "idx", []byte("42")), 2},
		{"raw cursor unknown subcommand", NewCmd(ctx, "FT.CURSOR", "GC", "idx"), 0},
		{"raw cursor too short", NewCmd(ctx, "FT.CURSOR", "READ"), 0},
		{"FTSearch", typed(func() { c.FTSearch(ctx, "idx", "*") }), 0},
		{"GET", NewStringCmd(ctx, "get", "key"), 0},
		{"non-string name", NewCmd(ctx, []byte("FT.CURSOR"), "READ", "idx", 1), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ftCursorIndexPos(tc.cmd); got != tc.want {
				t.Fatalf("ftCursorIndexPos(%v) = %d, want %d", tc.cmd.Args(), got, tc.want)
			}
		})
	}
}

func TestFTCursorCommandsRouteByIndexSlot(t *testing.T) {
	ctx := context.Background()
	c := &ClusterClient{opt: &ClusterOptions{}, cmdsInfoCache: newCmdsInfoCache(nil)}

	for _, index := range []string{"idx", "{tag}idx"} {
		want := hashtag.Slot(index)
		cmds := []Cmder{
			NewAggregateCmd(ctx, "FT.AGGREGATE", index, "*", "WITHCURSOR"),
			NewMapStringInterfaceCmd(ctx, "FT.CURSOR", "READ", index, 42),
			NewStatusCmd(ctx, "FT.CURSOR", "DEL", index, 42),
		}
		for _, cmd := range cmds {
			if got := c.cmdSlot(cmd, -1); got != want {
				t.Fatalf("%v: slot %d, want %d", cmd.Args(), got, want)
			}
			// The cached slot must agree.
			if got := c.cmdSlot(cmd, -1); got != want {
				t.Fatalf("%v: cached slot %d, want %d", cmd.Args(), got, want)
			}
			// A preferred random slot does not move a cursor command.
			if got := c.cmdSlot(cmd, 7); got != want {
				t.Fatalf("%v: slot %d with preferred slot, want %d", cmd.Args(), got, want)
			}
		}
	}
	if hashtag.Slot("{tag}idx") != hashtag.Slot("tag") {
		t.Fatal("hashtag in the index name is not used")
	}

	plain := NewAggregateCmd(ctx, "FT.AGGREGATE", "idx", "*")
	if got := c.cmdSlot(plain, -1); got != -1 {
		t.Fatalf("FT.AGGREGATE without WITHCURSOR: slot %d, want -1 (keyless)", got)
	}
}

func TestFTCursorCommandsPinnedToMasterUnderReadOnly(t *testing.T) {
	ctx := context.Background()
	master := toleranceTestNode(t, 100*time.Microsecond, false)
	state := toleranceTestState(
		master,
		toleranceTestNode(t, 100*time.Microsecond, false),
		toleranceTestNode(t, 100*time.Microsecond, false),
	)
	state.Masters = []*clusterNode{master}
	state.Slaves = state.slots[0].nodes[1:]
	state.createdAt = time.Now()

	// The server reports FT.AGGREGATE and FT.CURSOR as readonly, which used
	// to send them to a random replica.
	readonly := map[string]*CommandInfo{
		"ft.aggregate": {Name: "ft.aggregate", ReadOnly: true},
		"ft.cursor":    {Name: "ft.cursor", ReadOnly: true},
		"get":          {Name: "get", ReadOnly: true, FirstKeyPos: 1},
	}
	c := &ClusterClient{
		opt: &ClusterOptions{
			ReadOnly:      true,
			RouteRandomly: true,
			ShardPicker:   &routing.RoundRobinPicker{},
		},
		state: newClusterStateHolder(
			func(context.Context) (*clusterState, error) { return state, nil },
			time.Hour,
		),
		cmdsInfoCache: newCmdsInfoCache(func(context.Context) (map[string]*CommandInfo, error) {
			return readonly, nil
		}),
	}
	c.state.state.Store(state)

	cursorCmds := func() []Cmder {
		return []Cmder{
			NewAggregateCmd(ctx, "FT.AGGREGATE", "idx", "*", "WITHCURSOR"),
			NewMapStringInterfaceCmd(ctx, "FT.CURSOR", "READ", "idx", 42),
			NewStatusCmd(ctx, "FT.CURSOR", "DEL", "idx", 42),
		}
	}

	for i := 0; i < 50; i++ {
		for _, cmd := range cursorCmds() {
			slot := c.cmdSlot(cmd, -1)
			node, err := c.cmdNode(ctx, cmd, slot)
			if err != nil || node != master {
				t.Fatalf("cmdNode(%v) = %p, %v; want master", cmd.Args(), node, err)
			}
			node, err = c.cmdNodeWithShardPicker(ctx, cmd, slot, c.opt.ShardPicker)
			if err != nil || node != master {
				t.Fatalf("cmdNodeWithShardPicker(%v) = %p, %v; want master", cmd.Args(), node, err)
			}
		}
	}

	// The same setup sends a plain readonly command to replicas, so the
	// checks above can fail.
	toReplica := false
	for i := 0; i < 50 && !toReplica; i++ {
		get := NewStringCmd(ctx, "get", "idx")
		node, err := c.cmdNode(ctx, get, c.cmdSlot(get, -1))
		if err != nil {
			t.Fatal(err)
		}
		toReplica = node != master
	}
	if !toReplica {
		t.Fatal("GET never reached a replica; the test does not exercise replica routing")
	}

	// A pipeline that contains a cursor command uses the master path.
	cmds := append([]Cmder{NewStringCmd(ctx, "get", "idx")}, cursorCmds()...)
	if c.cmdsAreReadOnly(ctx, cmds) {
		t.Fatal("cmdsAreReadOnly is true for a pipeline with cursor commands")
	}
	for i := 0; i < 20; i++ {
		cmdsMap := newCmdsMap()
		if err := c.mapCmdsByNode(ctx, cmdsMap, cursorCmds()); err != nil {
			t.Fatal(err)
		}
		for node := range cmdsMap.m {
			if node != master {
				t.Fatal("mapCmdsByNode sent a cursor command to a replica")
			}
		}
	}
}

// executeCursorCommand runs on the node process picked (index slot master or
// MOVED target) and does not check the cursor id type.
func TestExecuteCursorCommandUsesGivenNode(t *testing.T) {
	ctx := context.Background()
	c := &ClusterClient{opt: &ClusterOptions{}}

	if err := c.executeCursorCommand(ctx, NewCmd(ctx, "FT.CURSOR", "READ", "idx"), nil); err != errInvalidCursorCmdArgsCount {
		t.Fatalf("short FT.CURSOR: err %v, want errInvalidCursorCmdArgsCount", err)
	}

	var seen []interface{}
	node := &clusterNode{Client: NewClient(&Options{Addr: "127.0.0.1:0"})}
	t.Cleanup(func() { _ = node.Client.Close() })
	node.Client.AddHook(cursorCaptureHook{seen: &seen})

	for _, id := range []interface{}{42, int64(1 << 40), "42"} {
		cmd := NewMapStringInterfaceCmd(ctx, "FT.CURSOR", "READ", "idx", id)
		if err := c.executeCursorCommand(ctx, cmd, node); err != nil {
			t.Fatalf("id %T: %v", id, err)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("node received %d commands, want 3", len(seen))
	}
}

type cursorCaptureHook struct{ seen *[]interface{} }

func (cursorCaptureHook) DialHook(next DialHook) DialHook { return next }

func (h cursorCaptureHook) ProcessHook(ProcessHook) ProcessHook {
	return func(_ context.Context, cmd Cmder) error {
		*h.seen = append(*h.seen, cmd.Args()[3])
		return nil
	}
}

func (cursorCaptureHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}
