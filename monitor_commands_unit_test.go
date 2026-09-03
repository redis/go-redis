package redis

import (
	"context"
	"reflect"
	"testing"
)

type monitorMethods interface {
	Monitor(ctx context.Context, ch chan string) *MonitorCmd
	MonitorWithArgs(ctx context.Context, ch chan string, args ...string) *MonitorCmd
}

var _ monitorMethods = (UniversalClient)(nil)

func TestMonitor_Args(t *testing.T) {
	cmd := monitor(captureCmdable(new(Cmder)), context.Background(), make(chan string, 1))
	if cmd == nil {
		t.Fatal("Monitor returned nil")
	}

	want := []interface{}{"monitor"}
	if !reflect.DeepEqual(cmd.Args(), want) {
		t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), want)
	}
}

func TestMonitorSurfaceExcludesPipeline(t *testing.T) {
	if _, ok := any(&Pipeline{}).(monitorMethods); ok {
		t.Fatal("Pipeline unexpectedly exposes Monitor methods")
	}

	var pipe Pipeliner = &Pipeline{}
	if _, ok := any(pipe).(monitorMethods); ok {
		t.Fatal("Pipeliner unexpectedly exposes Monitor methods")
	}

	for name, impl := range map[string]any{
		"Client":        (*Client)(nil),
		"Conn":          (*Conn)(nil),
		"Tx":            (*Tx)(nil),
		"Ring":          (*Ring)(nil),
		"ClusterClient": (*ClusterClient)(nil),
		"AutoPipeliner": (*AutoPipeliner)(nil),
	} {
		if _, ok := impl.(monitorMethods); !ok {
			t.Fatalf("%s must expose Monitor methods", name)
		}
	}
}

func TestMonitorWithArgs_Args(t *testing.T) {
	cmd := monitor(captureCmdable(new(Cmder)), context.Background(), make(chan string, 1), "127.0.0.1:6379")
	if cmd == nil {
		t.Fatal("MonitorWithArgs returned nil")
	}

	want := []interface{}{"monitor", "127.0.0.1:6379"}
	if !reflect.DeepEqual(cmd.Args(), want) {
		t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), want)
	}
}

func TestMonitorCmdClone_PreservesArgs(t *testing.T) {
	cmd := newMonitorCmd(context.Background(), make(chan string, 1), "127.0.0.1:6379")

	cloned, ok := cmd.Clone().(*MonitorCmd)
	if !ok {
		t.Fatalf("Clone returned %T, want *MonitorCmd", cmd.Clone())
	}

	want := []interface{}{"monitor", "127.0.0.1:6379"}
	if !reflect.DeepEqual(cloned.Args(), want) {
		t.Errorf("clone args mismatch\n got: %#v\nwant: %#v", cloned.Args(), want)
	}
}
