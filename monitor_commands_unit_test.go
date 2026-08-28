package redis

import (
	"context"
	"reflect"
	"testing"
)

func TestMonitor_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	cmd := c.Monitor(context.Background(), make(chan string, 1))
	if cmd == nil {
		t.Fatal("Monitor returned nil")
	}

	want := []interface{}{"monitor"}
	if !reflect.DeepEqual(cmd.Args(), want) {
		t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), want)
	}
}

func TestMonitorWithArgs_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	cmd := c.MonitorWithArgs(context.Background(), make(chan string, 1), "127.0.0.1:6379")
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
