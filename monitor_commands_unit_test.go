package redis

import (
	"context"
	"reflect"
	"testing"
)

type monitorMethods interface {
	Monitor(ctx context.Context, ch chan string) *MonitorCmd
}

func TestMonitor_Args(t *testing.T) {
	cmd := cmdable(captureCmdable(new(Cmder))).Monitor(context.Background(), make(chan string, 1))
	if cmd == nil {
		t.Fatal("Monitor returned nil")
	}

	want := []interface{}{"monitor"}
	if !reflect.DeepEqual(cmd.Args(), want) {
		t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), want)
	}
}

func TestMonitorSurface(t *testing.T) {
	for name, impl := range map[string]any{
		"Client":        (*Client)(nil),
		"Conn":          (*Conn)(nil),
		"Tx":            (*Tx)(nil),
		"Ring":          (*Ring)(nil),
		"ClusterClient": (*ClusterClient)(nil),
		"AutoPipeliner": (*AutoPipeliner)(nil),
	} {
		if _, ok := impl.(monitorMethods); !ok {
			t.Fatalf("%s must expose Monitor", name)
		}
	}
}

func TestMonitor_PipelineShadow(t *testing.T) {
	cmd := (&Pipeline{}).Monitor(context.Background(), make(chan string, 1))
	if cmd == nil {
		t.Fatal("Pipeline.Monitor returned nil")
	}

	want := "redis: MONITOR is not supported on a pipeline; run it on a dedicated client.Conn()"
	if got := cmd.Err(); got == nil || got.Error() != want {
		t.Fatalf("Pipeline.Monitor error = %v, want %q", got, want)
	}
}

func TestNewMonitorCmd_RequiresArgs(t *testing.T) {
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1))
	want := "redis: monitor command requires at least one argument"
	if got := cmd.Err(); got == nil || got.Error() != want {
		t.Fatalf("NewMonitorCmd error = %v, want %q", got, want)
	}
}

func TestMonitorCmdClone_PreservesArgs(t *testing.T) {
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "IMONITOR", "node-1")

	cloned, ok := cmd.Clone().(*MonitorCmd)
	if !ok {
		t.Fatalf("Clone returned %T, want *MonitorCmd", cmd.Clone())
	}

	want := []interface{}{"IMONITOR", "node-1"}
	if !reflect.DeepEqual(cloned.Args(), want) {
		t.Errorf("clone args mismatch\n got: %#v\nwant: %#v", cloned.Args(), want)
	}
}

func TestNewMonitorCmd_CustomCommand(t *testing.T) {
	cmd := NewMonitorCmd(
		context.Background(),
		make(chan string, 1),
		"IMONITOR",
		"node-1",
	)

	want := []interface{}{"IMONITOR", "node-1"}
	if !reflect.DeepEqual(cmd.Args(), want) {
		t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), want)
	}
}

func TestNewMonitorCmd_CustomCommandIsKeyless(t *testing.T) {
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "IMONITOR", "node-1")
	if got := cmdFirstKeyPosWithInfo(cmd, nil); got != 0 {
		t.Fatalf("custom monitor first key pos = %d, want 0", got)
	}
}

func TestNewMonitorCmd_ProcessPreservesError(t *testing.T) {
	client := NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer client.Close()

	cmd := NewMonitorCmd(context.Background(), make(chan string, 1))
	err := client.Process(context.Background(), cmd)
	want := "redis: monitor command requires at least one argument"
	if err == nil || err.Error() != want {
		t.Fatalf("Process error = %v, want %q", err, want)
	}
	if got := cmd.Err(); got == nil || got.Error() != want {
		t.Fatalf("command error = %v, want %q", got, want)
	}
}

func TestMonitorCmd_ClusterProcessPreservesError(t *testing.T) {
	client := &ClusterClient{}
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1))
	if err := client.process(context.Background(), cmd); err == nil || err.Error() != "redis: monitor command requires at least one argument" {
		t.Fatalf("ClusterClient.process error = %v, want constructor error", err)
	}
}

func TestMonitorCmd_RingProcessPreservesError(t *testing.T) {
	client := &Ring{}
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1))
	if err := client.process(context.Background(), cmd); err == nil || err.Error() != "redis: monitor command requires at least one argument" {
		t.Fatalf("Ring.process error = %v, want constructor error", err)
	}
}

func TestPipeline_RejectsCustomMonitor(t *testing.T) {
	pipe := &Pipeline{}
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "IMONITOR", "node-1")
	if err := pipe.Process(context.Background(), cmd); err == nil {
		t.Fatal("Pipeline.Process accepted a MonitorCmd")
	}
	if pipe.Len() != 0 {
		t.Fatalf("Pipeline queued %d commands, want 0", pipe.Len())
	}
}

func TestPipeline_RejectsBatchedCustomMonitor(t *testing.T) {
	pipe := &Pipeline{}
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "IMONITOR", "node-1")
	if err := pipe.BatchProcess(context.Background(), cmd); err == nil {
		t.Fatal("Pipeline.BatchProcess accepted a MonitorCmd")
	}
	if pipe.Len() != 0 {
		t.Fatalf("Pipeline queued %d commands, want 0", pipe.Len())
	}
}
