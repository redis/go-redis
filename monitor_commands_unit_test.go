package redis

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
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

	want := "redis: MONITOR is not supported on a pipeline; run it on a dedicated client.EphemeralConn()"
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

func TestMonitorCmd_RearmsCloseConn(t *testing.T) {
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "monitor")
	firstClosed := make(chan struct{})
	secondClosed := make(chan struct{})
	cmd.setActiveConn(func() error {
		close(firstClosed)
		return nil
	})
	cmd.close()
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("first monitor connection was not closed")
	}

	cmd.setActiveConn(func() error {
		close(secondClosed)
		return nil
	})
	cmd.close()
	select {
	case <-secondClosed:
	case <-time.After(time.Second):
		t.Fatal("rearmed monitor connection was not closed")
	}
}

func TestMonitorCmdTimeoutIsUnbounded(t *testing.T) {
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "monitor")
	if got := (&baseClient{}).cmdTimeout(cmd); got != 0 {
		t.Fatalf("monitor timeout = %s, want zero timeout", got)
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

func TestPipeline_RejectsWrappedMonitor(t *testing.T) {
	pipe := &Pipeline{}
	cmd := &wrappedMonitorCmd{MonitorCmd: NewMonitorCmd(context.Background(), make(chan string, 1), "IMONITOR")}
	if err := pipe.BatchProcess(context.Background(), cmd); err == nil {
		t.Fatal("Pipeline.BatchProcess accepted a wrapped MonitorCmd")
	}
	if pipe.Len() != 0 {
		t.Fatalf("Pipeline queued %d commands, want 0", pipe.Len())
	}
}

type wrappedMonitorCmd struct {
	*MonitorCmd
}

func TestMonitorCmd_StopDoesNotReportClosedConnection(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "monitor")
	cmd.setActiveConn(client.Close)
	cmd.Start()
	if err := cmd.readReply(proto.NewReader(client)); err != nil {
		t.Fatalf("readReply error = %v", err)
	}

	cmd.Stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cmd.Err() != nil {
			t.Fatalf("MonitorCmd error after Stop = %v", cmd.Err())
		}
		cmd.mu.Lock()
		closed := cmd.closeConn == nil
		cmd.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("monitor reader did not stop")
}

func TestSentinelClientRejectsMonitor(t *testing.T) {
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "monitor")
	client := &SentinelClient{}
	err := client.Process(context.Background(), cmd)
	want := "redis: MONITOR is not supported on SentinelClient; use a dedicated client"
	if err == nil || err.Error() != want {
		t.Fatalf("Process error = %v, want %q", err, want)
	}
	if got := cmd.Err(); got == nil || got.Error() != want {
		t.Fatalf("command error = %v, want %q", got, want)
	}
}

type monitorErrorHook struct{}

func (monitorErrorHook) DialHook(next DialHook) DialHook { return next }

func (monitorErrorHook) ProcessHook(ProcessHook) ProcessHook {
	return func(context.Context, Cmder) error { return errors.New("monitor hook error") }
}

func (monitorErrorHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func TestClientMonitor_PreservesHookError(t *testing.T) {
	client := NewClient(&Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	client.AddHook(monitorErrorHook{})

	cmd := client.Monitor(context.Background(), make(chan string, 1))
	if got := cmd.Err(); got == nil || got.Error() != "monitor hook error" {
		t.Fatalf("Monitor command error = %v, want hook error", got)
	}
}

func TestMonitorCmd_RejectsOverlappingCloseConn(t *testing.T) {
	cmd := NewMonitorCmd(context.Background(), make(chan string, 1), "monitor")
	if !cmd.setActiveConn(func() error { return nil }) {
		t.Fatal("first monitor connection was rejected")
	}
	if cmd.setActiveConn(func() error { return nil }) {
		t.Fatal("overlapping monitor connection was accepted")
	}
	cmd.clearCloseConn()
}

func TestMonitorCmd_ReadReplyWithInitial(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	ch := make(chan string, 1)
	cmd := NewMonitorCmd(context.Background(), ch, "monitor")
	cmd.setActiveConn(client.Close)
	cmd.Start()
	if err := cmd.readReplyWithInitial(proto.NewReader(client), "OK"); err != nil {
		t.Fatalf("readReplyWithInitial returned error: %v", err)
	}

	select {
	case got := <-ch:
		if got != "OK" {
			t.Fatalf("initial monitor reply = %q, want %q", got, "OK")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial monitor reply")
	}

	cmd.Stop()
}
