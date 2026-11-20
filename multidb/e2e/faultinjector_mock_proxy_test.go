package e2e

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	fi "github.com/redis/go-redis/v9/maintnotifications/e2e"
)

// recordingMechanism records Sever/Restore calls instead of touching docker.
type recordingMechanism struct {
	mu    sync.Mutex
	calls []string
}

func (*recordingMechanism) name() string { return "recording" }

func (m *recordingMechanism) Sever(_ context.Context, container string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "sever:"+container)
	return nil
}

func (m *recordingMechanism) Restore(_ context.Context, container, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "restore:"+container)
	return nil
}

func (m *recordingMechanism) seen() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

func TestMockProxyFailureStopStartRestart(t *testing.T) {
	mech := &recordingMechanism{}
	members := []MockMember{{Container: "m0", Addr: "127.0.0.1:1"}, {Container: "m1", Addr: "127.0.0.1:2"}}
	mock := NewMockFaultInjector(MockBDBID, members, mech)
	srv := mock.Start()
	defer srv.Close()
	c := fi.NewFaultInjectorClient(srv.URL)
	ctx := context.Background()

	run := func(op fi.ProxyFailureOp, cluster int) {
		t.Helper()
		resp, err := c.TriggerProxyFailure(ctx, fi.ProxyFailureRequest{BDBID: MockBDBID, ClusterIndex: cluster, Op: op})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		st, err := c.WaitForAction(ctx, resp.ActionID, fi.WithMaxWaitTime(10*time.Second), fi.WithPollInterval(10*time.Millisecond))
		if err != nil || st.Status != fi.StatusSuccess {
			t.Fatalf("%s: status %+v, err %v", op, st, err)
		}
	}

	run(fi.ProxyStop, 0)
	// A stop is not undone by the mock either: only the start restores it.
	if got := strings.Join(mech.seen(), ","); got != "sever:m0" {
		t.Fatalf("after stop: %q", got)
	}
	run(fi.ProxyStart, 0)
	run(fi.ProxyRestart, 1)
	want := "sever:m0,restore:m0,sever:m1,restore:m1"
	if got := strings.Join(mech.seen(), ","); got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}

func TestMockRejectsUnsupportedFaults(t *testing.T) {
	mock := NewMockFaultInjector(MockBDBID, []MockMember{{Container: "m0", Addr: "127.0.0.1:1"}}, &recordingMechanism{})
	srv := mock.Start()
	defer srv.Close()
	c := fi.NewFaultInjectorClient(srv.URL)
	ctx := context.Background()

	for name, req := range map[string]fi.ActionRequest{
		// The mock has no tc; it must refuse rather than pretend to inject latency.
		"network_latency": {Type: fi.ActionNetworkLatency, Parameters: map[string]interface{}{"bdb_id": "1", "delay_ms": 100, "duration": 5}},
		// A mock member is one container, not a node.
		"proxy_failure node_id": {Type: fi.ActionProxyFailure, Parameters: map[string]interface{}{"node_id": 1, "action": "stop"}},
		"proxy_failure bad op":  {Type: fi.ActionProxyFailure, Parameters: map[string]interface{}{"bdb_id": "1", "action": "explode"}},
		"proxy_failure bad idx": {Type: fi.ActionProxyFailure, Parameters: map[string]interface{}{"bdb_id": "1", "cluster_index": 9, "action": "stop"}},
	} {
		_, err := c.TriggerAction(ctx, req)
		if err == nil || !strings.Contains(err.Error(), "HTTP 400") {
			t.Errorf("%s: want an HTTP 400, got %v", name, err)
		}
	}
}
