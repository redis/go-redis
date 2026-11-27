package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordedRequest is one request the fake fault injector saw.
type recordedRequest struct {
	Method string
	Path   string
	Query  url.Values
	Body   string
}

// fakeFI answers like the real fault injector for the routes under test, and
// records what it was sent. Response bodies mirror the real service: bdb_id as
// a string, endpoints as redis:// URLs, extra fields the client must ignore.
type fakeFI struct {
	mu   sync.Mutex
	reqs []recordedRequest
	// setupBDBID is written verbatim into the setup response, so tests can
	// cover both a string and a numeric bdb_id.
	setupBDBID string
}

func (f *fakeFI) last(t *testing.T) recordedRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reqs) == 0 {
		t.Fatal("fake FI saw no request")
	}
	return f.reqs[len(f.reqs)-1]
}

func newFakeFI(t *testing.T) (*fakeFI, *FaultInjectorClient) {
	t.Helper()
	f := &fakeFI{setupBDBID: `"42"`}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.reqs = append(f.reqs, recordedRequest{r.Method, r.URL.Path, r.URL.Query(), string(body)})
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/action":
			_, _ = w.Write([]byte(`{"action_id":"a1","status":"pending"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/topology-change-standalone":
			_, _ = w.Write([]byte(`{"effect":"conn_drop","cluster":{"index":0,"nodes":3},"triggers":[
				{"name":"endpoint_rebind","description":"rebind","extra":1,"requirements":[
					{"config":"single","dbconfig":{"name":"db","port":12000,"replication":true},"mtls_files":null}]}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/topology-change-standalone/setup":
			_, _ = w.Write([]byte(`{"setup_id":"s1","bdb_id":` + f.setupBDBID + `,"effect":"conn_drop","trigger":"endpoint_rebind",
				"config":"single","cluster_index":0,"db_name":"db","endpoints":["redis://redis-12000.example.com:12000"],
				"password":"pw","tls":false,"mtls_files":null}`))
		case r.Method == http.MethodPost && r.URL.Path == "/topology-change-standalone":
			_, _ = w.Write([]byte(`{"action_id":"a2"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/topology-change-standalone/teardown":
			_, _ = w.Write([]byte(`{"bdb_id":"42","cluster_index":0}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return f, NewFaultInjectorClient(srv.URL)
}

func TestResiliencyProxyFailureRequest(t *testing.T) {
	f, c := newFakeFI(t)
	ctx := context.Background()

	resp, err := c.TriggerProxyFailure(ctx, ProxyFailureRequest{BDBID: 7, ClusterIndex: 1, Op: ProxyStop})
	if err != nil || resp.ActionID != "a1" {
		t.Fatalf("TriggerProxyFailure: %+v, %v", resp, err)
	}
	var got ActionRequest
	if err := json.Unmarshal([]byte(f.last(t).Body), &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "proxy_failure" {
		t.Errorf("type = %q", got.Type)
	}
	// bdb_id is a numeric string by convention; the others are literal.
	if got.Parameters["bdb_id"] != "7" || got.Parameters["action"] != "stop" || got.Parameters["cluster_index"] != float64(1) {
		t.Errorf("parameters = %v", got.Parameters)
	}
	if _, ok := got.Parameters["wait_for_active"]; ok {
		t.Errorf("wait_for_active must be omitted unless requested: %v", got.Parameters)
	}

	if _, err := c.TriggerProxyFailure(ctx, ProxyFailureRequest{BDBID: 7, Op: ProxyStart, WaitForActive: true}); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal([]byte(f.last(t).Body), &got)
	if got.Parameters["wait_for_active"] != true || got.Parameters["action"] != "start" {
		t.Errorf("start parameters = %v", got.Parameters)
	}

	// stop never reaches active, so waiting on it can only time out.
	if _, err := c.TriggerProxyFailure(ctx, ProxyFailureRequest{BDBID: 7, Op: ProxyStop, WaitForActive: true}); err == nil {
		t.Error("stop with wait_for_active must be refused")
	}
	if _, err := c.TriggerProxyFailure(ctx, ProxyFailureRequest{BDBID: 7, Op: "explode"}); err == nil {
		t.Error("unknown op must be refused")
	}
}

func TestResiliencyNetworkLatencyRequest(t *testing.T) {
	f, c := newFakeFI(t)
	ctx := context.Background()

	if _, err := c.TriggerBDBNetworkLatency(ctx, 7, 0, 3*time.Second, 0); err == nil {
		t.Fatal("a latency without a duration is permanent and must be refused")
	}
	if _, err := c.TriggerBDBNetworkLatency(ctx, 7, 0, 0, time.Minute); err == nil {
		t.Fatal("a zero delay must be refused")
	}
	if len(f.reqs) != 0 {
		t.Fatalf("refused requests must not reach the FI: %v", f.reqs)
	}

	if _, err := c.TriggerBDBNetworkLatency(ctx, 7, 2, 3*time.Second, 45*time.Second); err != nil {
		t.Fatal(err)
	}
	var got ActionRequest
	if err := json.Unmarshal([]byte(f.last(t).Body), &got); err != nil {
		t.Fatal(err)
	}
	p := got.Parameters
	if got.Type != "network_latency" || p["bdb_id"] != "7" || p["cluster_index"] != float64(2) ||
		p["delay_ms"] != float64(3000) || p["duration"] != float64(45) {
		t.Errorf("request = %+v", got)
	}

	// A positive sub-second duration is rounded up: truncating it to 0 would
	// make the latency permanent.
	if _, err := c.TriggerBDBNetworkLatency(ctx, 7, 0, 3*time.Second, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(f.last(t).Body), &got); err != nil {
		t.Fatal(err)
	}
	if got.Parameters["duration"] != float64(1) {
		t.Errorf("sub-second duration must round up to 1, parameters = %v", got.Parameters)
	}
}

func TestResiliencyStandaloneTopologyRoutes(t *testing.T) {
	f, c := newFakeFI(t)
	ctx := context.Background()

	triggers, err := c.GetStandaloneTriggers(ctx, StandaloneConnDrop, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r := f.last(t); r.Method != "GET" || r.Query.Get("effect") != "conn_drop" || r.Query.Get("cluster_index") != "0" {
		t.Errorf("GET request = %+v", r)
	}
	if triggers.Cluster.Nodes != 3 || len(triggers.Triggers) != 1 || triggers.Triggers[0].Name != "endpoint_rebind" ||
		len(triggers.Triggers[0].Requirements) != 1 || triggers.Triggers[0].Requirements[0].Config != "single" {
		t.Errorf("triggers = %+v", triggers)
	}

	setup, err := c.SetupTopologyChangeStandalone(ctx, StandaloneConnDrop, "endpoint_rebind", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r := f.last(t); r.Method != "POST" || r.Query.Get("trigger") != "endpoint_rebind" || r.Query.Get("variant_index") != "0" {
		t.Errorf("setup request = %+v", r)
	}
	if setup.SetupID != "s1" || setup.BDBID != "42" || setup.Password != "pw" || setup.TLS ||
		len(setup.Endpoints) != 1 || !strings.HasPrefix(setup.Endpoints[0], "redis://") {
		t.Errorf("setup = %+v", setup)
	}

	// The real service has sent bdb_id as a number too.
	f.setupBDBID = `42`
	setup, err = c.SetupTopologyChangeStandalone(ctx, StandaloneConnDrop, "endpoint_rebind", 0, 0)
	if err != nil || setup.BDBID != "42" {
		t.Fatalf("numeric bdb_id: %+v, %v", setup, err)
	}

	src := 1
	if _, err := c.TriggerTopologyChangeStandalone(ctx, StandaloneTriggerRequest{
		Effect: StandaloneConnDrop, Trigger: "endpoint_rebind", SetupID: "s1", SourceNode: &src, AutomaticallyCleanMM: true,
	}); err != nil {
		t.Fatal(err)
	}
	q := f.last(t).Query
	if q.Get("setup_id") != "s1" || q.Get("source_node") != "1" || q.Get("automatically_clean_mm") != "true" || q.Has("bdb_id") || q.Has("target_node") {
		t.Errorf("trigger query = %v", q)
	}
	if _, err := c.TriggerTopologyChangeStandalone(ctx, StandaloneTriggerRequest{Effect: StandaloneConnDrop, Trigger: "endpoint_rebind"}); err == nil {
		t.Error("a trigger with neither setup_id nor bdb_id must be refused")
	}

	if err := c.TeardownTopologyChangeStandalone(ctx, setup); err != nil {
		t.Fatal(err)
	}
	q = f.last(t).Query
	// Both ids: the FI registry is in memory, bdb_id still works after a restart.
	if q.Get("setup_id") != "s1" || q.Get("bdb_id") != "42" || q.Get("restore_nodes") != "true" {
		t.Errorf("teardown query = %v", q)
	}
	if err := c.TeardownTopologyChangeStandalone(ctx, nil); err == nil {
		t.Error("teardown of nothing must be refused")
	}
}

// Setup and teardown block server-side for minutes; the shared client's 30s
// timeout would cut them off, so they must use the long one.
func TestResiliencyLongClientOutlivesSharedTimeout(t *testing.T) {
	c := NewFaultInjectorClient("http://127.0.0.1:1")
	if got, short := c.longClient().httpClient.Timeout, c.httpClient.Timeout; got < 3*time.Minute || got <= short {
		t.Errorf("long client timeout %v, shared %v", got, short)
	}
	if c.httpClient.Timeout > time.Minute {
		t.Errorf("shared client timeout changed to %v", c.httpClient.Timeout)
	}
}
