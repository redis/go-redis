package e2e

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

// Checks the standalone scenario harness itself against a fake fault injector,
// so harness bugs show up in a minute here instead of in a real-cluster run.
//
// The fake serves the real routes with the real response shapes. Its "topology
// change" drops every client connection of a local Redis by running
// E2E_FAKE_FI_DROP_CMD, which stands in for the endpoint rebind. Skipped
// unless both variables are set:
//
//	E2E_FAKE_FI_ENDPOINT=redis://127.0.0.1:17100
//	E2E_FAKE_FI_DROP_CMD='docker exec e2e-fm-redis redis-cli client kill type normal skipme yes'
func TestStandaloneScenarioHarnessAgainstFakeFI(t *testing.T) {
	endpoint := os.Getenv("E2E_FAKE_FI_ENDPOINT")
	dropCmd := os.Getenv("E2E_FAKE_FI_DROP_CMD")
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" || endpoint == "" || dropCmd == "" {
		t.Skip("set E2E_SCENARIO_TESTS, E2E_FAKE_FI_ENDPOINT and E2E_FAKE_FI_DROP_CMD to run")
	}

	var mu sync.Mutex
	counts := map[string]int{}
	count := func(k string) {
		mu.Lock()
		counts[k]++
		mu.Unlock()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/topology-change-standalone", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"effect":"conn_drop","cluster":{"index":0,"nodes":3},"triggers":[
				{"name":"endpoint_rebind","description":"d","requirements":[
					{"config":"single_tls","dbconfig":{"name":"tls"}},
					{"config":"single","dbconfig":{"name":"plain"}}]}]}`))
			return
		}
		count("trigger")
		if r.URL.Query().Get("setup_id") != "s1" {
			http.Error(w, "unknown setup", http.StatusNotFound)
			return
		}
		go func() {
			parts := strings.Fields(dropCmd)
			_ = exec.Command(parts[0], parts[1:]...).Run()
		}()
		_, _ = w.Write([]byte(`{"action_id":"act-1"}`))
	})
	mux.HandleFunc("/topology-change-standalone/setup", func(w http.ResponseWriter, r *http.Request) {
		count("setup")
		// The harness must skip the TLS config and ask for the plain one.
		if r.URL.Query().Get("variant_index") != "1" {
			http.Error(w, "wrong variant", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"setup_id":"s1","bdb_id":"7","effect":"conn_drop","trigger":"endpoint_rebind","config":"single",
			"cluster_index":0,"db_name":"plain","endpoints":["` + endpoint + `"],"password":null,"tls":false}`))
	})
	mux.HandleFunc("/topology-change-standalone/teardown", func(w http.ResponseWriter, r *http.Request) {
		count("teardown")
		q := r.URL.Query()
		if q.Get("setup_id") != "s1" || q.Get("bdb_id") != "7" {
			http.Error(w, "teardown needs both ids", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bdb_id":"7","cluster_index":0}`))
	})
	mux.HandleFunc("/action/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"action_id":"act-1","status":"success"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var observed []standaloneResult
	standaloneObserve = func(_ StandaloneEffect, _ string, r standaloneResult) { observed = append(observed, r) }
	defer func() { standaloneObserve = nil }()

	t.Run("scenario", func(t *testing.T) {
		runStandaloneEffect(t, NewFaultInjectorClient(srv.URL), StandaloneConnDrop)
	})

	// The fake dropped every connection, so the harness must have seen it:
	// otherwise a passing run proves nothing about recovery.
	if len(observed) != 1 || !observed[0].Reconnected {
		t.Errorf("observed %+v, want one run that reconnected", observed)
	}

	mu.Lock()
	defer mu.Unlock()
	if counts["setup"] != 1 || counts["trigger"] != 1 || counts["teardown"] != 1 {
		t.Errorf("calls = %v, want one setup, trigger and teardown each", counts)
	}
}
