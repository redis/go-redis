package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// NetworkFaultMechanism severs and restores a proxy container's
// reachability, standing in for the real fault-injector's node-level
// network block. Two implementations exist because they are not
// equivalent: only one reproduces the real action's hang-on-established-
// connection behavior. See the design doc
// (~/work/client-designs/active-active/go-redis-e2e-cae-integration.md)
// for the tradeoff.
type NetworkFaultMechanism interface {
	name() string
	// Sever must return once new connections to container stop succeeding.
	Sever(ctx context.Context, container string) error
	// Restore must return once container is reachable again.
	Restore(ctx context.Context, container string, addr string) error
}

// DockerStopStartMechanism is the default: the listener dies, so clients
// see a refused connection / RST rather than a hang. Proven in PR #3952.
// Cannot reproduce the real action's blackhole-not-refuse behavior.
type DockerStopStartMechanism struct{}

func (DockerStopStartMechanism) name() string { return "docker-stop-start" }

func (DockerStopStartMechanism) Sever(ctx context.Context, container string) error {
	return RunDocker(ctx, "stop", "-t", "0", container)
}

func (DockerStopStartMechanism) Restore(ctx context.Context, container, addr string) error {
	if err := RunDocker(ctx, "start", container); err != nil {
		return err
	}
	return AwaitTCPReachable(ctx, addr, 30*time.Second)
}

// DockerIptablesMechanism blackholes traffic inside the container
// (iptables DROP on INPUT/FORWARD, matching the real fault-injector's own
// mechanism verified from re_fault_injector/actions/network_failure.py)
// instead of killing the listener, so established connections hang and new
// dials hang too, rather than being refused.
//
// NOT the default: it requires `iptables` to be present in the
// redislabs/client-resp-proxy image and `cap_add: [NET_ADMIN]` on the
// compose service, neither of which has been verified in an environment
// with a live Docker daemon. Opt in via MULTIDB_E2E_FI_MECHANISM=iptables
// once both are confirmed (see the design doc's verification command).
type DockerIptablesMechanism struct{}

func (DockerIptablesMechanism) name() string { return "docker-iptables" }

func (DockerIptablesMechanism) Sever(ctx context.Context, container string) error {
	return RunDocker(ctx, "exec", container, "iptables", "-P", "INPUT", "DROP", "-P", "FORWARD", "DROP")
}

func (DockerIptablesMechanism) Restore(ctx context.Context, container, addr string) error {
	if err := RunDocker(ctx, "exec", container, "iptables", "-P", "INPUT", "ACCEPT", "-P", "FORWARD", "ACCEPT"); err != nil {
		return err
	}
	return AwaitTCPReachable(ctx, addr, 30*time.Second)
}

// SelectNetworkFaultMechanism reads MULTIDB_E2E_FI_MECHANISM (default:
// docker-stop-start).
func SelectNetworkFaultMechanism(env func(string) string) NetworkFaultMechanism {
	switch env("MULTIDB_E2E_FI_MECHANISM") {
	case "iptables":
		return DockerIptablesMechanism{}
	default:
		return DockerStopStartMechanism{}
	}
}

func RunDocker(ctx context.Context, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %v: %w: %s", args, err, out)
	}
	return nil
}

func AwaitTCPReachable(ctx context.Context, addr string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond); err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("AwaitTCPReachable(%s): %w", addr, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// MockMember is one entry in the mock's static (bdb_id, cluster_index) ->
// container/addr mapping.
type MockMember struct {
	Container string
	Addr      string
}

// mockAction tracks one submitted action's async execution.
type mockAction struct {
	status string // pending|running|success|failed
	err    string
}

// MockFaultInjector implements the real fault-injector's wire contract
// (POST /action, GET /action/{id}) for ActionNetworkFailure only, backed by
// NetworkFaultMechanism against the local compose Topology. It is
// deliberately NOT an extension of maintnotifications/e2e's
// proxy_fault_injector_server.go: that server's generic HTTP/job-tracking
// shell is reusable in principle, but its executeAction cases are
// maintenance-notification-specific (RESP3 push-frame injection), and
// coupling two independently-gated e2e suites to share ~80 lines of
// boilerplate is a worse trade than a small dedicated implementation.
type MockFaultInjector struct {
	mech    NetworkFaultMechanism
	bdbID   int
	members []MockMember // index = cluster_index

	mu      sync.Mutex
	actions map[string]*mockAction
}

func NewMockFaultInjector(bdbID int, members []MockMember, mech NetworkFaultMechanism) *MockFaultInjector {
	return &MockFaultInjector{
		mech:    mech,
		bdbID:   bdbID,
		members: members,
		actions: map[string]*mockAction{},
	}
}

// Start launches the mock on an ephemeral local port and returns its base
// URL. It is intentionally test-process-only (no separate binary/image, per
// the design doc's stated tradeoff) — it runs for exactly the lifetime of
// the test process, started from TestMain.
func (s *MockFaultInjector) Start() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/action", s.handleAction)
	mux.HandleFunc("/action/", s.handleActionStatus)
	return httptest.NewServer(mux)
}

func (s *MockFaultInjector) handleAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req triggerActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)
		return
	}
	if ActionType(req.Type) != ActionNetworkFailure {
		http.Error(w, fmt.Sprintf("unsupported action type %q", req.Type), http.StatusBadRequest)
		return
	}
	// Re-marshal/unmarshal Parameters (decoded as map[string]interface{} by
	// the generic request shape) into the typed params.
	raw, err := json.Marshal(req.Parameters)
	if err != nil {
		http.Error(w, fmt.Sprintf("re-marshal parameters: %v", err), http.StatusInternalServerError)
		return
	}
	var params NetworkFailureParams
	if err := json.Unmarshal(raw, &params); err != nil {
		http.Error(w, fmt.Sprintf("decode parameters: %v", err), http.StatusBadRequest)
		return
	}
	if params.ClusterIndex < 0 || params.ClusterIndex >= len(s.members) {
		http.Error(w, fmt.Sprintf("cluster_index %d out of range [0,%d)", params.ClusterIndex, len(s.members)), http.StatusBadRequest)
		return
	}
	if params.Delay <= 0 {
		params.Delay = 1
	}

	id := newActionID()
	s.mu.Lock()
	s.actions[id] = &mockAction{status: "pending"}
	s.mu.Unlock()

	member := s.members[params.ClusterIndex]
	go s.execute(id, member, params.Delay)

	writeJSON(w, http.StatusOK, triggerActionResponse{ActionID: id})
}

func (s *MockFaultInjector) execute(id string, member MockMember, delaySeconds int) {
	s.setStatus(id, "running", "")

	ctx := context.Background()
	if err := s.mech.Sever(ctx, member.Container); err != nil {
		s.setStatus(id, "failed", fmt.Sprintf("sever %s: %v", member.Container, err))
		return
	}

	time.Sleep(time.Duration(delaySeconds) * time.Second)

	if err := s.mech.Restore(ctx, member.Container, member.Addr); err != nil {
		s.setStatus(id, "failed", fmt.Sprintf("restore %s: %v", member.Container, err))
		return
	}
	s.setStatus(id, "success", "")
}

func (s *MockFaultInjector) setStatus(id, status, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.actions[id]; ok {
		a.status = status
		a.err = errMsg
	}
}

func (s *MockFaultInjector) handleActionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/action/")
	s.mu.Lock()
	a, ok := s.actions[id]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "action not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, ActionStatus{Status: a.status, Error: a.err})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newActionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "mock-" + hex.EncodeToString(b)
}
