package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client methods for the fault-injector actions and routes behind the
// resiliency scenario matrix (the "Enhance Fault Injector with Resiliency
// scenarios" plan): proxy_failure, network_latency and the
// /topology-change-standalone route family. Parameter shapes follow
// re_fault_injector's action_params.py and rest_api.py.

const (
	// ActionProxyFailure stops, starts or restarts the DMC proxy.
	ActionProxyFailure ActionType = "proxy_failure"
)

// ProxyFailureOp is what proxy_failure does to the proxy process.
type ProxyFailureOp string

const (
	ProxyStop    ProxyFailureOp = "stop"
	ProxyStart   ProxyFailureOp = "start"
	ProxyRestart ProxyFailureOp = "restart"
)

// ProxyFailureRequest targets the proxy on every node hosting BDBID.
// Targeting a single node (node_id) is deliberately not exposed: it is not
// needed for client scenarios, and the proxy of a shared cluster node serves
// other databases too.
type ProxyFailureRequest struct {
	BDBID        int
	ClusterIndex int
	Op           ProxyFailureOp
	// WaitForActive blocks the action until the database is active and its
	// endpoint available again. Only meaningful for start and restart: after
	// stop the database never becomes active.
	WaitForActive bool
}

// TriggerProxyFailure runs a proxy_failure action. A stop is NOT undone by the
// fault injector: the caller must send a start (see ProxyStart).
func (c *FaultInjectorClient) TriggerProxyFailure(ctx context.Context, req ProxyFailureRequest) (*ActionResponse, error) {
	switch req.Op {
	case ProxyStop, ProxyStart, ProxyRestart:
	default:
		return nil, fmt.Errorf("proxy_failure: invalid op %q", req.Op)
	}
	if req.WaitForActive && req.Op == ProxyStop {
		return nil, errors.New("proxy_failure: wait_for_active cannot be combined with stop")
	}
	params := map[string]interface{}{
		"bdb_id":        strconv.Itoa(req.BDBID),
		"cluster_index": req.ClusterIndex,
		"action":        string(req.Op),
	}
	if req.WaitForActive {
		params["wait_for_active"] = true
	}
	return c.TriggerAction(ctx, ActionRequest{Type: ActionProxyFailure, Parameters: params})
}

// TriggerBDBNetworkLatency adds delay to every node hosting BDBID with tc/netem
// for duration, then the node removes it by itself. A duration is required:
// without one the latency is permanent and only removable over SSH, so this
// refuses to send such a request. (SimulateNetworkLatency is the older,
// node-addressed variant and takes different parameters.)
func (c *FaultInjectorClient) TriggerBDBNetworkLatency(ctx context.Context, bdbID, clusterIndex int, delay, duration time.Duration) (*ActionResponse, error) {
	if delay <= 0 {
		return nil, errors.New("network_latency: delay must be positive")
	}
	if duration <= 0 {
		return nil, errors.New("network_latency: duration must be positive, a latency without one is permanent")
	}
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNetworkLatency,
		Parameters: map[string]interface{}{
			"bdb_id":        strconv.Itoa(bdbID),
			"cluster_index": clusterIndex,
			"delay_ms":      int(delay / time.Millisecond),
			"duration":      int(duration / time.Second),
		},
	})
}

// StandaloneEffect is a client-observable effect of /topology-change-standalone.
type StandaloneEffect string

const (
	StandaloneDataMovementConnDrop   StandaloneEffect = "data_movement_conn_drop"
	StandaloneDataMovementNoConnDrop StandaloneEffect = "data_movement_no_conn_drop"
	StandaloneConnDrop               StandaloneEffect = "conn_drop"
	StandaloneDNSResolutionChange    StandaloneEffect = "dns_resolution_change"
)

// StandaloneEffects lists every effect, in matrix order.
var StandaloneEffects = []StandaloneEffect{
	StandaloneDataMovementConnDrop,
	StandaloneDataMovementNoConnDrop,
	StandaloneConnDrop,
	StandaloneDNSResolutionChange,
}

// StandaloneTrigger is one mechanism for an effect, as listed by the FI.
type StandaloneTrigger struct {
	Name         string                  `json:"name"`
	Description  string                  `json:"description"`
	Requirements []StandaloneRequirement `json:"requirements"`
}

// StandaloneRequirement is one database config that supports a trigger.
// DBConfig stays raw: it is passed to create_database server-side and its
// shape is not this client's business.
type StandaloneRequirement struct {
	Config   string          `json:"config"`
	DBConfig json.RawMessage `json:"dbconfig"`
}

// StandaloneTriggersResponse is the body of GET /topology-change-standalone.
type StandaloneTriggersResponse struct {
	Effect   StandaloneEffect    `json:"effect"`
	Triggers []StandaloneTrigger `json:"triggers"`
	Cluster  struct {
		Index int `json:"index"`
		Nodes int `json:"nodes"`
	} `json:"cluster"`
}

// StandaloneSetup is the body of POST /topology-change-standalone/setup. The FI
// owns the database: BDBID is a string on the wire in some versions and a
// number in others, so it is kept as the decoded text.
type StandaloneSetup struct {
	SetupID      string
	BDBID        string
	Effect       StandaloneEffect
	Trigger      string
	Config       string
	ClusterIndex int
	DBName       string
	// Endpoints are redis:// or rediss:// URLs.
	Endpoints []string
	Password  string
	TLS       bool
}

// UnmarshalJSON accepts a numeric or string bdb_id and ignores unknown fields.
func (s *StandaloneSetup) UnmarshalJSON(b []byte) error {
	var raw struct {
		SetupID      string           `json:"setup_id"`
		BDBID        json.RawMessage  `json:"bdb_id"`
		Effect       StandaloneEffect `json:"effect"`
		Trigger      string           `json:"trigger"`
		Config       string           `json:"config"`
		ClusterIndex int              `json:"cluster_index"`
		DBName       string           `json:"db_name"`
		Endpoints    []string         `json:"endpoints"`
		Password     string           `json:"password"`
		TLS          bool             `json:"tls"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	s.SetupID, s.Effect, s.Trigger, s.Config = raw.SetupID, raw.Effect, raw.Trigger, raw.Config
	s.ClusterIndex, s.DBName, s.Endpoints, s.Password, s.TLS = raw.ClusterIndex, raw.DBName, raw.Endpoints, raw.Password, raw.TLS
	s.BDBID = strings.Trim(string(raw.BDBID), `"`)
	if s.BDBID == "null" {
		s.BDBID = ""
	}
	return nil
}

// StandaloneTriggerRequest runs a topology change on a database from
// SetupTopologyChangeStandalone (SetupID) or an explicit BDBID.
type StandaloneTriggerRequest struct {
	Effect       StandaloneEffect
	Trigger      string
	SetupID      string
	BDBID        string
	ClusterIndex int
	SourceNode   *int
	TargetNode   *int
	// ReshardFactor is the TARGET master shard count for the reshard trigger.
	ReshardFactor *int
	// AutomaticallyCleanMM turns maintenance mode off again after 10s
	// (maintenance_mode trigger only).
	AutomaticallyCleanMM bool
}

// setupTimeout bounds the calls that block until the FI finished provisioning
// or deleting a database: the server waits up to 180s for the create job, and
// teardown waits for both its delete and restore jobs.
const setupTimeout = 5 * time.Minute

// longClient is the FI client with a timeout that fits blocking setup and
// teardown calls. The shared 30s client would cut them off.
func (c *FaultInjectorClient) longClient() *FaultInjectorClient {
	lc := *c
	lc.httpClient = &http.Client{Timeout: setupTimeout}
	return &lc
}

// GetStandaloneTriggers lists the triggers (and the database configs that
// support them) for an effect. An empty list means the cluster cannot run it,
// for example because it has fewer than 3 nodes.
func (c *FaultInjectorClient) GetStandaloneTriggers(ctx context.Context, effect StandaloneEffect, clusterIndex int) (*StandaloneTriggersResponse, error) {
	q := url.Values{}
	q.Set("effect", string(effect))
	q.Set("cluster_index", strconv.Itoa(clusterIndex))
	var resp StandaloneTriggersResponse
	err := c.request(ctx, http.MethodGet, "/topology-change-standalone?"+q.Encode(), nil, &resp)
	return &resp, err
}

// SetupTopologyChangeStandalone provisions a database for effect/trigger and
// blocks until it is ready. variantIndex selects the entry of the trigger's
// requirements list.
func (c *FaultInjectorClient) SetupTopologyChangeStandalone(ctx context.Context, effect StandaloneEffect, trigger string, clusterIndex, variantIndex int) (*StandaloneSetup, error) {
	q := url.Values{}
	q.Set("effect", string(effect))
	q.Set("trigger", trigger)
	q.Set("cluster_index", strconv.Itoa(clusterIndex))
	q.Set("variant_index", strconv.Itoa(variantIndex))
	var resp StandaloneSetup
	err := c.longClient().request(ctx, http.MethodPost, "/topology-change-standalone/setup?"+q.Encode(), nil, &resp)
	return &resp, err
}

// TriggerTopologyChangeStandalone starts the topology change and returns the
// action to poll with WaitForAction.
func (c *FaultInjectorClient) TriggerTopologyChangeStandalone(ctx context.Context, req StandaloneTriggerRequest) (*ActionResponse, error) {
	if req.SetupID == "" && req.BDBID == "" {
		return nil, errors.New("topology-change-standalone: SetupID or BDBID is required")
	}
	q := url.Values{}
	q.Set("effect", string(req.Effect))
	q.Set("trigger", req.Trigger)
	if req.SetupID != "" {
		q.Set("setup_id", req.SetupID)
	}
	if req.BDBID != "" {
		q.Set("bdb_id", req.BDBID)
	}
	q.Set("cluster_index", strconv.Itoa(req.ClusterIndex))
	if req.SourceNode != nil {
		q.Set("source_node", strconv.Itoa(*req.SourceNode))
	}
	if req.TargetNode != nil {
		q.Set("target_node", strconv.Itoa(*req.TargetNode))
	}
	if req.ReshardFactor != nil {
		q.Set("reshard_factor", strconv.Itoa(*req.ReshardFactor))
	}
	if req.AutomaticallyCleanMM {
		q.Set("automatically_clean_mm", "true")
	}
	var resp ActionResponse
	err := c.request(ctx, http.MethodPost, "/topology-change-standalone?"+q.Encode(), nil, &resp)
	return &resp, err
}

// TeardownTopologyChangeStandalone deletes the database and restores node state
// (maintenance mode off, max_listeners reset). It sends both setup_id and
// bdb_id: the FI keeps the setup registry in memory, so after a restart only
// bdb_id still resolves. Blocks until the FI finished.
func (c *FaultInjectorClient) TeardownTopologyChangeStandalone(ctx context.Context, s *StandaloneSetup) error {
	if s == nil || (s.SetupID == "" && s.BDBID == "") {
		return errors.New("topology-change-standalone: nothing to tear down")
	}
	q := url.Values{}
	if s.SetupID != "" {
		q.Set("setup_id", s.SetupID)
	}
	if s.BDBID != "" {
		q.Set("bdb_id", s.BDBID)
	}
	q.Set("cluster_index", strconv.Itoa(s.ClusterIndex))
	q.Set("restore_nodes", "true")
	return c.longClient().request(ctx, http.MethodPost, "/topology-change-standalone/teardown?"+q.Encode(), nil, nil)
}
