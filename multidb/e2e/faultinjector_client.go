package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ActionType is a fault-injector action name. It matches the wire protocol
// of the real Redis Enterprise fault-injector service used by
// redis-developer/cae-client-testing (re_fault_injector) so the same test
// code drives either the real service or the local MockFaultInjector
// (faultinjector_mock.go) by baseURL alone.
type ActionType string

// ActionNetworkFailure is the only action this suite uses. Verified against
// the real service's source (re_fault_injector/actions/network_failure.py):
// it blocks ingress/egress on every node hosting BDBID within the cluster
// selected by ClusterIndex, for Delay seconds, then restores automatically.
const ActionNetworkFailure ActionType = "network_failure"

// NetworkFailureParams is the payload for ActionNetworkFailure.
//
// ClusterIndex selects which cluster/region of an Active-Active database
// pair to fault (0-based, default 0) — it is a region selector, not a shard
// or member index within one cluster.
//
// Delay is the outage DURATION in seconds, not a delay before onset: the
// real action's restore only happens after the target has slept out the
// full Delay, and the action does not reach a terminal status until then.
// Waiting on the action therefore blocks for the entire outage window —
// callers that want to observe behavior DURING the outage must fire the
// action and assert against its effect without waiting on it; only wait on
// it when the assertion is specifically about post-restore behavior.
type NetworkFailureParams struct {
	BDBID        int `json:"bdb_id"`
	ClusterIndex int `json:"cluster_index,omitempty"`
	Delay        int `json:"delay,omitempty"`
}

// FaultInjectorClient is a thin, protocol-generic HTTP client for the
// fault-injector wire contract (POST /action, GET /action/{id}). It carries
// no mode-specific state: pointing baseURL at the real cae-client-testing
// service or at a local MockFaultInjector is the entire mode switch.
type FaultInjectorClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewFaultInjectorClient builds a client against baseURL (no trailing
// slash expected, none required either).
func NewFaultInjectorClient(baseURL string) *FaultInjectorClient {
	return &FaultInjectorClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

type triggerActionRequest struct {
	Type       string      `json:"type"`
	Parameters interface{} `json:"parameters"`
}

type triggerActionResponse struct {
	ActionID string `json:"action_id"`
}

// TriggerAction submits an action and returns its id immediately — actions
// run asynchronously server-side (matching the real service's RQ-job
// model), so the effect is not guaranteed to be in place the instant this
// call returns. Callers that need to observe the effect synchronously must
// poll for it independently (e.g. dialing the target) rather than assume
// TriggerAction returning means the fault is already live.
func (c *FaultInjectorClient) TriggerAction(ctx context.Context, action ActionType, params interface{}) (string, error) {
	body, err := json.Marshal(triggerActionRequest{Type: string(action), Parameters: params})
	if err != nil {
		return "", fmt.Errorf("faultinjector: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/action", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("faultinjector: POST /action: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("faultinjector: POST /action: status %d: %s", resp.StatusCode, data)
	}
	var out triggerActionResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return "", fmt.Errorf("faultinjector: decode /action response: %w", err)
	}
	if out.ActionID == "" {
		return "", fmt.Errorf("faultinjector: /action response missing action_id: %s", data)
	}
	return out.ActionID, nil
}

// ActionStatus mirrors the real service's GET /action/{id} response. Status
// is one of the real service's vocabulary: "pending", "running", "success",
// "failed" ("finished"/"cancelled" are accepted defensively but never sent
// by the real service).
type ActionStatus struct {
	Status    string      `json:"status"`
	Error     string      `json:"error"`
	Traceback string      `json:"traceback"`
	Output    interface{} `json:"output"`
}

func (s *ActionStatus) terminal() bool {
	switch s.Status {
	case "success", "finished", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func (s *ActionStatus) failed() bool {
	return s.Status == "failed" || s.Status == "cancelled"
}

// GetActionStatus fetches the current status of a previously triggered
// action without blocking.
func (c *FaultInjectorClient) GetActionStatus(ctx context.Context, actionID string) (*ActionStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/action/"+actionID, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("faultinjector: GET /action/%s: %w", actionID, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("faultinjector: GET /action/%s: status %d: %s", actionID, resp.StatusCode, data)
	}
	var out ActionStatus
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("faultinjector: decode /action/%s response: %w", actionID, err)
	}
	return &out, nil
}

// WaitForAction polls until the action reaches a terminal status or ctx/
// timeout expires. See NetworkFailureParams' doc comment: for
// ActionNetworkFailure this blocks for the entire outage duration, since
// the action is not terminal until the target has been restored.
func (c *FaultInjectorClient) WaitForAction(ctx context.Context, actionID string, timeout time.Duration) (*ActionStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		st, err := c.GetActionStatus(ctx, actionID)
		if err != nil {
			return nil, err
		}
		if st.terminal() {
			if st.failed() {
				return st, fmt.Errorf("faultinjector: action %s failed: %s", actionID, st.Error)
			}
			return st, nil
		}
		select {
		case <-ctx.Done():
			return st, fmt.Errorf("faultinjector: action %s did not finish within %s (last status %q): %w", actionID, timeout, st.Status, ctx.Err())
		case <-ticker.C:
		}
	}
}
