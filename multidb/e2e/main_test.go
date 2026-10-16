// Package e2e contains end-to-end tests for redis.MultiDBClient driven by
// a fault-injector HTTP contract (POST /action, GET /action/{id}) shared
// with redis-developer/cae-client-testing's real Redis Enterprise
// fault-injector service. Test code talks to whichever implements that
// contract:
//
//   - Real mode: REDIS_ENDPOINTS_CONFIG_PATH + FAULT_INJECTION_API_URL point
//     at a real Active-Active database and its fault-injector service
//     (cae-client-testing sets both).
//   - Mock mode (default, no env needed): a local MockFaultInjector
//     (faultinjector_mock.go) starts in-process, backed by the compose
//     "multidb" profile's three per-member RESP proxies, and drives faults
//     via docker stop/start (default) or iptables (opt-in, see
//     faultinjector_mock.go).
//
// A few scenarios have no real-FI equivalent (docker-pause hang semantics
// under the default mechanism, or timing choreography too jitter-sensitive
// for real mode) and stay mock-only — see scenarios_test.go and the design
// doc for the full partition.
//
// Run via `make test.multidb.e2e`, or manually:
//
//	docker compose --profile multidb up -d
//	E2E_MULTIDB_TESTS=true go test -race ./multidb/e2e/...
package e2e

import (
	"os"
	"testing"

	fi "github.com/redis/go-redis/v9/maintnotifications/e2e"
)

var (
	e2eTopology   Topology
	faultInjector *fi.FaultInjectorClient
)

func TestMain(m *testing.M) {
	if os.Getenv("E2E_MULTIDB_TESTS") != "true" {
		// Silent gated skip: the suite only runs when explicitly requested
		// (make test.multidb.e2e), and direct logging is against repository
		// conventions.
		os.Exit(0)
	}

	topo, err := LoadTopology()
	if err != nil {
		os.Stderr.WriteString("multidb/e2e: " + err.Error() + "\n")
		os.Exit(1)
	}
	e2eTopology = topo

	var mockServer interface{ Close() }
	if url := os.Getenv("FAULT_INJECTION_API_URL"); url != "" {
		// Real mode: cae-client-testing (or a manually pointed run) already
		// has a fault-injector service up.
		faultInjector = fi.NewFaultInjectorClient(url)
	} else {
		// Mock mode: start the in-process mock, wired to the local compose
		// Topology it also owns (MockMembers), independent of e2eTopology
		// (which could in principle be overridden without FAULT_INJECTION_API_URL,
		// though the default config-free path uses the same addresses for both).
		mech := SelectNetworkFaultMechanism(os.Getenv)
		mock := NewMockFaultInjector(MockBDBID, MockMembers, mech)
		srv := mock.Start()
		mockServer = srv
		faultInjector = fi.NewFaultInjectorClient(srv.URL)
	}

	// os.Exit does not run deferred calls, so the mock server (if any) is
	// closed explicitly before exiting rather than via defer.
	code := m.Run()
	if mockServer != nil {
		mockServer.Close()
	}
	os.Exit(code)
}
