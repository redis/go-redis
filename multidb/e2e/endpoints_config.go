package e2e

import (
	"encoding/json"
	"fmt"
	"os"
)

// EndpointEntry is one named entry in a REDIS_ENDPOINTS_CONFIG_PATH file —
// the same JSON shape cae-client-testing writes for redis-py's and
// go-redis's other e2e suites (one bdb_id, N region endpoints).
type EndpointEntry struct {
	BDBID     int      `json:"bdb_id"`
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Endpoints []string `json:"endpoints"`
}

// Topology is the resolved (bdb_id, per-region address, credentials) set
// this suite drives, independent of where it came from. Username/Password
// are empty for the local mock Topology (cae-resp-proxy fronts an
// unauthenticated standalone Redis) and populated from the endpoints file
// in real mode.
type Topology struct {
	BDBID     int
	Endpoints []string
	Username  string
	Password  string
}

// mockTopology is the suite's zero-config default: the three local compose
// proxies (cae-proxy-db0/1/2). It keeps `make test.multidb.e2e` runnable
// with no endpoints file, and doubles as the mock fault-injector's
// cluster_index -> container mapping.
var MockMembers = []MockMember{
	// 127.0.0.1, not localhost: docker publishes on IPv4, and hosts that
	// resolve localhost to ::1 first would dial the wrong stack.
	{Container: "cae-proxy-db0", Addr: "127.0.0.1:17100"},
	{Container: "cae-proxy-db1", Addr: "127.0.0.1:17101"},
	{Container: "cae-proxy-db2", Addr: "127.0.0.1:17102"},
}

// MockBDBID is the fixed fixture value the local mock Topology reports as
// its bdb_id. It is never validated against anything real — in mock mode
// the real fault-injector service is never in the request path, so nothing
// checks it; in real mode, bdbID instead comes from the loaded endpoints
// config, always a real uid.
const MockBDBID = 1

func DefaultMockTopology() Topology {
	addrs := make([]string, len(MockMembers))
	for i, m := range MockMembers {
		addrs[i] = m.Addr
	}
	return Topology{BDBID: MockBDBID, Endpoints: addrs}
}

// LoadTopology resolves the suite's endpoint Topology. REDIS_ENDPOINTS_CONFIG_PATH
// unset selects the local mock Topology; set, it loads that file and picks
// one entry: REDIS_ENDPOINT_NAME names it explicitly (matching redis-py's
// convention), otherwise a file with exactly one entry uses that entry, and
// a file with several requires REDIS_ENDPOINT_NAME to disambiguate.
func LoadTopology() (Topology, error) {
	path := os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH")
	if path == "" {
		return DefaultMockTopology(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Topology{}, fmt.Errorf("read REDIS_ENDPOINTS_CONFIG_PATH %q: %w", path, err)
	}
	var entries map[string]EndpointEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return Topology{}, fmt.Errorf("decode REDIS_ENDPOINTS_CONFIG_PATH %q: %w", path, err)
	}

	name := os.Getenv("REDIS_ENDPOINT_NAME")
	var entry EndpointEntry
	switch {
	case name != "":
		e, ok := entries[name]
		if !ok {
			return Topology{}, fmt.Errorf("REDIS_ENDPOINT_NAME %q not found in %q", name, path)
		}
		entry = e
	case len(entries) == 1:
		for _, e := range entries {
			entry = e
		}
	default:
		return Topology{}, fmt.Errorf("%q has %d entries; set REDIS_ENDPOINT_NAME to disambiguate", path, len(entries))
	}

	if len(entry.Endpoints) < 2 {
		return Topology{}, fmt.Errorf("%q: entry has %d endpoints, need at least 2 for failover", path, len(entry.Endpoints))
	}
	return Topology{
		BDBID:     entry.BDBID,
		Endpoints: entry.Endpoints,
		Username:  entry.Username,
		Password:  entry.Password,
	}, nil
}
