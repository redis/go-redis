package e2e

import (
	"os"
	"path/filepath"
	"testing"
)

// The cae-client-testing endpoints file carries credentials next to a quoted
// bdb_id; the custom decoder must keep them.
func TestLoadTopologyKeepsCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "endpoints.json")
	body := `{"aa":{"bdb_id":"1","username":"default","password":"pw","tls":false,` +
		`"endpoints":["redis://a.example:10769","redis://b.example:10769"]}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", path)
	t.Setenv("REDIS_ENDPOINT_NAME", "")

	topo, err := LoadTopology()
	if err != nil {
		t.Fatalf("LoadTopology: %v", err)
	}
	if topo.Username != "default" || topo.Password != "pw" {
		t.Fatalf("credentials = (%q, %q), want (\"default\", \"pw\")", topo.Username, topo.Password)
	}
	if topo.BDBID != 1 || len(topo.Endpoints) != 2 {
		t.Fatalf("topology = %+v", topo)
	}
}
