// Package e2e contains end-to-end tests for redis.MultiDBClient driven by
// per-member RESP proxies (docker-compose profile "multidb"). Member outages
// are injected at the container level: docker stop (connection reset /
// refused) and docker pause (hung connections / timeouts).
//
// Run via `make test.multidb.e2e`, or manually:
//
//	docker compose --profile multidb up -d
//	E2E_MULTIDB_TESTS=true go test -race ./multidb/e2e/...
package e2e

import (
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv("E2E_MULTIDB_TESTS") != "true" {
		log.Println("Skipping MultiDB e2e tests, E2E_MULTIDB_TESTS is not set")
		os.Exit(0)
	}
	os.Exit(m.Run())
}
