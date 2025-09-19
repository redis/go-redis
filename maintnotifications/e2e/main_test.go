package e2e

import (
	"log"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
)

// Global log collector
var logCollector *TestLogCollector

// Global fault injector client
var faultInjector *FaultInjectorClient

func TestMain(m *testing.M) {
	var err error
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		log.Println("Skipping scenario tests, E2E_SCENARIO_TESTS is not set")
		return
	}

	faultInjector, err = CreateTestFaultInjector()
	if err != nil {
		panic("Failed to create fault injector: " + err.Error())
	}
	// use log collector to capture logs from redis clients
	logCollector = NewTestLogCollector()
	redis.SetLogger(logCollector)
	redis.SetLogLevel(logging.LogLevelDebug)

	logCollector.Clear()
	defer logCollector.Clear()
	log.Println("Running scenario tests...")
	status := m.Run()
	os.Exit(status)
}
