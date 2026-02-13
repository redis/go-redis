package e2e

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
)

// Global log collector
var logCollector *TestLogCollector

const defaultTestTimeout = 30 * time.Minute

// Global fault injector client
var faultInjector *FaultInjectorClient
var faultInjectorCleanup func()

func TestMain(m *testing.M) {
	var err error
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		log.Println("Skipping scenario tests, E2E_SCENARIO_TESTS is not set")
		return
	}

	// Only create fault injector if we're not using the unified injector
	// The unified injector tests use NewNotificationInjector() which auto-detects the mode
	// and doesn't require the global faultInjector variable
	// We can detect unified injector mode by checking if REDIS_ENDPOINTS_CONFIG_PATH is NOT set
	// (which means we're using the proxy mock mode)
	if os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH") != "" {
		// Real fault injector mode - create the global fault injector
		faultInjector, faultInjectorCleanup, err = CreateTestFaultInjectorWithCleanup()
		if err != nil {
			panic("Failed to create fault injector: " + err.Error())
		}
		defer faultInjectorCleanup()
	} else {
		log.Println("Using proxy mock mode - skipping global fault injector setup")
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
