package e2e

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestStressPushNotifications tests push notifications under extreme stress conditions
func TestStressPushNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("[STRESS][SKIP] Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	var dump = true
	var errorsDetected = false

	var p = func(format string, args ...interface{}) {
		printLog("STRESS", false, format, args...)
	}

	var e = func(format string, args ...interface{}) {
		errorsDetected = true
		printLog("STRESS", true, format, args...)
	}

	var ef = func(format string, args ...interface{}) {
		printLog("STRESS", true, format, args...)
		t.FailNow()
	}

	logCollector.ClearLogs()
	defer func() {
		logCollector.Clear()
	}()

	// Create client factory from configuration
	factory, err := CreateTestClientFactory("standalone")
	if err != nil {
		t.Skipf("[STRESS][SKIP] Enterprise cluster not available, skipping stress test: %v", err)
	}
	endpointConfig := factory.GetConfig()

	// Create fault injector
	faultInjector, err := CreateTestFaultInjector()
	if err != nil {
		ef("Failed to create fault injector: %v", err)
	}

	// Extreme stress configuration
	minIdleConns := 50
	poolSize := 150
	maxConnections := 200
	numClients := 4

	var clients []redis.UniversalClient
	var trackers []*TrackingNotificationsHook
	var commandRunners []*CommandRunner

	// Create multiple clients for extreme stress
	for i := 0; i < numClients; i++ {
		client, err := factory.Create(fmt.Sprintf("stress-client-%d", i), &CreateClientOptions{
			Protocol:       3, // RESP3 required for push notifications
			PoolSize:       poolSize,
			MinIdleConns:   minIdleConns,
			MaxActiveConns: maxConnections,
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode:                       maintnotifications.ModeEnabled,
				HandoffTimeout:             60 * time.Second, // Longer timeout for stress
				RelaxedTimeout:             20 * time.Second, // Longer relaxed timeout
				PostHandoffRelaxedDuration: 5 * time.Second,  // Longer post-handoff duration
				MaxWorkers:                 50,               // Maximum workers for stress
				HandoffQueueSize:           1000,             // Large queue for stress
				EndpointType:               maintnotifications.EndpointTypeExternalIP,
			},
			ClientName: fmt.Sprintf("stress-test-client-%d", i),
		})
		if err != nil {
			ef("Failed to create stress client %d: %v", i, err)
		}
		clients = append(clients, client)

		// Setup tracking for each client
		tracker := NewTrackingNotificationsHook()
		logger := maintnotifications.NewLoggingHook(int(logging.LogLevelWarn)) // Minimal logging for stress
		setupNotificationHooks(client, tracker, logger)
		trackers = append(trackers, tracker)

		// Create command runner for each client
		commandRunner, _ := NewCommandRunner(client)
		commandRunners = append(commandRunners, commandRunner)
	}

	defer func() {
		if dump {
			p("Pool stats:")
			factory.PrintPoolStats(t)
		}
		for _, runner := range commandRunners {
			runner.Stop()
		}
		factory.DestroyAll()
	}()

	// Verify initial connectivity for all clients
	for i, client := range clients {
		err = client.Ping(ctx).Err()
		if err != nil {
			ef("Failed to ping Redis with stress client %d: %v", i, err)
		}
	}

	p("All %d stress clients connected successfully", numClients)

	// Start extreme traffic load on all clients
	var trafficWg sync.WaitGroup
	for i, runner := range commandRunners {
		trafficWg.Add(1)
		go func(clientID int, r *CommandRunner) {
			defer trafficWg.Done()
			p("Starting extreme traffic load on stress client %d", clientID)
			r.FireCommandsUntilStop(ctx)
		}(i, runner)
	}

	// Wait for traffic to stabilize
	time.Sleep(10 * time.Second)

	// Trigger multiple concurrent fault injection actions
	var actionWg sync.WaitGroup
	var actionResults []string
	var actionMutex sync.Mutex

	actions := []struct {
		name   string
		action string
		delay  time.Duration
	}{
		{"failover-1", "failover", 0},
		{"migrate-1", "migrate", 5 * time.Second},
		{"failover-2", "failover", 10 * time.Second},
	}

	p("Starting %d concurrent fault injection actions under extreme stress...", len(actions))

	for _, action := range actions {
		actionWg.Add(1)
		go func(actionName, actionType string, delay time.Duration) {
			defer actionWg.Done()

			if delay > 0 {
				time.Sleep(delay)
			}

			p("Triggering %s action under extreme stress...", actionName)
			var resp *ActionResponse
			var err error

			switch actionType {
			case "failover":
				resp, err = faultInjector.TriggerAction(ctx, ActionRequest{
					Type: "failover",
					Parameters: map[string]interface{}{
						"bdb_id": endpointConfig.BdbID,
					},
				})
			case "migrate":
				resp, err = faultInjector.TriggerAction(ctx, ActionRequest{
					Type: "migrate",
					Parameters: map[string]interface{}{
						"bdb_id": endpointConfig.BdbID,
					},
				})
			}

			if err != nil {
				e("Failed to trigger %s action: %v", actionName, err)
				return
			}

			// Wait for action to complete
			status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
				WithMaxWaitTime(360*time.Second), // Longer wait time for stress
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				e("[FI] %s action failed: %v", actionName, err)
				return
			}

			actionMutex.Lock()
			actionResults = append(actionResults, fmt.Sprintf("%s: %+v", actionName, status.Status))
			actionMutex.Unlock()

			p("[FI] %s action completed: %+v", actionName, status.Status)
		}(action.name, action.action, action.delay)
	}

	// Wait for all actions to complete
	actionWg.Wait()

	// Continue stress for a bit longer
	p("All fault injection actions completed, continuing stress for 2 more minutes...")
	time.Sleep(2 * time.Minute)

	// Stop all command runners
	for _, runner := range commandRunners {
		runner.Stop()
	}
	trafficWg.Wait()

	// Analyze stress test results
	allLogsAnalysis := logCollector.GetAnalysis()
	totalOperations := int64(0)
	totalErrors := int64(0)
	totalTimeoutErrors := int64(0)

	for i, runner := range commandRunners {
		stats := runner.GetStats()
		p("Stress client %d stats: Operations: %d, Errors: %d, Timeout Errors: %d",
			i, stats.Operations, stats.Errors, stats.TimeoutErrors)
		totalOperations += stats.Operations
		totalErrors += stats.Errors
		totalTimeoutErrors += stats.TimeoutErrors
	}

	p("STRESS TEST RESULTS:")
	p("Total operations across all clients: %d", totalOperations)
	p("Total errors: %d (%.2f%%)", totalErrors, float64(totalErrors)/float64(totalOperations)*100)
	p("Total timeout errors: %d (%.2f%%)", totalTimeoutErrors, float64(totalTimeoutErrors)/float64(totalOperations)*100)
	p("Total connections used: %d", allLogsAnalysis.ConnectionCount)

	// Print action results
	actionMutex.Lock()
	p("Fault injection action results:")
	for _, result := range actionResults {
		p("  %s", result)
	}
	actionMutex.Unlock()

	// Validate stress test results
	if totalOperations < 1000 {
		e("Expected at least 1000 operations under stress, got %d", totalOperations)
	}

	// Allow higher error rates under extreme stress (up to 20%)
	errorRate := float64(totalErrors) / float64(totalOperations) * 100
	if errorRate > 20.0 {
		e("Error rate too high under stress: %.2f%% (max allowed: 20%%)", errorRate)
	}

	// Validate connection limits weren't exceeded
	expectedMaxConnections := int64(numClients * maxConnections)
	if allLogsAnalysis.ConnectionCount > expectedMaxConnections {
		e("Connection count exceeded limit: %d > %d", allLogsAnalysis.ConnectionCount, expectedMaxConnections)
	}

	// Validate notifications were processed
	totalTrackerNotifications := int64(0)
	totalProcessingErrors := int64(0)
	for _, tracker := range trackers {
		analysis := tracker.GetAnalysis()
		totalTrackerNotifications += analysis.TotalNotifications
		totalProcessingErrors += analysis.NotificationProcessingErrors
	}

	if totalProcessingErrors > totalTrackerNotifications/10 { // Allow up to 10% processing errors under stress
		e("Too many notification processing errors under stress: %d/%d", totalProcessingErrors, totalTrackerNotifications)
	}

	if errorsDetected {
		ef("Errors detected under stress")
		logCollector.DumpLogs()
		for i, tracker := range trackers {
			p("=== Stress Client %d Analysis ===", i)
			tracker.GetAnalysis().Print(t)
		}
		logCollector.Clear()
		for _, tracker := range trackers {
			tracker.Clear()
		}
	}

	dump = false
	p("[SUCCESS] Stress test completed successfully!")
	p("Processed %d operations across %d clients with %d connections",
		totalOperations, numClients, allLogsAnalysis.ConnectionCount)
	p("Error rate: %.2f%%, Notification processing errors: %d/%d",
		errorRate, totalProcessingErrors, totalTrackerNotifications)

	// Print final analysis
	allLogsAnalysis.Print(t)
	for i, tracker := range trackers {
		p("=== Stress Client %d Analysis ===", i)
		tracker.GetAnalysis().Print(t)
	}
}
