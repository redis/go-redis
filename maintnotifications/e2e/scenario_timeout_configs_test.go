package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	logs2 "github.com/redis/go-redis/v9/internal/maintnotifications/logs"
	"github.com/redis/go-redis/v9/logging"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestTimeoutConfigurationsPushNotifications tests push notifications with different timeout configurations
func TestTimeoutConfigurationsPushNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	var dump = true
	var p = func(format string, args ...interface{}) {
		format = "[%s][TIMEOUT-CONFIGS] " + format
		ts := time.Now().Format("15:04:05.000")
		args = append([]interface{}{ts}, args...)
		t.Logf(format, args...)
	}

	// Test different timeout configurations
	timeoutConfigs := []struct {
		name                       string
		handoffTimeout             time.Duration
		relaxedTimeout             time.Duration
		postHandoffRelaxedDuration time.Duration
		description                string
		expectedBehavior           string
	}{
		{
			name:                       "Conservative",
			handoffTimeout:             60 * time.Second,
			relaxedTimeout:             30 * time.Second,
			postHandoffRelaxedDuration: 2 * time.Minute,
			description:                "Conservative timeouts for stable environments",
			expectedBehavior:           "Longer timeouts, fewer timeout errors",
		},
		{
			name:                       "Aggressive",
			handoffTimeout:             5 * time.Second,
			relaxedTimeout:             3 * time.Second,
			postHandoffRelaxedDuration: 1 * time.Second,
			description:                "Aggressive timeouts for fast failover",
			expectedBehavior:           "Shorter timeouts, faster recovery",
		},
		{
			name:                       "HighLatency",
			handoffTimeout:             90 * time.Second,
			relaxedTimeout:             30 * time.Second,
			postHandoffRelaxedDuration: 10 * time.Minute,
			description:                "High latency environment timeouts",
			expectedBehavior:           "Very long timeouts for high latency networks",
		},
	}

	logCollector.ClearLogs()
	defer func() {
		if dump {
			p("Dumping logs...")
			logCollector.DumpLogs()
			p("Log Analysis:")
			logCollector.GetAnalysis().Print(t)
		}
		logCollector.Clear()
	}()

	// Create client factory from configuration
	factory, err := CreateTestClientFactory("standalone")
	if err != nil {
		t.Skipf("[TIMEOUT-CONFIGS][SKIP] Enterprise cluster not available, skipping timeout configs test: %v", err)
	}
	endpointConfig := factory.GetConfig()

	// Create fault injector
	faultInjector, err := CreateTestFaultInjector()
	if err != nil {
		t.Fatalf("[ERROR] Failed to create fault injector: %v", err)
	}

	defer func() {
		if dump {
			p("Pool stats:")
			factory.PrintPoolStats(t)
		}
		factory.DestroyAll()
	}()

	// Test each timeout configuration
	for _, timeoutTest := range timeoutConfigs {
		t.Run(timeoutTest.name, func(t *testing.T) {
			// redefine p and e for each test to get
			// proper test name in logs and proper test failures
			var p = func(format string, args ...interface{}) {
				format = "[%s][TIMEOUT-CONFIGS] " + format
				ts := time.Now().Format("15:04:05.000")
				args = append([]interface{}{ts}, args...)
				t.Logf(format, args...)
			}

			var e = func(format string, args ...interface{}) {
				format = "[%s][TIMEOUT-CONFIGS][ERROR] " + format
				ts := time.Now().Format("15:04:05.000")
				args = append([]interface{}{ts}, args...)
				t.Errorf(format, args...)
			}

			var ef = func(format string, args ...interface{}) {
				format = "[%s][TIMEOUT-CONFIGS][ERROR] " + format
				ts := time.Now().Format("15:04:05.000")
				args = append([]interface{}{ts}, args...)
				t.Fatalf(format, args...)
			}

			p("Testing timeout configuration: %s - %s", timeoutTest.name, timeoutTest.description)
			p("Expected behavior: %s", timeoutTest.expectedBehavior)
			p("Handoff timeout: %v, Relaxed timeout: %v, Post-handoff duration: %v",
				timeoutTest.handoffTimeout, timeoutTest.relaxedTimeout, timeoutTest.postHandoffRelaxedDuration)

			minIdleConns := 4
			poolSize := 10
			maxConnections := 15

			// Create Redis client with specific timeout configuration
			client, err := factory.Create(fmt.Sprintf("timeout-test-%s", timeoutTest.name), &CreateClientOptions{
				Protocol:       3, // RESP3 required for push notifications
				PoolSize:       poolSize,
				MinIdleConns:   minIdleConns,
				MaxActiveConns: maxConnections,
				MaintNotificationsConfig: &maintnotifications.Config{
					Mode:                       maintnotifications.ModeEnabled,
					HandoffTimeout:             timeoutTest.handoffTimeout,
					RelaxedTimeout:             timeoutTest.relaxedTimeout,
					PostHandoffRelaxedDuration: timeoutTest.postHandoffRelaxedDuration,
					MaxWorkers:                 20,
					EndpointType:               maintnotifications.EndpointTypeExternalIP,
				},
				ClientName: fmt.Sprintf("timeout-test-%s", timeoutTest.name),
			})
			if err != nil {
				ef("Failed to create client for %s: %v", timeoutTest.name, err)
			}

			// Create timeout tracker
			tracker := NewTrackingNotificationsHook()
			logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
			setupNotificationHooks(client, tracker, logger)
			defer func() {
				if dump {
					p("Tracker analysis for %s:", timeoutTest.name)
					tracker.GetAnalysis().Print(t)
				}
				tracker.Clear()
			}()

			// Verify initial connectivity
			err = client.Ping(ctx).Err()
			if err != nil {
				ef("Failed to ping Redis with %s timeout config: %v", timeoutTest.name, err)
			}

			p("Client connected successfully with %s timeout configuration", timeoutTest.name)

			commandsRunner, _ := NewCommandRunner(client)
			defer func() {
				if dump {
					stats := commandsRunner.GetStats()
					p("%s timeout config stats: Operations: %d, Errors: %d, Timeout Errors: %d",
						timeoutTest.name, stats.Operations, stats.Errors, stats.TimeoutErrors)
				}
				commandsRunner.Stop()
			}()

			// Start command traffic
			go func() {
				commandsRunner.FireCommandsUntilStop(ctx)
			}()

			// Record start time for timeout analysis
			testStartTime := time.Now()

			// Test failover with this timeout configuration
			p("Testing failover with %s timeout configuration...", timeoutTest.name)
			failoverResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "failover",
				Parameters: map[string]interface{}{
					"bdb_id": endpointConfig.BdbID,
				},
			})
			if err != nil {
				ef("Failed to trigger failover action for %s: %v", timeoutTest.name, err)
			}

			// Wait for FAILING_OVER notification
			match, found := logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && notificationType(s, "FAILING_OVER")
			}, 3*time.Minute)
			if !found {
				ef("FAILING_OVER notification was not received for %s timeout config", timeoutTest.name)
			}
			failingOverData := logs2.ExtractDataFromLogMessage(match)
			p("FAILING_OVER notification received for %s. %v", timeoutTest.name, failingOverData)

			// Wait for FAILED_OVER notification
			seqIDToObserve := int64(failingOverData["seqID"].(float64))
			connIDToObserve := uint64(failingOverData["connID"].(float64))
			match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return notificationType(s, "FAILED_OVER") && connID(s, connIDToObserve) && seqID(s, seqIDToObserve+1)
			}, 3*time.Minute)
			if !found {
				ef("FAILED_OVER notification was not received for %s timeout config", timeoutTest.name)
			}
			failedOverData := logs2.ExtractDataFromLogMessage(match)
			p("FAILED_OVER notification received for %s. %v", timeoutTest.name, failedOverData)

			// Wait for failover to complete
			status, err := faultInjector.WaitForAction(ctx, failoverResp.ActionID,
				WithMaxWaitTime(180*time.Second),
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				ef("[FI] Failover action failed for %s: %v", timeoutTest.name, err)
			}
			p("[FI] Failover action completed for %s: %s", timeoutTest.name, status.Status)

			// Continue traffic to observe timeout behavior
			p("Continuing traffic for %v to observe timeout behavior...", timeoutTest.relaxedTimeout*2)
			time.Sleep(timeoutTest.relaxedTimeout * 2)

			// Test migration to trigger more timeout scenarios
			p("Testing migration with %s timeout configuration...", timeoutTest.name)
			migrateResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "migrate",
				Parameters: map[string]interface{}{
					"bdb_id": endpointConfig.BdbID,
				},
			})
			if err != nil {
				ef("Failed to trigger migrate action for %s: %v", timeoutTest.name, err)
			}

			// Wait for migration to complete
			status, err = faultInjector.WaitForAction(ctx, migrateResp.ActionID,
				WithMaxWaitTime(240*time.Second),
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				ef("[FI] Migrate action failed for %s: %v", timeoutTest.name, err)
			}

			p("[FI] Migrate action completed for %s: %s", timeoutTest.name, status.Status)

			// Wait for MIGRATING notification
			match, found = logCollector.WaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && strings.Contains(s, "MIGRATING")
			}, 60*time.Second)
			if !found {
				ef("MIGRATING notification was not received for %s timeout config", timeoutTest.name)
			}
			migrateData := logs2.ExtractDataFromLogMessage(match)
			p("MIGRATING notification received for %s: %v", timeoutTest.name, migrateData)


			// do a bind action
			bindResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "bind",
				Parameters: map[string]interface{}{
					"bdb_id": endpointConfig.BdbID,
				},
			})
			if err != nil {
				ef("Failed to trigger bind action for %s: %v", timeoutTest.name, err)
			}
			status, err = faultInjector.WaitForAction(ctx, bindResp.ActionID,
				WithMaxWaitTime(240*time.Second),
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				ef("[FI] Bind action failed for %s: %v", timeoutTest.name, err)
			}
			p("[FI] Bind action completed for %s: %s", timeoutTest.name, status.Status)
			// waiting for moving notification
			match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && notificationType(s, "MOVING")
			}, 3*time.Minute)
			if !found {
				ef("MOVING notification was not received for %s timeout config", timeoutTest.name)
			}

			movingData := logs2.ExtractDataFromLogMessage(match)
			p("MOVING notification received for %s. %v", timeoutTest.name, movingData)

			// Continue traffic for post-handoff timeout observation
			p("Continuing traffic for %v to observe post-handoff timeout behavior...", 1*time.Minute)
			time.Sleep(1 * time.Minute)

			commandsRunner.Stop()
			testDuration := time.Since(testStartTime)

			// Analyze timeout behavior
			trackerAnalysis := tracker.GetAnalysis()
			logAnalysis := logCollector.GetAnalysis()
			if trackerAnalysis.NotificationProcessingErrors > 0 {
				e("Notification processing errors with %s timeout config: %d", timeoutTest.name, trackerAnalysis.NotificationProcessingErrors)
			}

			// Validate timeout-specific behavior
			switch timeoutTest.name {
			case "Conservative":
				if trackerAnalysis.UnrelaxedTimeoutCount > trackerAnalysis.RelaxedTimeoutCount {
					e("Conservative config should have more relaxed than unrelaxed timeouts, got relaxed=%d, unrelaxed=%d",
						trackerAnalysis.RelaxedTimeoutCount, trackerAnalysis.UnrelaxedTimeoutCount)
				}
			case "Aggressive":
				// Aggressive timeouts should complete faster
				if testDuration > 5*time.Minute {
					e("Aggressive config took too long: %v", testDuration)
				}
				if logAnalysis.TotalHandoffRetries > logAnalysis.TotalHandoffCount {
					e("Expect handoff retries since aggressive timeouts are shorter, got %d retries for %d handoffs",
						logAnalysis.TotalHandoffRetries, logAnalysis.TotalHandoffCount)
				}
			case "HighLatency":
				// High latency config should have very few unrelaxed after moving
				if logAnalysis.UnrelaxedAfterMoving > 2 {
					e("High latency config should have minimal unrelaxed timeouts after moving, got %d", logAnalysis.UnrelaxedAfterMoving)
				}
			}

			// Validate we received expected notifications
			if trackerAnalysis.FailingOverCount == 0 {
				e("Expected FAILING_OVER notifications with %s timeout config, got none", timeoutTest.name)
			}
			if trackerAnalysis.FailedOverCount == 0 {
				e("Expected FAILED_OVER notifications with %s timeout config, got none", timeoutTest.name)
			}
			if trackerAnalysis.MigratingCount == 0 {
				e("Expected MIGRATING notifications with %s timeout config, got none", timeoutTest.name)
			}

			// Validate timeout counts are reasonable
			if trackerAnalysis.RelaxedTimeoutCount == 0 {
				e("Expected relaxed timeouts with %s config, got none", timeoutTest.name)
			}

			if logAnalysis.SucceededHandoffCount == 0 {
				e("Expected successful handoffs with %s config, got none", timeoutTest.name)
			}

			p("Timeout configuration %s test completed successfully in %v", timeoutTest.name, testDuration)
			p("Command runner stats:")
			p("Operations: %d, Errors: %d, Timeout Errors: %d",
				commandsRunner.GetStats().Operations, commandsRunner.GetStats().Errors, commandsRunner.GetStats().TimeoutErrors)
			p("Relaxed timeouts: %d, Unrelaxed timeouts: %d", trackerAnalysis.RelaxedTimeoutCount, trackerAnalysis.UnrelaxedTimeoutCount)
		})

		// Clear logs between timeout configuration tests
		logCollector.ClearLogs()
	}

	p("All timeout configurations tested successfully")
}
