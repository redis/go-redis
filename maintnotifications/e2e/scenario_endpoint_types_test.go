package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal"
	logs2 "github.com/redis/go-redis/v9/internal/maintnotifications/logs"
	"github.com/redis/go-redis/v9/logging"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestEndpointTypesPushNotifications tests push notifications with different endpoint types
func TestEndpointTypesPushNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	var dump = true
	var errorsDetected = false

	// Test different endpoint types
	endpointTypes := []struct {
		name         string
		endpointType maintnotifications.EndpointType
		description  string
	}{
		{
			name:         "ExternalIP",
			endpointType: maintnotifications.EndpointTypeExternalIP,
			description:  "External IP endpoint type for enterprise clusters",
		},
		{
			name:         "ExternalFQDN",
			endpointType: maintnotifications.EndpointTypeExternalFQDN,
			description:  "External FQDN endpoint type for DNS-based routing",
		},
		{
			name:         "None",
			endpointType: maintnotifications.EndpointTypeNone,
			description:  "No endpoint type - reconnect with current config",
		},
	}

	defer func() {
		logCollector.Clear()
	}()

	// Test each endpoint type with its own fresh database
	for _, endpointTest := range endpointTypes {
		t.Run(endpointTest.name, func(t *testing.T) {
			// Setup: Create fresh database and client factory for THIS endpoint type test
			bdbID, factory, cleanup := SetupTestDatabaseAndFactory(t, ctx, "standalone")
			defer cleanup()
			t.Logf("[ENDPOINT-TYPES-%s] Created test database with bdb_id: %d", endpointTest.name, bdbID)

			// Create fault injector with cleanup
			faultInjector, fiCleanup, err := CreateTestFaultInjectorWithCleanup()
			if err != nil {
				t.Fatalf("[ERROR] Failed to create fault injector: %v", err)
			}
			defer fiCleanup()

			// Get endpoint config from factory (now connected to new database)
			endpointConfig := factory.GetConfig()

			defer func() {
				if dump {
					fmt.Println("Pool stats:")
					factory.PrintPoolStats(t)
				}
			}()
			// Clear logs between endpoint type tests
			logCollector.Clear()
			// reset errors detected flag
			errorsDetected = false
			// reset dump flag
			dump = true
			// redefine p and e for each test to get
			// proper test name in logs and proper test failures
			var p = func(format string, args ...interface{}) {
				printLog("ENDPOINT-TYPES", false, format, args...)
			}

			var e = func(format string, args ...interface{}) {
				errorsDetected = true
				printLog("ENDPOINT-TYPES", true, format, args...)
			}

			var ef = func(format string, args ...interface{}) {
				printLog("ENDPOINT-TYPES", true, format, args...)
				t.FailNow()
			}

			p("Testing endpoint type: %s - %s", endpointTest.name, endpointTest.description)

			minIdleConns := 3
			poolSize := 8
			maxConnections := 12

			// Create Redis client with specific endpoint type
			client, err := factory.Create(fmt.Sprintf("endpoint-test-%s", endpointTest.name), &CreateClientOptions{
				Protocol:       3, // RESP3 required for push notifications
				PoolSize:       poolSize,
				MinIdleConns:   minIdleConns,
				MaxActiveConns: maxConnections,
				MaintNotificationsConfig: &maintnotifications.Config{
					Mode:                       maintnotifications.ModeEnabled,
					HandoffTimeout:             30 * time.Second,
					RelaxedTimeout:             8 * time.Second,
					PostHandoffRelaxedDuration: 2 * time.Second,
					MaxWorkers:                 15,
					EndpointType:               endpointTest.endpointType, // Test specific endpoint type
				},
				ClientName: fmt.Sprintf("endpoint-test-%s", endpointTest.name),
			})
			if err != nil {
				ef("Failed to create client for %s: %v", endpointTest.name, err)
			}

			// Create timeout tracker
			tracker := NewTrackingNotificationsHook()
			logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
			setupNotificationHooks(client, tracker, logger)
			defer func() {
				tracker.Clear()
			}()

			// Verify initial connectivity
			err = client.Ping(ctx).Err()
			if err != nil {
				ef("Failed to ping Redis with %s endpoint type: %v", endpointTest.name, err)
			}

			p("Client connected successfully with %s endpoint type", endpointTest.name)

			commandsRunner, _ := NewCommandRunner(client)
			defer func() {
				if dump {
					stats := commandsRunner.GetStats()
					p("%s endpoint stats: Operations: %d, Errors: %d, Timeout Errors: %d",
						endpointTest.name, stats.Operations, stats.Errors, stats.TimeoutErrors)
				}
				commandsRunner.Stop()
			}()

			// Test failover with this endpoint type
			p("Testing failover with %s endpoint type on database [bdb_id:%s]...", endpointTest.name, endpointConfig.BdbID)
			failoverResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "failover",
				Parameters: map[string]interface{}{
					"bdb_id": endpointConfig.BdbID,
				},
			})
			if err != nil {
				ef("Failed to trigger failover action for %s: %v", endpointTest.name, err)
			}

			// Start command traffic
			go func() {
				commandsRunner.FireCommandsUntilStop(ctx)
			}()

			// Wait for failover to complete
			status, err := faultInjector.WaitForAction(ctx, failoverResp.ActionID,
				WithMaxWaitTime(240*time.Second),
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				ef("[FI] Failover action failed for %s: %v", endpointTest.name, err)
			}
			p("[FI] Failover action completed for %s: %s %s", endpointTest.name, status.Status, actionOutputIfFailed(status))

			// Wait for FAILING_OVER notification
			match, found := logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && notificationType(s, "FAILING_OVER")
			}, 3*time.Minute)
			if !found {
				ef("FAILING_OVER notification was not received for %s endpoint type", endpointTest.name)
			}
			failingOverData := logs2.ExtractDataFromLogMessage(match)
			p("FAILING_OVER notification received for %s. %v", endpointTest.name, failingOverData)

			// Wait for FAILED_OVER notification
			seqIDToObserve := int64(failingOverData["seqID"].(float64))
			connIDToObserve := uint64(failingOverData["connID"].(float64))
			match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return notificationType(s, "FAILED_OVER") && connID(s, connIDToObserve) && seqID(s, seqIDToObserve+1)
			}, 3*time.Minute)
			if !found {
				ef("FAILED_OVER notification was not received for %s endpoint type", endpointTest.name)
			}
			failedOverData := logs2.ExtractDataFromLogMessage(match)
			p("FAILED_OVER notification received for %s. %v", endpointTest.name, failedOverData)

			// Test migration with this endpoint type
			p("Testing migration with %s endpoint type...", endpointTest.name)
			migrateResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "migrate",
				Parameters: map[string]interface{}{
					"bdb_id": endpointConfig.BdbID,
				},
			})
			if err != nil {
				ef("Failed to trigger migrate action for %s: %v", endpointTest.name, err)
			}

			// Wait for migration to complete
			status, err = faultInjector.WaitForAction(ctx, migrateResp.ActionID,
				WithMaxWaitTime(240*time.Second),
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				ef("[FI] Migrate action failed for %s: %v", endpointTest.name, err)
			}
			p("[FI] Migrate action completed for %s: %s %s", endpointTest.name, status.Status, actionOutputIfFailed(status))

			// Wait for MIGRATING notification
			match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && strings.Contains(s, "MIGRATING")
			}, 60*time.Second)
			if !found {
				ef("MIGRATING notification was not received for %s endpoint type", endpointTest.name)
			}
			migrateData := logs2.ExtractDataFromLogMessage(match)
			p("MIGRATING notification received for %s: %v", endpointTest.name, migrateData)

			// Wait for MIGRATED notification
			seqIDToObserve = int64(migrateData["seqID"].(float64))
			connIDToObserve = uint64(migrateData["connID"].(float64))
			match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return notificationType(s, "MIGRATED") && connID(s, connIDToObserve) && seqID(s, seqIDToObserve+1)
			}, 3*time.Minute)
			if !found {
				ef("MIGRATED notification was not received for %s endpoint type", endpointTest.name)
			}
			migratedData := logs2.ExtractDataFromLogMessage(match)
			p("MIGRATED notification received for %s. %v", endpointTest.name, migratedData)

			// Complete migration with bind action
			// Pass endpoint_type to the bind action so it knows what format to use
			var endpointTypeStr string
			switch endpointTest.endpointType {
			case maintnotifications.EndpointTypeExternalIP:
				endpointTypeStr = "external-ip"
			case maintnotifications.EndpointTypeExternalFQDN:
				endpointTypeStr = "external-fqdn"
			case maintnotifications.EndpointTypeNone:
				endpointTypeStr = "none"
			}

			bindResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "bind",
				Parameters: map[string]interface{}{
					"bdb_id":        endpointConfig.BdbID,
					"endpoint_type": endpointTypeStr,
				},
			})
			if err != nil {
				ef("Failed to trigger bind action for %s: %v", endpointTest.name, err)
			}

			// Wait for MOVING notification
			match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && notificationType(s, "MOVING")
			}, 3*time.Minute)
			if !found {
				ef("MOVING notification was not received for %s endpoint type", endpointTest.name)
			}
			movingData := logs2.ExtractDataFromLogMessage(match)
			p("MOVING notification received for %s. %v", endpointTest.name, movingData)

			notification, ok := movingData["notification"].(string)
			if !ok {
				e("invalid notification message")
			}

			notification = notification[:len(notification)-1]
			notificationParts := strings.Split(notification, " ")
			address := notificationParts[len(notificationParts)-1]

			switch endpointTest.endpointType {
			case maintnotifications.EndpointTypeExternalFQDN:
				address = strings.Split(address, ":")[0]
				addressParts := strings.SplitN(address, ".", 2)
				if len(addressParts) != 2 {
					e("invalid address %s", address)
				} else {
					address = addressParts[1]
				}

				var expectedAddress string
				hostParts := strings.SplitN(endpointConfig.Host, ".", 2)
				if len(hostParts) != 2 {
					// Docker proxy setup uses "localhost" without domain suffix
					// In this case, skip FQDN validation
					p("Skipping FQDN validation for Docker proxy setup (host=%s)", endpointConfig.Host)
				} else {
					expectedAddress = hostParts[1]
					if address != expectedAddress {
						e("invalid fqdn, expected: %s, got: %s", expectedAddress, address)
					}
				}

			case maintnotifications.EndpointTypeExternalIP:
				address = strings.Split(address, ":")[0]
				ip := net.ParseIP(address)
				if ip == nil {
					e("invalid message format, expected valid IP, got: %s", address)
				}
			case maintnotifications.EndpointTypeNone:
				if address != internal.RedisNull {
					e("invalid endpoint type, expected: %s, got: %s", internal.RedisNull, address)
				}
			}

			// Wait for bind to complete
			bindStatus, err := faultInjector.WaitForAction(ctx, bindResp.ActionID,
				WithMaxWaitTime(240*time.Second),
				WithPollInterval(2*time.Second))
			if err != nil {
				ef("Bind action failed for %s: %v", endpointTest.name, err)
			}
			p("Bind action completed for %s: %s %s", endpointTest.name, bindStatus.Status, actionOutputIfFailed(bindStatus))

			// Continue traffic for analysis
			time.Sleep(60 * time.Second)
			commandsRunner.Stop()

			// Analyze results for this endpoint type
			trackerAnalysis := tracker.GetAnalysis()
			if trackerAnalysis.NotificationProcessingErrors > 0 {
				e("Notification processing errors with %s endpoint type: %d", endpointTest.name, trackerAnalysis.NotificationProcessingErrors)
			}

			if trackerAnalysis.UnexpectedNotificationCount > 0 {
				e("Unexpected notifications with %s endpoint type: %d", endpointTest.name, trackerAnalysis.UnexpectedNotificationCount)
			}

			// Validate we received all expected notification types
			if trackerAnalysis.FailingOverCount == 0 {
				e("Expected FAILING_OVER notifications with %s endpoint type, got none", endpointTest.name)
			}
			if trackerAnalysis.FailedOverCount == 0 {
				e("Expected FAILED_OVER notifications with %s endpoint type, got none", endpointTest.name)
			}
			if trackerAnalysis.MigratingCount == 0 {
				e("Expected MIGRATING notifications with %s endpoint type, got none", endpointTest.name)
			}
			if trackerAnalysis.MigratedCount == 0 {
				e("Expected MIGRATED notifications with %s endpoint type, got none", endpointTest.name)
			}
			if trackerAnalysis.MovingCount == 0 {
				e("Expected MOVING notifications with %s endpoint type, got none", endpointTest.name)
			}

			logAnalysis := logCollector.GetAnalysis()
			if logAnalysis.TotalHandoffCount == 0 {
				e("Expected at least one handoff with %s endpoint type, got none", endpointTest.name)
			}
			if logAnalysis.TotalHandoffCount != logAnalysis.SucceededHandoffCount {
				e("Expected all handoffs to succeed with %s endpoint type, got %d failed", endpointTest.name, logAnalysis.FailedHandoffCount)
			}

			if errorsDetected {
				logCollector.DumpLogs()
				trackerAnalysis.Print(t)
				logCollector.Clear()
				tracker.Clear()
				ef("[FAIL] Errors detected with %s endpoint type", endpointTest.name)
			}
			p("Endpoint type %s test completed successfully", endpointTest.name)
			logCollector.GetAnalysis().Print(t)
			trackerAnalysis.Print(t)
			logCollector.Clear()
			tracker.Clear()
		})
	}

	t.Log("All endpoint types tested successfully")
}
