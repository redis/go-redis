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

// TODO ADD TLS CONFIGS
// TestTLSConfigurationsPushNotifications tests push notifications with different TLS configurations
func ТestTLSConfigurationsPushNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	var dump = true
	var p = func(format string, args ...interface{}) {
		format = "[%s][TLS-CONFIGS] " + format
		ts := time.Now().Format("15:04:05.000")
		args = append([]interface{}{ts}, args...)
		t.Logf(format, args...)
	}

	// Test different TLS configurations
	// Note: TLS configuration is typically handled at the Redis connection config level
	// This scenario demonstrates the testing pattern for different TLS setups
	tlsConfigs := []struct {
		name        string
		description string
		skipReason  string
	}{
		{
			name:        "NoTLS",
			description: "No TLS encryption (plain text)",
		},
		{
			name:        "TLSInsecure",
			description: "TLS with insecure skip verify (testing only)",
		},
		{
			name:        "TLSSecure",
			description: "Secure TLS with certificate verification",
			skipReason:  "Requires valid certificates in test environment",
		},
		{
			name:        "TLSMinimal",
			description: "TLS with minimal version requirements",
		},
		{
			name:        "TLSStrict",
			description: "Strict TLS with TLS 1.3 and specific cipher suites",
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
		t.Skipf("[TLS-CONFIGS][SKIP] Enterprise cluster not available, skipping TLS configs test: %v", err)
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

	// Test each TLS configuration
	for _, tlsTest := range tlsConfigs {
		t.Run(tlsTest.name, func(t *testing.T) {
			// redefine p and e for each test to get
			// proper test name in logs and proper test failures
			var p = func(format string, args ...interface{}) {
				format = "[%s][TLS-CONFIGS] " + format
				ts := time.Now().Format("15:04:05.000")
				args = append([]interface{}{ts}, args...)
				t.Logf(format, args...)
			}

			var e = func(format string, args ...interface{}) {
				format = "[%s][TLS-CONFIGS][ERROR] " + format
				ts := time.Now().Format("15:04:05.000")
				args = append([]interface{}{ts}, args...)
				t.Errorf(format, args...)
			}

			var ef = func(format string, args ...interface{}) {
				format = "[%s][TLS-CONFIGS][ERROR] " + format
				ts := time.Now().Format("15:04:05.000")
				args = append([]interface{}{ts}, args...)
				t.Fatalf(format, args...)
			}
			if tlsTest.skipReason != "" {
				t.Skipf("Skipping %s: %s", tlsTest.name, tlsTest.skipReason)
			}

			p("Testing TLS configuration: %s - %s", tlsTest.name, tlsTest.description)

			minIdleConns := 3
			poolSize := 8
			maxConnections := 12

			// Create Redis client with specific TLS configuration
			// Note: TLS configuration is handled at the factory/connection level
			client, err := factory.Create(fmt.Sprintf("tls-test-%s", tlsTest.name), &CreateClientOptions{
				Protocol:       3, // RESP3 required for push notifications
				PoolSize:       poolSize,
				MinIdleConns:   minIdleConns,
				MaxActiveConns: maxConnections,
				MaintNotificationsConfig: &maintnotifications.Config{
					Mode:                       maintnotifications.ModeEnabled,
					HandoffTimeout:             30 * time.Second,
					RelaxedTimeout:             10 * time.Second,
					PostHandoffRelaxedDuration: 2 * time.Second,
					MaxWorkers:                 15,
					EndpointType:               maintnotifications.EndpointTypeExternalIP,
				},
				ClientName: fmt.Sprintf("tls-test-%s", tlsTest.name),
			})
			if err != nil {
				// Some TLS configurations might fail in test environments
				if tlsTest.name == "TLSSecure" || tlsTest.name == "TLSStrict" {
					t.Skipf("TLS configuration %s failed (expected in test environment): %v", tlsTest.name, err)
				}
				ef("Failed to create client for %s: %v", tlsTest.name, err)
			}

			// Create timeout tracker
			tracker := NewTrackingNotificationsHook()
			logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
			setupNotificationHooks(client, tracker, logger)
			defer func() {
				if dump {
					p("Tracker analysis for %s:", tlsTest.name)
					tracker.GetAnalysis().Print(t)
				}
				tracker.Clear()
			}()

			// Verify initial connectivity
			err = client.Ping(ctx).Err()
			if err != nil {
				if tlsTest.name == "TLSSecure" || tlsTest.name == "TLSStrict" {
					t.Skipf("TLS configuration %s ping failed (expected in test environment): %v", tlsTest.name, err)
				}
				ef("Failed to ping Redis with %s TLS config: %v", tlsTest.name, err)
			}

			p("Client connected successfully with %s TLS configuration", tlsTest.name)

			commandsRunner, _ := NewCommandRunner(client)
			defer func() {
				if dump {
					stats := commandsRunner.GetStats()
					p("%s TLS config stats: Operations: %d, Errors: %d, Timeout Errors: %d",
						tlsTest.name, stats.Operations, stats.Errors, stats.TimeoutErrors)
				}
				commandsRunner.Stop()
			}()

			// Start command traffic
			go func() {
				commandsRunner.FireCommandsUntilStop(ctx)
			}()

			// Test failover with this TLS configuration
			p("Testing failover with %s TLS configuration...", tlsTest.name)
			failoverResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "failover",
				Parameters: map[string]interface{}{
					"cluster_index": "0",
					"bdb_id":        endpointConfig.BdbID,
				},
			})
			if err != nil {
				ef("Failed to trigger failover action for %s: %v", tlsTest.name, err)
			}

			// Wait for FAILING_OVER notification
			match, found := logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && notificationType(s, "FAILING_OVER")
			}, 2*time.Minute)
			if !found {
				ef("FAILING_OVER notification was not received for %s TLS config", tlsTest.name)
			}
			failingOverData := logs2.ExtractDataFromLogMessage(match)
			p("FAILING_OVER notification received for %s. %v", tlsTest.name, failingOverData)

			// Wait for FAILED_OVER notification
			seqIDToObserve := int64(failingOverData["seqID"].(float64))
			connIDToObserve := uint64(failingOverData["connID"].(float64))
			match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
				return notificationType(s, "FAILED_OVER") && connID(s, connIDToObserve) && seqID(s, seqIDToObserve+1)
			}, 2*time.Minute)
			if !found {
				ef("FAILED_OVER notification was not received for %s TLS config", tlsTest.name)
			}
			failedOverData := logs2.ExtractDataFromLogMessage(match)
			p("FAILED_OVER notification received for %s. %v", tlsTest.name, failedOverData)

			// Wait for failover to complete
			status, err := faultInjector.WaitForAction(ctx, failoverResp.ActionID,
				WithMaxWaitTime(240*time.Second),
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				ef("[FI] Failover action failed for %s: %v", tlsTest.name, err)
			}
			p("[FI] Failover action completed for %s: %s", tlsTest.name, status.Status)

			// Test migration with this TLS configuration
			p("Testing migration with %s TLS configuration...", tlsTest.name)
			migrateResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
				Type: "migrate",
				Parameters: map[string]interface{}{
					"cluster_index": "0",
				},
			})
			if err != nil {
				ef("Failed to trigger migrate action for %s: %v", tlsTest.name, err)
			}

			// Wait for MIGRATING notification
			match, found = logCollector.WaitForLogMatchFunc(func(s string) bool {
				return strings.Contains(s, logs2.ProcessingNotificationMessage) && strings.Contains(s, "MIGRATING")
			}, 30*time.Second)
			if !found {
				ef("MIGRATING notification was not received for %s TLS config", tlsTest.name)
			}
			migrateData := logs2.ExtractDataFromLogMessage(match)
			p("MIGRATING notification received for %s: %v", tlsTest.name, migrateData)

			// Wait for migration to complete
			status, err = faultInjector.WaitForAction(ctx, migrateResp.ActionID,
				WithMaxWaitTime(240*time.Second),
				WithPollInterval(2*time.Second),
			)
			if err != nil {
				ef("[FI] Migrate action failed for %s: %v", tlsTest.name, err)
			}
			p("[FI] Migrate action completed for %s: %s", tlsTest.name, status.Status)

			// Continue traffic for a bit to observe TLS behavior
			time.Sleep(5 * time.Second)
			commandsRunner.Stop()

			// Analyze results for this TLS configuration
			trackerAnalysis := tracker.GetAnalysis()
			if trackerAnalysis.NotificationProcessingErrors > 0 {
				e("Notification processing errors with %s TLS config: %d", tlsTest.name, trackerAnalysis.NotificationProcessingErrors)
			}

			if trackerAnalysis.UnexpectedNotificationCount > 0 {
				e("Unexpected notifications with %s TLS config: %d", tlsTest.name, trackerAnalysis.UnexpectedNotificationCount)
			}

			// Validate we received expected notifications
			if trackerAnalysis.FailingOverCount == 0 {
				e("Expected FAILING_OVER notifications with %s TLS config, got none", tlsTest.name)
			}
			if trackerAnalysis.FailedOverCount == 0 {
				e("Expected FAILED_OVER notifications with %s TLS config, got none", tlsTest.name)
			}
			if trackerAnalysis.MigratingCount == 0 {
				e("Expected MIGRATING notifications with %s TLS config, got none", tlsTest.name)
			}

			// TLS-specific validations
			stats := commandsRunner.GetStats()
			switch tlsTest.name {
			case "NoTLS":
				// Plain text should work fine
				p("Plain text connection processed %d operations", stats.Operations)
			case "TLSInsecure", "TLSMinimal":
				// Insecure TLS should work in test environments
				p("Insecure TLS connection processed %d operations", stats.Operations)
				if stats.Operations == 0 {
					e("Expected operations with %s TLS config, got none", tlsTest.name)
				}
			case "TLSStrict":
				// Strict TLS might have different performance characteristics
				p("Strict TLS connection processed %d operations", stats.Operations)
			}

			p("TLS configuration %s test completed successfully", tlsTest.name)
		})

		// Clear logs between TLS configuration tests
		logCollector.ClearLogs()
	}

	p("All TLS configurations tested successfully")
}
