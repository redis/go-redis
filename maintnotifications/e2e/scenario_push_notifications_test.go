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

// TestPushNotifications tests Redis Enterprise push notifications (MOVING, MIGRATING, MIGRATED)
func TestPushNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Setup: Create fresh database and client factory for this test
	bdbID, factory, cleanup := SetupTestDatabaseAndFactory(t, ctx, "standalone")
	defer cleanup()
	t.Logf("[PUSH-NOTIFICATIONS] Created test database with bdb_id: %d", bdbID)

	// Wait for database to be fully ready
	time.Sleep(10 * time.Second)

	var dump = true
	var seqIDToObserve int64
	var connIDToObserve uint64

	var match string
	var found bool

	var status *ActionStatusResponse
	var errorsDetected = false

	var p = func(format string, args ...interface{}) {
		printLog("PUSH-NOTIFICATIONS", false, format, args...)
	}

	var e = func(format string, args ...interface{}) {
		errorsDetected = true
		printLog("PUSH-NOTIFICATIONS", true, format, args...)
	}

	var ef = func(format string, args ...interface{}) {
		printLog("PUSH-NOTIFICATIONS", true, format, args...)
		t.FailNow()
	}

	logCollector.ClearLogs()
	defer func() {
		logCollector.Clear()
	}()

	// Get endpoint config from factory (now connected to new database)
	endpointConfig := factory.GetConfig()

	// Create fault injector
	faultInjector, err := CreateTestFaultInjector()
	if err != nil {
		ef("Failed to create fault injector: %v", err)
	}

	minIdleConns := 5
	poolSize := 10
	maxConnections := 15
	// Create Redis client with push notifications enabled
	client, err := factory.Create("push-notification-client", &CreateClientOptions{
		Protocol:       3, // RESP3 required for push notifications
		PoolSize:       poolSize,
		MinIdleConns:   minIdleConns,
		MaxActiveConns: maxConnections,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:                       maintnotifications.ModeEnabled,
			HandoffTimeout:             40 * time.Second, // 30 seconds
			RelaxedTimeout:             10 * time.Second, // 10 seconds relaxed timeout
			PostHandoffRelaxedDuration: 2 * time.Second,  // 2 seconds post-handoff relaxed duration
			MaxWorkers:                 20,
			EndpointType:               maintnotifications.EndpointTypeExternalIP, // Use external IP for enterprise
		},
		ClientName: "push-notification-test-client",
	})
	if err != nil {
		ef("Failed to create client: %v", err)
	}

	defer func() {
		factory.DestroyAll()
	}()

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
		ef("Failed to ping Redis: %v", err)
	}

	p("Client connected successfully, starting push notification test")

	commandsRunner, _ := NewCommandRunner(client)
	defer func() {
		if dump {
			p("Command runner stats:")
			p("Operations: %d, Errors: %d, Timeout Errors: %d",
				commandsRunner.GetStats().Operations, commandsRunner.GetStats().Errors, commandsRunner.GetStats().TimeoutErrors)
		}
		p("Stopping command runner...")
		commandsRunner.Stop()
	}()

	p("Starting FAILING_OVER / FAILED_OVER notifications test...")
	// Test: Trigger failover action to generate FAILING_OVER, FAILED_OVER notifications
	p("Triggering failover action to generate push notifications...")
	failoverResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
		Type: "failover",
		Parameters: map[string]interface{}{
			"bdb_id": endpointConfig.BdbID,
		},
	})
	if err != nil {
		ef("Failed to trigger failover action: %v", err)
	}
	go func() {
		p("Waiting for FAILING_OVER notification")
		match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
			return strings.Contains(s, logs2.ProcessingNotificationMessage) && notificationType(s, "FAILING_OVER")
		}, 3*time.Minute)
		commandsRunner.Stop()
	}()
	commandsRunner.FireCommandsUntilStop(ctx)
	if !found {
		ef("FAILING_OVER notification was not received within 3 minutes")
	}
	failingOverData := logs2.ExtractDataFromLogMessage(match)
	p("FAILING_OVER notification received. %v", failingOverData)
	seqIDToObserve = int64(failingOverData["seqID"].(float64))
	connIDToObserve = uint64(failingOverData["connID"].(float64))
	go func() {
		p("Waiting for FAILED_OVER notification on conn %d with seqID %d...", connIDToObserve, seqIDToObserve+1)
		match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
			return notificationType(s, "FAILED_OVER") && connID(s, connIDToObserve) && seqID(s, seqIDToObserve+1)
		}, 3*time.Minute)
		commandsRunner.Stop()
	}()
	commandsRunner.FireCommandsUntilStop(ctx)
	if !found {
		ef("FAILED_OVER notification was not received within 3 minutes")
	}
	failedOverData := logs2.ExtractDataFromLogMessage(match)
	p("FAILED_OVER notification received. %v", failedOverData)

	status, err = faultInjector.WaitForAction(ctx, failoverResp.ActionID,
		WithMaxWaitTime(240*time.Second),
		WithPollInterval(2*time.Second),
	)
	if err != nil {
		ef("[FI] Failover action failed: %v", err)
	}
	p("[FI] Failover action completed: %v %s", status.Status, actionOutputIfFailed(status))

	p("FAILING_OVER / FAILED_OVER notifications test completed successfully")

	// Test: Trigger migrate action to generate MOVING, MIGRATING, MIGRATED notifications
	p("Triggering migrate action to generate push notifications...")
	migrateResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
		Type: "migrate",
		Parameters: map[string]interface{}{
			"bdb_id": endpointConfig.BdbID,
		},
	})
	if err != nil {
		ef("Failed to trigger migrate action: %v", err)
	}
	go func() {
		match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
			return strings.Contains(s, logs2.ProcessingNotificationMessage) && strings.Contains(s, "MIGRATING")
		}, 60*time.Second)
		commandsRunner.Stop()
	}()
	commandsRunner.FireCommandsUntilStop(ctx)
	if !found {
		status, err = faultInjector.WaitForAction(ctx, migrateResp.ActionID,
			WithMaxWaitTime(240*time.Second),
			WithPollInterval(2*time.Second),
		)
		if err != nil {
			ef("[FI] Migrate action failed: %v", err)
		}
		p("[FI] Migrate action completed: %s %s", status.Status, actionOutputIfFailed(status))
		ef("MIGRATING notification for migrate action was not received within 60 seconds")
	}
	migrateData := logs2.ExtractDataFromLogMessage(match)
	seqIDToObserve = int64(migrateData["seqID"].(float64))
	connIDToObserve = uint64(migrateData["connID"].(float64))
	p("MIGRATING notification received: seqID: %d, connID: %d", seqIDToObserve, connIDToObserve)

	status, err = faultInjector.WaitForAction(ctx, migrateResp.ActionID,
		WithMaxWaitTime(240*time.Second),
		WithPollInterval(2*time.Second),
	)
	if err != nil {
		ef("[FI] Migrate action failed: %v", err)
	}
	p("[FI] Migrate action completed: %s %s", status.Status, actionOutputIfFailed(status))

	go func() {
		p("Waiting for MIGRATED notification on conn %d with seqID %d...", connIDToObserve, seqIDToObserve+1)
		match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
			return notificationType(s, "MIGRATED") && connID(s, connIDToObserve) && seqID(s, seqIDToObserve+1)
		}, 3*time.Minute)
		commandsRunner.Stop()
	}()
	commandsRunner.FireCommandsUntilStop(ctx)
	if !found {
		ef("MIGRATED notification was not received within 3 minutes")
	}
	migratedData := logs2.ExtractDataFromLogMessage(match)
	p("MIGRATED notification received. %v", migratedData)

	p("MIGRATING / MIGRATED notifications test completed successfully")

	// Trigger bind action to complete the migration process
	p("Triggering bind action to complete migration...")

	bindResp, err := faultInjector.TriggerAction(ctx, ActionRequest{
		Type: "bind",
		Parameters: map[string]interface{}{
			"bdb_id": endpointConfig.BdbID,
		},
	})
	if err != nil {
		ef("Failed to trigger bind action: %v", err)
	}

	// start a second client but don't execute any commands on it
	p("Starting a second client to observe notification during moving...")
	client2, err := factory.Create("push-notification-client-2", &CreateClientOptions{
		Protocol:       3, // RESP3 required for push notifications
		PoolSize:       poolSize,
		MinIdleConns:   minIdleConns,
		MaxActiveConns: maxConnections,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:                       maintnotifications.ModeEnabled,
			HandoffTimeout:             40 * time.Second, // 30 seconds
			RelaxedTimeout:             30 * time.Minute, // 30 minutes relaxed timeout for second client
			PostHandoffRelaxedDuration: 2 * time.Second,  // 2 seconds post-handoff relaxed duration
			MaxWorkers:                 20,
			EndpointType:               maintnotifications.EndpointTypeExternalIP, // Use external IP for enterprise
		},
		ClientName: "push-notification-test-client-2",
	})

	if err != nil {
		ef("failed to create client: %v", err)
	}
	// setup tracking for second client
	tracker2 := NewTrackingNotificationsHook()
	logger2 := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(client2, tracker2, logger2)
	commandsRunner2, _ := NewCommandRunner(client2)
	p("Second client created")

	// Use a channel to communicate errors from the goroutine
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("goroutine panic: %v", r)
			}
		}()

		p("Waiting for MOVING notification on first client")
		match, found = logCollector.MatchOrWaitForLogMatchFunc(func(s string) bool {
			return strings.Contains(s, logs2.ProcessingNotificationMessage) && notificationType(s, "MOVING")
		}, 3*time.Minute)
		commandsRunner.Stop()
		if !found {
			errChan <- fmt.Errorf("MOVING notification was not received within 3 minutes ON A FIRST CLIENT")
			return
		}

		// once moving is received, start a second client commands runner
		p("Starting commands on second client")
		go commandsRunner2.FireCommandsUntilStop(ctx)
		defer func() {
			// stop the second runner
			commandsRunner2.Stop()
			// destroy the second client
			factory.Destroy("push-notification-client-2")
		}()

		p("Waiting for MOVING notification on second client")
		matchNotif, fnd := tracker2.FindOrWaitForNotification("MOVING", 3*time.Minute)
		if !fnd {
			errChan <- fmt.Errorf("MOVING notification was not received within 3 minutes ON A SECOND CLIENT")
			return
		} else {
			p("MOVING notification received on second client %v", matchNotif)
		}

		// Signal success
		errChan <- nil
	}()
	commandsRunner.FireCommandsUntilStop(ctx)
	// wait for moving on first client
	// once the commandRunner stops, it means a waiting
	// on the logCollector match has completed and we can proceed
	if !found {
		ef("MOVING notification was not received within 3 minutes")
	}
	movingData := logs2.ExtractDataFromLogMessage(match)
	p("MOVING notification received. %v", movingData)
	seqIDToObserve = int64(movingData["seqID"].(float64))
	connIDToObserve = uint64(movingData["connID"].(float64))

	time.Sleep(3 * time.Second)
	// start a third client but don't execute any commands on it
	p("Starting a third client to observe notification during moving...")
	client3, err := factory.Create("push-notification-client-2", &CreateClientOptions{
		Protocol:       3, // RESP3 required for push notifications
		PoolSize:       poolSize,
		MinIdleConns:   minIdleConns,
		MaxActiveConns: maxConnections,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:                       maintnotifications.ModeEnabled,
			HandoffTimeout:             40 * time.Second, // 30 seconds
			RelaxedTimeout:             30 * time.Minute, // 30 minutes relaxed timeout for second client
			PostHandoffRelaxedDuration: 2 * time.Second,  // 2 seconds post-handoff relaxed duration
			MaxWorkers:                 20,
			EndpointType:               maintnotifications.EndpointTypeExternalIP, // Use external IP for enterprise
		},
		ClientName: "push-notification-test-client-3",
	})

	if err != nil {
		ef("failed to create client: %v", err)
	}
	// setup tracking for second client
	tracker3 := NewTrackingNotificationsHook()
	logger3 := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(client3, tracker3, logger3)
	commandsRunner3, _ := NewCommandRunner(client3)
	p("Third client created")
	go commandsRunner3.FireCommandsUntilStop(ctx)
	// wait for moving on third client
	movingNotification, found := tracker3.FindOrWaitForNotification("MOVING", 3*time.Minute)
	if !found {
		p("[NOTICE] MOVING notification was not received within 3 minutes ON A THIRD CLIENT")
	} else {
		p("MOVING notification received on third client. %v", movingNotification)
		if len(movingNotification) != 4 {
			p("[NOTICE] Invalid MOVING notification format: %s", movingNotification)
		}
		mNotifTimeS, ok := movingNotification[2].(int64)
		if !ok {
			p("[NOTICE] Invalid timeS in MOVING notification: %s", movingNotification)
		}
		// expect timeS to be less than 15
		if mNotifTimeS < 15 {
			p("[NOTICE] Expected timeS < 15, got %d", mNotifTimeS)
		}
	}
	commandsRunner3.Stop()
	// Wait for the goroutine to complete and check for errors
	if err := <-errChan; err != nil {
		ef("Second client goroutine error: %v", err)
	}

	// Wait for bind action to complete
	bindStatus, err := faultInjector.WaitForAction(ctx, bindResp.ActionID,
		WithMaxWaitTime(240*time.Second),
		WithPollInterval(2*time.Second))
	if err != nil {
		ef("Bind action failed: %v", err)
	}

	p("Bind action completed: %s %s", bindStatus.Status, actionOutputIfFailed(bindStatus))

	p("MOVING notification test completed successfully")

	p("Executing commands and collecting logs for analysis... This will take 30 seconds...")
	go commandsRunner.FireCommandsUntilStop(ctx)
	time.Sleep(30 * time.Second)
	commandsRunner.Stop()
	allLogsAnalysis := logCollector.GetAnalysis()
	trackerAnalysis := tracker.GetAnalysis()

	if allLogsAnalysis.TimeoutErrorsCount > 0 {
		e("Unexpected timeout errors: %d", allLogsAnalysis.TimeoutErrorsCount)
	}
	if trackerAnalysis.UnexpectedNotificationCount > 0 {
		e("Unexpected notifications: %d", trackerAnalysis.UnexpectedNotificationCount)
	}
	if trackerAnalysis.NotificationProcessingErrors > 0 {
		e("Notification processing errors: %d", trackerAnalysis.NotificationProcessingErrors)
	}
	if allLogsAnalysis.RelaxedTimeoutCount == 0 {
		e("Expected relaxed timeouts, got none")
	}
	if allLogsAnalysis.UnrelaxedTimeoutCount == 0 {
		e("Expected unrelaxed timeouts, got none")
	}
	if allLogsAnalysis.UnrelaxedAfterMoving == 0 {
		e("Expected unrelaxed timeouts after moving, got none")
	}
	if allLogsAnalysis.RelaxedPostHandoffCount == 0 {
		e("Expected relaxed timeouts after post-handoff, got none")
	}
	// validate number of connections we do not exceed max connections
	// we started three clients, so we expect 3x the connections
	if allLogsAnalysis.ConnectionCount > int64(maxConnections)*3 {
		e("Expected no more than %d connections, got %d", maxConnections*3, allLogsAnalysis.ConnectionCount)
	}

	if allLogsAnalysis.ConnectionCount < int64(minIdleConns) {
		e("Expected at least %d connections, got %d", minIdleConns, allLogsAnalysis.ConnectionCount)
	}

	// validate logs are present for all connections
	for connID := range trackerAnalysis.connIds {
		if len(allLogsAnalysis.connLogs[connID]) == 0 {
			e("No logs found for connection %d", connID)
		}
	}

	// validate number of notifications in tracker matches number of notifications in logs
	// allow for more moving in the logs since we started a second client
	if trackerAnalysis.TotalNotifications > allLogsAnalysis.TotalNotifications {
		e("Expected %d or more notifications, got %d", trackerAnalysis.TotalNotifications, allLogsAnalysis.TotalNotifications)
	}

	// and per type
	// allow for more moving in the logs since we started a second client
	if trackerAnalysis.MovingCount > allLogsAnalysis.MovingCount {
		e("Expected %d or more MOVING notifications, got %d", trackerAnalysis.MovingCount, allLogsAnalysis.MovingCount)
	}

	if trackerAnalysis.MigratingCount != allLogsAnalysis.MigratingCount {
		e("Expected %d MIGRATING notifications, got %d", trackerAnalysis.MigratingCount, allLogsAnalysis.MigratingCount)
	}

	if trackerAnalysis.MigratedCount != allLogsAnalysis.MigratedCount {
		e("Expected %d MIGRATED notifications, got %d", trackerAnalysis.MigratedCount, allLogsAnalysis.MigratedCount)
	}

	if trackerAnalysis.FailingOverCount != allLogsAnalysis.FailingOverCount {
		e("Expected %d FAILING_OVER notifications, got %d", trackerAnalysis.FailingOverCount, allLogsAnalysis.FailingOverCount)
	}

	if trackerAnalysis.FailedOverCount != allLogsAnalysis.FailedOverCount {
		e("Expected %d FAILED_OVER notifications, got %d", trackerAnalysis.FailedOverCount, allLogsAnalysis.FailedOverCount)
	}

	if trackerAnalysis.UnexpectedNotificationCount != allLogsAnalysis.UnexpectedCount {
		e("Expected %d unexpected notifications, got %d", trackerAnalysis.UnexpectedNotificationCount, allLogsAnalysis.UnexpectedCount)
	}

	// unrelaxed (and relaxed) after moving wont be tracked by the hook, so we have to exclude it
	if trackerAnalysis.UnrelaxedTimeoutCount != allLogsAnalysis.UnrelaxedTimeoutCount-allLogsAnalysis.UnrelaxedAfterMoving {
		e("Expected %d unrelaxed timeouts, got %d", trackerAnalysis.UnrelaxedTimeoutCount, allLogsAnalysis.UnrelaxedTimeoutCount)
	}
	if trackerAnalysis.RelaxedTimeoutCount != allLogsAnalysis.RelaxedTimeoutCount-allLogsAnalysis.RelaxedPostHandoffCount {
		e("Expected %d relaxed timeouts, got %d", trackerAnalysis.RelaxedTimeoutCount, allLogsAnalysis.RelaxedTimeoutCount)
	}

	// validate all handoffs succeeded
	if allLogsAnalysis.FailedHandoffCount > 0 {
		e("Expected no failed handoffs, got %d", allLogsAnalysis.FailedHandoffCount)
	}
	if allLogsAnalysis.SucceededHandoffCount == 0 {
		e("Expected at least one successful handoff, got none")
	}
	if allLogsAnalysis.TotalHandoffCount != allLogsAnalysis.SucceededHandoffCount {
		e("Expected total handoffs to match successful handoffs, got %d != %d", allLogsAnalysis.TotalHandoffCount, allLogsAnalysis.SucceededHandoffCount)
	}

	// no additional retries
	if allLogsAnalysis.TotalHandoffRetries != allLogsAnalysis.TotalHandoffCount {
		e("Expected no additional handoff retries, got %d", allLogsAnalysis.TotalHandoffRetries-allLogsAnalysis.TotalHandoffCount)
	}

	if errorsDetected {
		logCollector.DumpLogs()
		trackerAnalysis.Print(t)
		logCollector.Clear()
		tracker.Clear()
		ef("[FAIL] Errors detected in push notification test")
	}

	p("Analysis complete, no errors found")
	allLogsAnalysis.Print(t)
	trackerAnalysis.Print(t)
	p("Command runner stats:")
	p("Operations: %d, Errors: %d, Timeout Errors: %d",
		commandsRunner.GetStats().Operations, commandsRunner.GetStats().Errors, commandsRunner.GetStats().TimeoutErrors)

	p("Push notification test completed successfully")
}
