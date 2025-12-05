package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// DiagnosticsEvent represents a notification event
// it may be a push notification or an error when processing
// push notifications
type DiagnosticsEvent struct {
	// is this pre or post hook
	Type   string `json:"type"`
	ConnID uint64 `json:"connID"`
	SeqID  int64  `json:"seqID"`

	Error error `json:"error"`

	Pre       bool                   `json:"pre"`
	Timestamp time.Time              `json:"timestamp"`
	Details   map[string]interface{} `json:"details"`
}

// TrackingNotificationsHook is a notification hook that tracks notifications
type TrackingNotificationsHook struct {
	// unique connection count
	connectionCount atomic.Int64

	// timeouts
	relaxedTimeoutCount   atomic.Int64
	unrelaxedTimeoutCount atomic.Int64

	notificationProcessingErrors atomic.Int64

	// notification types
	totalNotifications          atomic.Int64
	migratingCount              atomic.Int64
	migratedCount               atomic.Int64
	failingOverCount            atomic.Int64
	failedOverCount             atomic.Int64
	movingCount                 atomic.Int64
	unexpectedNotificationCount atomic.Int64

	diagnosticsLog []DiagnosticsEvent
	connIds        map[uint64]bool
	connLogs       map[uint64][]DiagnosticsEvent
	mutex          sync.RWMutex
}

// NewTrackingNotificationsHook creates a new notification hook with counters
func NewTrackingNotificationsHook() *TrackingNotificationsHook {
	return &TrackingNotificationsHook{
		diagnosticsLog: make([]DiagnosticsEvent, 0),
		connIds:        make(map[uint64]bool),
		connLogs:       make(map[uint64][]DiagnosticsEvent),
	}
}

// it is not reusable, but just to keep it consistent
// with the log collector
func (tnh *TrackingNotificationsHook) Clear() {
	tnh.mutex.Lock()
	defer tnh.mutex.Unlock()
	tnh.diagnosticsLog = make([]DiagnosticsEvent, 0)
	tnh.connIds = make(map[uint64]bool)
	tnh.connLogs = make(map[uint64][]DiagnosticsEvent)
	tnh.relaxedTimeoutCount.Store(0)
	tnh.unrelaxedTimeoutCount.Store(0)
	tnh.notificationProcessingErrors.Store(0)
	tnh.totalNotifications.Store(0)
	tnh.migratingCount.Store(0)
	tnh.migratedCount.Store(0)
	tnh.failingOverCount.Store(0)
}
// wait for notification in prehook
func (tnh *TrackingNotificationsHook) FindOrWaitForNotification(notificationType string, timeout time.Duration) (notification []interface{}, found bool) {
	if notification, found := tnh.FindNotification(notificationType); found {
		return notification, true
	}

	// wait for notification
	timeoutCh := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-timeoutCh:
			return nil, false
		case <-ticker.C:
			if notification, found := tnh.FindNotification(notificationType); found {
				return notification, true
			}
		}
	}
}

func (tnh *TrackingNotificationsHook) FindNotification(notificationType string) (notification []interface{}, found bool) {
	tnh.mutex.RLock()
	defer tnh.mutex.RUnlock()
	for _, event := range tnh.diagnosticsLog {
		if event.Type == notificationType {
			return event.Details["notification"].([]interface{}), true
		}
	}
	return nil, false
}

// PreHook captures timeout-related events before processing
func (tnh *TrackingNotificationsHook) PreHook(_ context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}) ([]interface{}, bool) {
	tnh.increaseNotificationCount(notificationType)
	tnh.storeDiagnosticsEvent(notificationType, notification, notificationCtx)
	tnh.increaseRelaxedTimeoutCount(notificationType)
	return notification, true
}

func (tnh *TrackingNotificationsHook) getConnID(notificationCtx push.NotificationHandlerContext) uint64 {
	if conn, ok := notificationCtx.Conn.(*pool.Conn); ok {
		return conn.GetID()
	}
	return 0
}

func (tnh *TrackingNotificationsHook) getSeqID(notification []interface{}) int64 {
	seqID, ok := notification[1].(int64)
	if !ok {
		return 0
	}
	return seqID
}

// PostHook captures the result after processing push notification
func (tnh *TrackingNotificationsHook) PostHook(_ context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}, err error) {
	if err != nil {
		event := DiagnosticsEvent{
			Type:      notificationType + "_ERROR",
			ConnID:    tnh.getConnID(notificationCtx),
			SeqID:     tnh.getSeqID(notification),
			Error:     err,
			Timestamp: time.Now(),
			Details: map[string]interface{}{
				"notification": notification,
				"context":      "post-hook",
			},
		}

		tnh.notificationProcessingErrors.Add(1)
		tnh.mutex.Lock()
		tnh.diagnosticsLog = append(tnh.diagnosticsLog, event)
		tnh.mutex.Unlock()
	}
}

func (tnh *TrackingNotificationsHook) storeDiagnosticsEvent(notificationType string, notification []interface{}, notificationCtx push.NotificationHandlerContext) {
	connID := tnh.getConnID(notificationCtx)
	event := DiagnosticsEvent{
		Type:      notificationType,
		ConnID:    connID,
		SeqID:     tnh.getSeqID(notification),
		Pre:       true,
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"notification": notification,
			"context":      "pre-hook",
		},
	}

	tnh.mutex.Lock()
	if v, ok := tnh.connIds[connID]; !ok || !v {
		tnh.connIds[connID] = true
		tnh.connectionCount.Add(1)
	}
	tnh.connLogs[connID] = append(tnh.connLogs[connID], event)
	tnh.diagnosticsLog = append(tnh.diagnosticsLog, event)
	tnh.mutex.Unlock()
}

// GetRelaxedTimeoutCount returns the count of relaxed timeout events
func (tnh *TrackingNotificationsHook) GetRelaxedTimeoutCount() int64 {
	return tnh.relaxedTimeoutCount.Load()
}

// GetUnrelaxedTimeoutCount returns the count of unrelaxed timeout events
func (tnh *TrackingNotificationsHook) GetUnrelaxedTimeoutCount() int64 {
	return tnh.unrelaxedTimeoutCount.Load()
}

// GetNotificationProcessingErrors returns the count of timeout errors
func (tnh *TrackingNotificationsHook) GetNotificationProcessingErrors() int64 {
	return tnh.notificationProcessingErrors.Load()
}

// GetTotalNotifications returns the total number of notifications processed
func (tnh *TrackingNotificationsHook) GetTotalNotifications() int64 {
	return tnh.totalNotifications.Load()
}

// GetConnectionCount returns the current connection count
func (tnh *TrackingNotificationsHook) GetConnectionCount() int64 {
	return tnh.connectionCount.Load()
}

// GetMovingCount returns the count of MOVING notifications
func (tnh *TrackingNotificationsHook) GetMovingCount() int64 {
	return tnh.movingCount.Load()
}

// GetDiagnosticsLog returns a copy of the diagnostics log
func (tnh *TrackingNotificationsHook) GetDiagnosticsLog() []DiagnosticsEvent {
	tnh.mutex.RLock()
	defer tnh.mutex.RUnlock()

	logCopy := make([]DiagnosticsEvent, len(tnh.diagnosticsLog))
	copy(logCopy, tnh.diagnosticsLog)
	return logCopy
}

func (tnh *TrackingNotificationsHook) increaseNotificationCount(notificationType string) {
	tnh.totalNotifications.Add(1)
	switch notificationType {
	case "MOVING":
		tnh.movingCount.Add(1)
	case "MIGRATING", "SMIGRATING":
		tnh.migratingCount.Add(1)
	case "MIGRATED", "SMIGRATED":
		tnh.migratedCount.Add(1)
	case "FAILING_OVER":
		tnh.failingOverCount.Add(1)
	case "FAILED_OVER":
		tnh.failedOverCount.Add(1)
	default:
		tnh.unexpectedNotificationCount.Add(1)
	}
}

func (tnh *TrackingNotificationsHook) increaseRelaxedTimeoutCount(notificationType string) {
	switch notificationType {
	case "MIGRATING", "SMIGRATING", "FAILING_OVER":
		tnh.relaxedTimeoutCount.Add(1)
	case "MIGRATED", "SMIGRATED", "FAILED_OVER":
		tnh.unrelaxedTimeoutCount.Add(1)
	}
}

func (tnh *TrackingNotificationsHook) GetAnalysis() *DiagnosticsAnalysis {
	return NewDiagnosticsAnalysis(tnh.GetDiagnosticsLog())
}

func (tnh *TrackingNotificationsHook) GetDiagnosticsLogForConn(connID uint64) []DiagnosticsEvent {
	tnh.mutex.RLock()
	defer tnh.mutex.RUnlock()

	var connLogs []DiagnosticsEvent
	for _, log := range tnh.diagnosticsLog {
		if log.ConnID == connID {
			connLogs = append(connLogs, log)
		}
	}
	return connLogs
}

func (tnh *TrackingNotificationsHook) GetAnalysisForConn(connID uint64) *DiagnosticsAnalysis {
	return NewDiagnosticsAnalysis(tnh.GetDiagnosticsLogForConn(connID))
}

type DiagnosticsAnalysis struct {
	RelaxedTimeoutCount          int64
	UnrelaxedTimeoutCount        int64
	NotificationProcessingErrors int64
	ConnectionCount              int64
	MovingCount                  int64
	MigratingCount               int64
	MigratedCount                int64
	FailingOverCount             int64
	FailedOverCount              int64
	UnexpectedNotificationCount  int64
	TotalNotifications           int64
	diagnosticsLog               []DiagnosticsEvent
	connLogs                     map[uint64][]DiagnosticsEvent
	connIds                      map[uint64]bool
}

func NewDiagnosticsAnalysis(diagnosticsLog []DiagnosticsEvent) *DiagnosticsAnalysis {
	da := &DiagnosticsAnalysis{
		diagnosticsLog: diagnosticsLog,
		connLogs:       make(map[uint64][]DiagnosticsEvent),
		connIds:        make(map[uint64]bool),
	}

	da.Analyze()
	return da
}

func (a *DiagnosticsAnalysis) Analyze() {
	for _, log := range a.diagnosticsLog {
		a.TotalNotifications++
		switch log.Type {
		case "MOVING":
			a.MovingCount++
		case "MIGRATING", "SMIGRATING":
			a.MigratingCount++
		case "MIGRATED", "SMIGRATED":
			a.MigratedCount++
		case "FAILING_OVER":
			a.FailingOverCount++
		case "FAILED_OVER":
			a.FailedOverCount++
		default:
			a.UnexpectedNotificationCount++
		}
		if log.Error != nil {
			fmt.Printf("[ERROR] Notification processing error: %v\n", log.Error)
			fmt.Printf("[ERROR] Notification: %v\n", log.Details["notification"])
			fmt.Printf("[ERROR] Context: %v\n", log.Details["context"])
			a.NotificationProcessingErrors++
		}
		switch log.Type {
		case "MIGRATING", "SMIGRATING", "FAILING_OVER":
			a.RelaxedTimeoutCount++
		case "MIGRATED", "SMIGRATED", "FAILED_OVER":
			a.UnrelaxedTimeoutCount++
		}
		if log.ConnID != 0 {
			if v, ok := a.connIds[log.ConnID]; !ok || !v {
				a.connIds[log.ConnID] = true
				a.connLogs[log.ConnID] = make([]DiagnosticsEvent, 0)
				a.ConnectionCount++
			}
			a.connLogs[log.ConnID] = append(a.connLogs[log.ConnID], log)
		}

	}
}

func (a *DiagnosticsAnalysis) Print(t *testing.T) {
	t.Logf("Notification Analysis results for %d events and %d connections:", len(a.diagnosticsLog), len(a.connIds))
	t.Logf("-------------")
	t.Logf("-Timeout Analysis based on type of notification-")
	t.Logf("Note: MIGRATED and FAILED_OVER notifications are not tracked by the hook, so they are not included in the relaxed/unrelaxed count")
	t.Logf("Note: The hook only tracks timeouts that occur after the notification is processed, so timeouts that occur during processing are not included")
	t.Logf("-------------")
	t.Logf(" - Relaxed Timeout Count: %d", a.RelaxedTimeoutCount)
	t.Logf(" - Unrelaxed Timeout Count: %d", a.UnrelaxedTimeoutCount)
	t.Logf("-------------")
	t.Logf("-Notification Analysis-")
	t.Logf("-------------")
	t.Logf(" - MOVING: %d", a.MovingCount)
	t.Logf(" - MIGRATING: %d", a.MigratingCount)
	t.Logf(" - MIGRATED: %d", a.MigratedCount)
	t.Logf(" - FAILING_OVER: %d", a.FailingOverCount)
	t.Logf(" - FAILED_OVER: %d", a.FailedOverCount)
	t.Logf(" - Unexpected: %d", a.UnexpectedNotificationCount)
	t.Logf("-------------")
	t.Logf(" - Total Notifications: %d", a.TotalNotifications)
	t.Logf(" - Notification Processing Errors: %d", a.NotificationProcessingErrors)
	t.Logf(" - Connection Count: %d", a.ConnectionCount)
	t.Logf("-------------")
	t.Logf("Diagnostics Analysis completed successfully")
}

// setupNotificationHook adds a notification hook to a cluster client
//
//nolint:unused // Used in test files
func setupNotificationHook(client *redis.ClusterClient, hook maintnotifications.NotificationHook) {
	_ = client.ForEachShard(context.Background(), func(ctx context.Context, nodeClient *redis.Client) error {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.AddNotificationHook(hook)
		}
		return nil
	})

	// Also add hook to new nodes
	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.AddNotificationHook(hook)
		}
	})
}

// setupNotificationHooks adds multiple notification hooks to a regular client
//
//nolint:unused // Used in test files
func setupNotificationHooks(client redis.UniversalClient, hooks ...maintnotifications.NotificationHook) {
	// Try to get manager from the client
	var manager *maintnotifications.Manager

	// Check if it's a regular client
	if regularClient, ok := client.(*redis.Client); ok {
		manager = regularClient.GetMaintNotificationsManager()
	}

	// Check if it's a cluster client
	if clusterClient, ok := client.(*redis.ClusterClient); ok {
		// For cluster clients, add hooks to all shards
		_ = clusterClient.ForEachShard(context.Background(), func(ctx context.Context, nodeClient *redis.Client) error {
			nodeManager := nodeClient.GetMaintNotificationsManager()
			if nodeManager != nil {
				for _, hook := range hooks {
					nodeManager.AddNotificationHook(hook)
				}
			}
			return nil
		})

		// Also add hooks to new nodes
		clusterClient.OnNewNode(func(nodeClient *redis.Client) {
			nodeManager := nodeClient.GetMaintNotificationsManager()
			if nodeManager != nil {
				for _, hook := range hooks {
					nodeManager.AddNotificationHook(hook)
				}
			}
		})
		return
	}

	// For regular clients, add hooks directly
	if manager != nil {
		for _, hook := range hooks {
			manager.AddNotificationHook(hook)
		}
	}
}
