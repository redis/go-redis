package hitless

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9/internal"
)

// MetricsHook collects metrics about notification processing.
type MetricsHook struct {
	NotificationCounts map[string]int64
	ProcessingTimes    map[string]time.Duration
	ErrorCounts        map[string]int64
}

// NewMetricsHook creates a new metrics collection hook.
func NewMetricsHook() *MetricsHook {
	return &MetricsHook{
		NotificationCounts: make(map[string]int64),
		ProcessingTimes:    make(map[string]time.Duration),
		ErrorCounts:        make(map[string]int64),
	}
}

// PreHook records the start time for processing metrics.
func (mh *MetricsHook) PreHook(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool) {
	mh.NotificationCounts[notificationType]++

	// Store start time in context for duration calculation
	startTime := time.Now()
	ctx = context.WithValue(ctx, "start_time", startTime)

	return notification, true
}

// PostHook records processing completion and any errors.
func (mh *MetricsHook) PostHook(ctx context.Context, notificationType string, notification []interface{}, result error) {
	// Calculate processing duration
	if startTime, ok := ctx.Value("start_time").(time.Time); ok {
		duration := time.Since(startTime)
		mh.ProcessingTimes[notificationType] = duration
	}

	// Record errors
	if result != nil {
		mh.ErrorCounts[notificationType]++
	}
}

// GetMetrics returns a summary of collected metrics.
func (mh *MetricsHook) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"notification_counts": mh.NotificationCounts,
		"processing_times":    mh.ProcessingTimes,
		"error_counts":        mh.ErrorCounts,
	}
}

// EndpointRewriteHook rewrites endpoints based on configured rules.
type EndpointRewriteHook struct {
	RewriteRules map[string]string // old -> new endpoint mappings
}

// NewEndpointRewriteHook creates a new endpoint rewrite hook.
func NewEndpointRewriteHook(rules map[string]string) *EndpointRewriteHook {
	return &EndpointRewriteHook{
		RewriteRules: rules,
	}
}

// PreHook rewrites endpoints in MOVING notifications based on configured rules.
func (erh *EndpointRewriteHook) PreHook(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool) {
	if notificationType == NotificationMoving && len(notification) > 3 {
		if endpoint, ok := notification[3].(string); ok {
			if newEndpoint, exists := erh.RewriteRules[endpoint]; exists {
				// Create a copy of the notification with rewritten endpoint
				modifiedNotification := make([]interface{}, len(notification))
				copy(modifiedNotification, notification)
				modifiedNotification[3] = newEndpoint

				internal.Logger.Printf(ctx, "hitless: rewriting endpoint %s -> %s", endpoint, newEndpoint)
				return modifiedNotification, true
			}
		}
	}
	return notification, true
}

// PostHook does nothing for endpoint rewrite hook.
func (erh *EndpointRewriteHook) PostHook(ctx context.Context, notificationType string, notification []interface{}, result error) {
	// No post-processing needed
}

// ThrottleHook limits the rate of notification processing.
type ThrottleHook struct {
	MaxNotificationsPerSecond int
	lastNotificationTime      time.Time
	notificationCount         int
}

// NewThrottleHook creates a new throttling hook.
func NewThrottleHook(maxPerSecond int) *ThrottleHook {
	return &ThrottleHook{
		MaxNotificationsPerSecond: maxPerSecond,
		lastNotificationTime:      time.Now(),
	}
}

// PreHook implements rate limiting for notifications.
func (th *ThrottleHook) PreHook(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool) {
	now := time.Now()

	// Reset counter if more than a second has passed
	if now.Sub(th.lastNotificationTime) >= time.Second {
		th.notificationCount = 0
		th.lastNotificationTime = now
	}

	// Check if we've exceeded the rate limit
	if th.notificationCount >= th.MaxNotificationsPerSecond {
		internal.Logger.Printf(ctx, "hitless: throttling %s notification (rate limit: %d/sec)",
			notificationType, th.MaxNotificationsPerSecond)
		return notification, false // Skip processing
	}

	th.notificationCount++
	return notification, true
}

// PostHook does nothing for throttle hook.
func (th *ThrottleHook) PostHook(ctx context.Context, notificationType string, notification []interface{}, result error) {
	// No post-processing needed
}

// ValidationHook validates notification format and content.
type ValidationHook struct {
	StrictMode bool
}

// NewValidationHook creates a new validation hook.
func NewValidationHook(strictMode bool) *ValidationHook {
	return &ValidationHook{
		StrictMode: strictMode,
	}
}

// PreHook validates notification format and content.
func (vh *ValidationHook) PreHook(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool) {
	switch notificationType {
	case NotificationMoving:
		if len(notification) < 3 {
			internal.Logger.Printf(ctx, "hitless: invalid MOVING notification - insufficient fields")
			return notification, false
		}

		// Validate sequence ID
		if seqIDStr, ok := notification[1].(string); ok {
			if seqIDStr == "" {
				internal.Logger.Printf(ctx, "hitless: invalid MOVING notification - empty sequence ID")
				return notification, false
			}
		} else {
			internal.Logger.Printf(ctx, "hitless: invalid MOVING notification - sequence ID not a string")
			return notification, false
		}

		// Validate timeout
		if timeStr, ok := notification[2].(string); ok {
			if timeStr == "" || timeStr == "0" {
				internal.Logger.Printf(ctx, "hitless: invalid MOVING notification - invalid timeout")
				return notification, false
			}
		} else {
			internal.Logger.Printf(ctx, "hitless: invalid MOVING notification - timeout not a string")
			return notification, false
		}

		// In strict mode, validate endpoint format
		if vh.StrictMode && len(notification) > 3 {
			if endpoint, ok := notification[3].(string); ok && endpoint != "" {
				if !strings.Contains(endpoint, ":") {
					internal.Logger.Printf(ctx, "hitless: invalid MOVING notification - malformed endpoint: %s", endpoint)
					return notification, false
				}
			}
		}

	case NotificationMigrating, NotificationMigrated, NotificationFailingOver, NotificationFailedOver:
		if len(notification) < 2 {
			internal.Logger.Printf(ctx, "hitless: invalid %s notification - insufficient fields", notificationType)
			return notification, false
		}
	}

	return notification, true
}

// PostHook does nothing for validation hook.
func (vh *ValidationHook) PostHook(ctx context.Context, notificationType string, notification []interface{}, result error) {
	// No post-processing needed
}

// ExampleUsage demonstrates how to use the hooks and notification types with HitlessManager.
func ExampleUsage() {
	// This is just an example - in real usage, you'd have actual client and config
	fmt.Println("Example of using hooks and notification types with HitlessManager:")
	fmt.Println()

	fmt.Println("1. Using notification type constants:")
	fmt.Printf("   MOVING: %s\n", NotificationMoving)
	fmt.Printf("   MIGRATING: %s\n", NotificationMigrating)
	fmt.Printf("   MIGRATED: %s\n", NotificationMigrated)
	fmt.Printf("   FAILING_OVER: %s\n", NotificationFailingOver)
	fmt.Printf("   FAILED_OVER: %s\n", NotificationFailedOver)
	fmt.Println()

	fmt.Println("2. Using notification type sets:")
	fmt.Println("   // Register handlers for all notification types")
	fmt.Println("   manager.RegisterSelectiveHandlers(AllNotificationTypes())")
	fmt.Println()
	fmt.Println("   // Register handlers only for MOVING notifications")
	fmt.Println("   manager.RegisterSelectiveHandlers(MovingOnlyNotifications())")
	fmt.Println()
	fmt.Println("   // Register handlers only for migration-related notifications")
	fmt.Println("   manager.RegisterSelectiveHandlers(MigrationNotifications())")
	fmt.Println()
	fmt.Println("   // Register handlers only for failover-related notifications")
	fmt.Println("   manager.RegisterSelectiveHandlers(FailoverNotifications())")
	fmt.Println()
	fmt.Println("   // Register handlers for custom set of notifications")
	fmt.Println("   customTypes := NewNotificationTypeSet(NotificationMoving, NotificationMigrated)")
	fmt.Println("   manager.RegisterSelectiveHandlers(customTypes)")
	fmt.Println()

	fmt.Println("3. Create hooks:")
	fmt.Println("   metricsHook := NewMetricsHook()")
	fmt.Println("   rewriteHook := NewEndpointRewriteHook(map[string]string{")
	fmt.Println("       \"old-redis:6379\": \"new-redis:6379\",")
	fmt.Println("   })")
	fmt.Println("   throttleHook := NewThrottleHook(10) // 10 notifications per second")
	fmt.Println("   validationHook := NewValidationHook(true) // strict mode")
	fmt.Println()

	fmt.Println("4. Add hooks to manager:")
	fmt.Println("   manager.AddHook(validationHook)  // Validate first")
	fmt.Println("   manager.AddHook(throttleHook)    // Then throttle")
	fmt.Println("   manager.AddHook(rewriteHook)     // Then rewrite")
	fmt.Println("   manager.AddHook(metricsHook)     // Finally collect metrics")
	fmt.Println()

	fmt.Println("5. Hooks will be called in order for each notification:")
	fmt.Println("   - ValidationHook validates the notification")
	fmt.Println("   - ThrottleHook may skip processing if rate limit exceeded")
	fmt.Println("   - EndpointRewriteHook may modify the endpoint")
	fmt.Println("   - MetricsHook collects processing statistics")
	fmt.Println()

	fmt.Println("6. Remove hooks when no longer needed:")
	fmt.Println("   manager.RemoveHook(throttleHook)")
}
