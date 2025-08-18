package hitless

import (
	"context"

	"github.com/redis/go-redis/v9/internal"
)

// LoggingHook is an example hook implementation that logs all notifications.
type LoggingHook struct {
	LogLevel int
}

// PreHook logs the notification before processing and allows modification.
func (lh *LoggingHook) PreHook(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool) {
	if lh.LogLevel >= 2 { // Info level
		internal.Logger.Printf(ctx, "hitless: processing %s notification: %v", notificationType, notification)
	}
	return notification, true // Continue processing with unmodified notification
}

// PostHook logs the result after processing.
func (lh *LoggingHook) PostHook(ctx context.Context, notificationType string, notification []interface{}, result error) {
	if result != nil && lh.LogLevel >= 1 { // Warning level
		internal.Logger.Printf(ctx, "hitless: %s notification processing failed: %v", notificationType, result)
	} else if lh.LogLevel >= 3 { // Debug level
		internal.Logger.Printf(ctx, "hitless: %s notification processed successfully", notificationType)
	}
}

// FilterHook is an example hook that can filter out certain notifications.
type FilterHook struct {
	BlockedTypes map[string]bool
}

// PreHook filters notifications based on type.
func (fh *FilterHook) PreHook(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool) {
	if fh.BlockedTypes[notificationType] {
		internal.Logger.Printf(ctx, "hitless: filtering out %s notification", notificationType)
		return notification, false // Skip processing
	}
	return notification, true
}

// PostHook does nothing for filter hook.
func (fh *FilterHook) PostHook(ctx context.Context, notificationType string, notification []interface{}, result error) {
	// No post-processing needed for filter hook
}
