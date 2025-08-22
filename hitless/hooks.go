package hitless

import (
	"context"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// LoggingHook is an example hook implementation that logs all notifications.
type LoggingHook struct {
	LogLevel int
}

// PreHook logs the notification before processing and allows modification.
func (lh *LoggingHook) PreHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}) ([]interface{}, bool) {
	if lh.LogLevel >= 2 { // Info level
		// Log the notification type and content
		connID := uint64(0)
		if conn, ok := notificationCtx.Conn.(*pool.Conn); ok {
			connID = conn.GetID()
		}
		internal.Logger.Printf(ctx, "hitless: conn[%d] processing %s notification: %v", connID, notificationType, notification)
	}
	return notification, true // Continue processing with unmodified notification
}

// PostHook logs the result after processing.
func (lh *LoggingHook) PostHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}, result error) {
	connID := uint64(0)
	if conn, ok := notificationCtx.Conn.(*pool.Conn); ok {
		connID = conn.GetID()
	}
	if result != nil && lh.LogLevel >= 1 { // Warning level
		internal.Logger.Printf(ctx, "hitless: conn[%d] %s notification processing failed: %v - %v", connID, notificationType, result, notification)
	} else if lh.LogLevel >= 3 { // Debug level
		internal.Logger.Printf(ctx, "hitless: conn[%d] %s notification processed successfully", connID, notificationType)
	}
}

// NewLoggingHook creates a new logging hook with the specified log level.
// Log levels: 0=errors, 1=warnings, 2=info, 3=debug
func NewLoggingHook(logLevel int) *LoggingHook {
	return &LoggingHook{LogLevel: logLevel}
}
