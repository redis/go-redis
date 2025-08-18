package hitless

import (
	"context"
	"time"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	startTimeKey contextKey = "notif_hitless_start_time"
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
	_ = context.WithValue(ctx, startTimeKey, startTime) // Context not used further

	return notification, true
}

// PostHook records processing completion and any errors.
func (mh *MetricsHook) PostHook(ctx context.Context, notificationType string, notification []interface{}, result error) {
	// Calculate processing duration
	if startTime, ok := ctx.Value(startTimeKey).(time.Time); ok {
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
