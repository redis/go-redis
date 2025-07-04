package redis

import (
	"github.com/redis/go-redis/v9/push"
)

// Push notification constants for cluster operations
const (
	// MOVING indicates a slot is being moved to a different node
	PushNotificationMoving = "MOVING"

	// MIGRATING indicates a slot is being migrated from this node
	PushNotificationMigrating = "MIGRATING"

	// MIGRATED indicates a slot has been migrated to this node
	PushNotificationMigrated = "MIGRATED"

	// FAILING_OVER indicates a failover is starting
	PushNotificationFailingOver = "FAILING_OVER"

	// FAILED_OVER indicates a failover has completed
	PushNotificationFailedOver = "FAILED_OVER"
)

// NewPushNotificationProcessor creates a new push notification processor
// This processor maintains a registry of handlers and processes push notifications
// It is used for RESP3 connections where push notifications are available
func NewPushNotificationProcessor() push.NotificationProcessor {
	return push.NewProcessor()
}

// NewVoidPushNotificationProcessor creates a new void push notification processor
// This processor does not maintain any handlers and always returns nil for all operations
// It is used for RESP2 connections where push notifications are not available
// It can also be used to disable push notifications for RESP3 connections, where
// it will discard all push notifications without processing them
func NewVoidPushNotificationProcessor() push.NotificationProcessor {
	return push.NewVoidProcessor()
}
