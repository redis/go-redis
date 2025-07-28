package hitless

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9/internal/interfaces"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// NotificationHandler handles push notifications for the simplified manager.
type NotificationHandler struct {
	manager *HitlessManager
}

// HandlePushNotification processes push notifications.
func (snh *NotificationHandler) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) == 0 {
		return ErrInvalidNotification
	}

	notificationType, ok := notification[0].(string)
	if !ok {
		return ErrInvalidNotification
	}

	switch notificationType {
	case "MOVING":
		return snh.handleMoving(ctx, handlerCtx, notification)
	case "MIGRATING":
		return snh.handleMigrating(ctx, handlerCtx, notification)
	case "MIGRATED":
		return snh.handleMigrated(ctx, handlerCtx, notification)
	case "FAILING_OVER":
		return snh.handleFailingOver(ctx, handlerCtx, notification)
	case "FAILED_OVER":
		return snh.handleFailedOver(ctx, handlerCtx, notification)
	default:
		// Ignore other notification types (e.g., pub/sub messages)
		return nil
	}
}

// handleMoving processes MOVING notifications.
// ["MOVING", seqNum, timeS, endpoint] - per-connection handoff
func (snh *NotificationHandler) handleMoving(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	seqIDStr, ok := notification[1].(string)
	if !ok {
		return ErrInvalidNotification
	}

	seqID, err := strconv.ParseInt(seqIDStr, 10, 64)
	if err != nil {
		return ErrInvalidNotification
	}

	// Extract timeS
	timeSStr, ok := notification[2].(string)
	if !ok {
		return ErrInvalidNotification
	}

	timeS, err := strconv.ParseInt(timeSStr, 10, 64)
	if err != nil {
		return ErrInvalidNotification
	}

	// Extract new endpoint
	newEndpoint, ok := notification[3].(string)
	if !ok {
		return ErrInvalidNotification
	}

	// Get the connection that received this notification
	conn := handlerCtx.Conn
	if conn == nil {
		return ErrInvalidNotification
	}

	// Type assert to get the underlying pool connection
	var poolConn *pool.Conn
	if connAdapter, ok := conn.(interface{ GetPoolConn() *pool.Conn }); ok {
		poolConn = connAdapter.GetPoolConn()
	} else if pc, ok := conn.(*pool.Conn); ok {
		poolConn = pc
	} else {
		return ErrInvalidNotification
	}

	// Mark the connection for handoff
	if err := poolConn.MarkForHandoff(newEndpoint, seqID); err != nil {
		// Connection is already marked for handoff, which is acceptable
		// This can happen if multiple MOVING notifications are received for the same connection
		return nil
	}

	// Optionally track in hitless manager for monitoring/debugging
	if snh.manager != nil {
		connID := poolConn.GetID()
		// Use a default deadline since Approach #3 doesn't provide timing info
		deadline := time.Now().Add(time.Duration(timeS) * time.Second)

		// Track the operation (ignore errors since this is optional)
		_ = snh.manager.StartMovingOperationWithConnID(context.Background(), newEndpoint, deadline, seqID, connID)
	} else {
		return fmt.Errorf("hitless: manager not initialized")
	}

	return nil
}

// handleMigrating processes MIGRATING notifications.
func (snh *NotificationHandler) handleMigrating(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	// MIGRATING notifications indicate that a connection is about to be migrated
	// Apply relaxed timeouts to the specific connection that received this notification
	if len(notification) < 2 {
		return ErrInvalidNotification
	}

	// Get the connection from handler context and type assert to connectionAdapter
	if handlerCtx.Conn == nil {
		return ErrInvalidNotification
	}

	// Type assert to connectionAdapter which implements ConnectionWithRelaxedTimeout
	connAdapter, ok := handlerCtx.Conn.(interfaces.ConnectionWithRelaxedTimeout)
	if !ok {
		return ErrInvalidNotification
	}

	// Apply relaxed timeout to this specific connection
	connAdapter.SetRelaxedTimeout(snh.manager.config.RelaxedTimeout, snh.manager.config.RelaxedTimeout)
	return nil
}

// handleMigrated processes MIGRATED notifications.
func (snh *NotificationHandler) handleMigrated(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	// MIGRATED notifications indicate that a connection migration has completed
	// Restore normal timeouts for the specific connection that received this notification
	if len(notification) < 2 {
		return ErrInvalidNotification
	}

	// Get the connection from handler context and type assert to connectionAdapter
	if handlerCtx.Conn == nil {
		return ErrInvalidNotification
	}

	// Type assert to connectionAdapter which implements ConnectionWithRelaxedTimeout
	connAdapter, ok := handlerCtx.Conn.(interfaces.ConnectionWithRelaxedTimeout)
	if !ok {
		return ErrInvalidNotification
	}

	// Clear relaxed timeout for this specific connection
	connAdapter.ClearRelaxedTimeout()
	return nil
}

// handleFailingOver processes FAILING_OVER notifications.
func (snh *NotificationHandler) handleFailingOver(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	// FAILING_OVER notifications indicate that a connection is about to failover
	// Apply relaxed timeouts to the specific connection that received this notification
	if len(notification) < 2 {
		return ErrInvalidNotification
	}

	// Get the connection from handler context and type assert to connectionAdapter
	if handlerCtx.Conn == nil {
		return ErrInvalidNotification
	}

	// Type assert to connectionAdapter which implements ConnectionWithRelaxedTimeout
	connAdapter, ok := handlerCtx.Conn.(interfaces.ConnectionWithRelaxedTimeout)
	if !ok {
		return ErrInvalidNotification
	}

	// Apply relaxed timeout to this specific connection
	connAdapter.SetRelaxedTimeout(snh.manager.config.RelaxedTimeout, snh.manager.config.RelaxedTimeout)
	return nil
}

// handleFailedOver processes FAILED_OVER notifications.
func (snh *NotificationHandler) handleFailedOver(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	// FAILED_OVER notifications indicate that a connection failover has completed
	// Restore normal timeouts for the specific connection that received this notification
	if len(notification) < 2 {
		return ErrInvalidNotification
	}

	// Get the connection from handler context and type assert to connectionAdapter
	if handlerCtx.Conn == nil {
		return ErrInvalidNotification
	}

	// Type assert to connectionAdapter which implements ConnectionWithRelaxedTimeout
	connAdapter, ok := handlerCtx.Conn.(interfaces.ConnectionWithRelaxedTimeout)
	if !ok {
		return ErrInvalidNotification
	}

	// Clear relaxed timeout for this specific connection
	connAdapter.ClearRelaxedTimeout()
	return nil
}
