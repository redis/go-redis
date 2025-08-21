package hitless

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/interfaces"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// NotificationHandler handles push notifications for the simplified manager.
type NotificationHandler struct {
	manager *HitlessManager
}

// HandlePushNotification processes push notifications with hook support.
func (snh *NotificationHandler) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) == 0 {
		return ErrInvalidNotification
	}

	notificationType, ok := notification[0].(string)
	if !ok {
		return ErrInvalidNotification
	}

	// Process pre-hooks - they can modify the notification or skip processing
	modifiedNotification, shouldContinue := snh.manager.processPreHooks(ctx, notificationType, notification)
	if !shouldContinue {
		return nil // Hooks decided to skip processing
	}

	var err error
	switch notificationType {
	case NotificationMoving:
		err = snh.handleMoving(ctx, handlerCtx, modifiedNotification)
	case NotificationMigrating:
		err = snh.handleMigrating(ctx, handlerCtx, modifiedNotification)
	case NotificationMigrated:
		err = snh.handleMigrated(ctx, handlerCtx, modifiedNotification)
	case NotificationFailingOver:
		err = snh.handleFailingOver(ctx, handlerCtx, modifiedNotification)
	case NotificationFailedOver:
		err = snh.handleFailedOver(ctx, handlerCtx, modifiedNotification)
	default:
		// Ignore other notification types (e.g., pub/sub messages)
		err = nil
	}

	// Process post-hooks with the result
	snh.manager.processPostHooks(ctx, notificationType, modifiedNotification, err)

	return err
}

// handleMoving processes MOVING notifications.
// ["MOVING", seqNum, timeS, endpoint] - per-connection handoff
func (snh *NotificationHandler) handleMoving(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 3 {
		return ErrInvalidNotification
	}
	seqID, ok := notification[1].(int64)
	if !ok {
		return ErrInvalidNotification
	}

	// Extract timeS
	timeS, ok := notification[2].(int64)
	if !ok {
		return ErrInvalidNotification
	}

	newEndpoint := ""
	if len(notification) > 3 {
		// Extract new endpoint
		newEndpoint, ok = notification[3].(string)
		if !ok {
			return ErrInvalidNotification
		}
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

	deadline := time.Now().Add(time.Duration(timeS) * time.Second)
	// If newEndpoint is empty, we should schedule a handoff to the current endpoint in timeS/2 seconds
	if newEndpoint == "" || newEndpoint == internal.RedisNull {
		// same as current endpoint
		newEndpoint = snh.manager.options.GetAddr()
		// delay the handoff for timeS/2 seconds to the same endpoint
		// do this in a goroutine to avoid blocking the notification handler
		// NOTE: This timer is started while parsing the notification, so the connection is not marked for handoff
		// and there should be no possibility of a race condition or double handoff.
		time.AfterFunc(time.Duration(timeS/2)*time.Second, func() {
			if poolConn == nil || poolConn.IsClosed() {
				return
			}
			if err := snh.markConnForHandoff(poolConn, newEndpoint, seqID, deadline); err != nil {
				// Log error but don't fail the goroutine
				internal.Logger.Printf(ctx, "hitless: failed to mark connection for handoff: %v", err)
			}
		})
		return nil
	}

	return snh.markConnForHandoff(poolConn, newEndpoint, seqID, deadline)
}

func (snh *NotificationHandler) markConnForHandoff(conn *pool.Conn, newEndpoint string, seqID int64, deadline time.Time) error {
	if err := conn.MarkForHandoff(newEndpoint, seqID); err != nil {
		// Connection is already marked for handoff, which is acceptable
		// This can happen if multiple MOVING notifications are received for the same connection
		return nil
	}
	// Optionally track in hitless manager for monitoring/debugging
	if snh.manager != nil {
		connID := conn.GetID()

		// Track the operation (ignore errors since this is optional)
		_ = snh.manager.TrackMovingOperationWithConnID(context.Background(), newEndpoint, deadline, seqID, connID)
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
