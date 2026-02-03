package maintnotifications

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/maintnotifications/logs"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// NotificationHandler handles push notifications for the simplified manager.
type NotificationHandler struct {
	manager           *Manager
	operationsManager OperationsManagerInterface
}

// HandlePushNotification processes push notifications with hook support.
func (snh *NotificationHandler) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) == 0 {
		internal.Logger.Printf(ctx, logs.InvalidNotificationFormat(notification))
		return ErrInvalidNotification
	}

	notificationType, ok := notification[0].(string)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidNotificationTypeFormat(notification[0]))
		return ErrInvalidNotification
	}

	// Process pre-hooks - they can modify the notification or skip processing
	modifiedNotification, shouldContinue := snh.manager.processPreHooks(ctx, handlerCtx, notificationType, notification)
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
	case NotificationSMigrating:
		err = snh.handleSMigrating(ctx, handlerCtx, modifiedNotification)
	case NotificationSMigrated:
		err = snh.handleSMigrated(ctx, handlerCtx, modifiedNotification)
	default:
		// Ignore other notification types (e.g., pub/sub messages)
		err = nil
	}

	// Process post-hooks with the result
	snh.manager.processPostHooks(ctx, handlerCtx, notificationType, modifiedNotification, err)

	return err
}

// handleMoving processes MOVING notifications.
// MOVING indicates that a connection should be handed off to a new endpoint.
// This is a per-connection notification that triggers connection handoff.
// Expected format: ["MOVING", seqNum, timeS, endpoint]
func (snh *NotificationHandler) handleMoving(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 3 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("MOVING", notification))
		return ErrInvalidNotification
	}
	seqID, ok := notification[1].(int64)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidSeqIDInMovingNotification(notification[1]))
		return ErrInvalidNotification
	}

	// Extract timeS
	timeS, ok := notification[2].(int64)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidTimeSInMovingNotification(notification[2]))
		return ErrInvalidNotification
	}

	newEndpoint := ""
	if len(notification) > 3 {
		// Extract new endpoint
		newEndpoint, ok = notification[3].(string)
		if !ok {
			stringified := fmt.Sprintf("%v", notification[3])
			// this could be <nil> which is valid
			if notification[3] == nil || stringified == internal.RedisNull {
				newEndpoint = ""
			} else {
				internal.Logger.Printf(ctx, logs.InvalidNewEndpointInMovingNotification(notification[3]))
				return ErrInvalidNotification
			}
		}
	}

	// Get the connection that received this notification
	conn := handlerCtx.Conn
	if conn == nil {
		internal.Logger.Printf(ctx, logs.NoConnectionInHandlerContext("MOVING"))
		return ErrInvalidNotification
	}

	// Type assert to get the underlying pool connection
	var poolConn *pool.Conn
	if pc, ok := conn.(*pool.Conn); ok {
		poolConn = pc
	} else {
		internal.Logger.Printf(ctx, logs.InvalidConnectionTypeInHandlerContext("MOVING", conn, handlerCtx))
		return ErrInvalidNotification
	}

	// If the connection is closed or not pooled, we can ignore the notification
	// this connection won't be remembered by the pool and will be garbage collected
	// Keep pubsub connections around since they are not pooled but are long-lived
	// and should be allowed to handoff (the pubsub instance will reconnect and change
	// the underlying *pool.Conn)
	if (poolConn.IsClosed() || !poolConn.IsPooled()) && !poolConn.IsPubSub() {
		return nil
	}

	deadline := time.Now().Add(time.Duration(timeS) * time.Second)
	// If newEndpoint is empty, we should schedule a handoff to the current endpoint in timeS/2 seconds
	if newEndpoint == "" || newEndpoint == internal.RedisNull {
		if internal.LogLevel.DebugOrAbove() {
			internal.Logger.Printf(ctx, logs.SchedulingHandoffToCurrentEndpoint(poolConn.GetID(), float64(timeS)/2))
		}
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
				// Log error but don't fail the goroutine - use background context since original may be cancelled
				internal.Logger.Printf(context.Background(), logs.FailedToMarkForHandoff(poolConn.GetID(), err))
				return
			}

			// Queue the handoff immediately if the connection is idle in the pool.
			// If the connection is in use (StateInUse), it will be queued when returned to the pool via OnPut.
			// This handles the case where the connection is idle and might never be retrieved again.
			if poolConn.GetStateMachine().GetState() == pool.StateIdle {
				if snh.manager.poolHooksRef != nil && snh.manager.poolHooksRef.workerManager != nil {
					if err := snh.manager.poolHooksRef.workerManager.queueHandoff(poolConn); err != nil {
						internal.Logger.Printf(context.Background(), logs.FailedToQueueHandoff(poolConn.GetID(), err))
					} else {
						// Mark the connection as queued for handoff to prevent it from being retrieved
						// This transitions the connection to StateUnusable
						if err := poolConn.MarkQueuedForHandoff(); err != nil {
							internal.Logger.Printf(context.Background(), logs.FailedToMarkForHandoff(poolConn.GetID(), err))
						} else {
							internal.Logger.Printf(context.Background(), logs.MarkedForHandoff(poolConn.GetID()))
						}
					}
				}
			}
			// If connection is StateInUse, the handoff will be queued when it's returned to the pool
		})
		return nil
	}

	return snh.markConnForHandoff(poolConn, newEndpoint, seqID, deadline)
}

func (snh *NotificationHandler) markConnForHandoff(conn *pool.Conn, newEndpoint string, seqID int64, deadline time.Time) error {
	if err := conn.MarkForHandoff(newEndpoint, seqID); err != nil {
		internal.Logger.Printf(context.Background(), logs.FailedToMarkForHandoff(conn.GetID(), err))
		// Connection is already marked for handoff, which is acceptable
		// This can happen if multiple MOVING notifications are received for the same connection
		return nil
	}
	// Optionally track in m
	if snh.operationsManager != nil {
		connID := conn.GetID()
		// Track the operation (ignore errors since this is optional)
		_ = snh.operationsManager.TrackMovingOperationWithConnID(context.Background(), newEndpoint, deadline, seqID, connID)
	} else {
		return errors.New(logs.ManagerNotInitialized())
	}
	return nil
}

// handleMigrating processes MIGRATING notifications.
// MIGRATING indicates that a connection migration is starting.
// This is a per-connection notification that applies relaxed timeouts.
// Expected format: ["MIGRATING", ...]
func (snh *NotificationHandler) handleMigrating(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 2 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("MIGRATING", notification))
		return ErrInvalidNotification
	}

	if handlerCtx.Conn == nil {
		internal.Logger.Printf(ctx, logs.NoConnectionInHandlerContext("MIGRATING"))
		return ErrInvalidNotification
	}

	conn, ok := handlerCtx.Conn.(*pool.Conn)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidConnectionTypeInHandlerContext("MIGRATING", handlerCtx.Conn, handlerCtx))
		return ErrInvalidNotification
	}

	// Apply relaxed timeout to this specific connection
	if internal.LogLevel.InfoOrAbove() {
		internal.Logger.Printf(ctx, logs.RelaxedTimeoutDueToNotification(conn.GetID(), "MIGRATING", snh.manager.config.RelaxedTimeout))
	}
	conn.SetRelaxedTimeout(snh.manager.config.RelaxedTimeout, snh.manager.config.RelaxedTimeout)
	return nil
}

// handleMigrated processes MIGRATED notifications.
// MIGRATED indicates that a connection migration has completed.
// This is a per-connection notification that clears relaxed timeouts.
// Expected format: ["MIGRATED", ...]
func (snh *NotificationHandler) handleMigrated(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 2 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("MIGRATED", notification))
		return ErrInvalidNotification
	}

	if handlerCtx.Conn == nil {
		internal.Logger.Printf(ctx, logs.NoConnectionInHandlerContext("MIGRATED"))
		return ErrInvalidNotification
	}

	conn, ok := handlerCtx.Conn.(*pool.Conn)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidConnectionTypeInHandlerContext("MIGRATED", handlerCtx.Conn, handlerCtx))
		return ErrInvalidNotification
	}

	// Clear relaxed timeout for this specific connection
	if internal.LogLevel.InfoOrAbove() {
		connID := conn.GetID()
		internal.Logger.Printf(ctx, logs.UnrelaxedTimeout(connID))
	}
	conn.ClearRelaxedTimeout()
	return nil
}

// handleFailingOver processes FAILING_OVER notifications.
// FAILING_OVER indicates that a failover is starting.
// This is a per-connection notification that applies relaxed timeouts.
// Expected format: ["FAILING_OVER", ...]
func (snh *NotificationHandler) handleFailingOver(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 2 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("FAILING_OVER", notification))
		return ErrInvalidNotification
	}

	if handlerCtx.Conn == nil {
		internal.Logger.Printf(ctx, logs.NoConnectionInHandlerContext("FAILING_OVER"))
		return ErrInvalidNotification
	}

	conn, ok := handlerCtx.Conn.(*pool.Conn)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidConnectionTypeInHandlerContext("FAILING_OVER", handlerCtx.Conn, handlerCtx))
		return ErrInvalidNotification
	}

	// Apply relaxed timeout to this specific connection
	if internal.LogLevel.InfoOrAbove() {
		connID := conn.GetID()
		internal.Logger.Printf(ctx, logs.RelaxedTimeoutDueToNotification(connID, "FAILING_OVER", snh.manager.config.RelaxedTimeout))
	}
	conn.SetRelaxedTimeout(snh.manager.config.RelaxedTimeout, snh.manager.config.RelaxedTimeout)
	return nil
}

// handleFailedOver processes FAILED_OVER notifications.
// FAILED_OVER indicates that a failover has completed.
// This is a per-connection notification that clears relaxed timeouts.
// Expected format: ["FAILED_OVER", ...]
func (snh *NotificationHandler) handleFailedOver(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 2 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("FAILED_OVER", notification))
		return ErrInvalidNotification
	}

	if handlerCtx.Conn == nil {
		internal.Logger.Printf(ctx, logs.NoConnectionInHandlerContext("FAILED_OVER"))
		return ErrInvalidNotification
	}

	conn, ok := handlerCtx.Conn.(*pool.Conn)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidConnectionTypeInHandlerContext("FAILED_OVER", handlerCtx.Conn, handlerCtx))
		return ErrInvalidNotification
	}

	// Clear relaxed timeout for this specific connection
	if internal.LogLevel.InfoOrAbove() {
		connID := conn.GetID()
		internal.Logger.Printf(ctx, logs.UnrelaxedTimeout(connID))
	}
	conn.ClearRelaxedTimeout()
	return nil
}

// handleSMigrating processes SMIGRATING notifications.
// SMIGRATING indicates that a cluster slot is in the process of migrating to a different node.
// This is a per-connection notification that applies relaxed timeouts during slot migration.
// Expected format: ["SMIGRATING", SeqID, slot/range1-range2, ...]
func (snh *NotificationHandler) handleSMigrating(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 3 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATING", notification))
		return ErrInvalidNotification
	}

	// Extract SeqID (position 1)
	seqID, ok := notification[1].(int64)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidSeqIDInSMigratingNotification(notification[1]))
		return ErrInvalidNotification
	}

	// Extract slot ranges (position 2+)
	// For now, we just extract them for logging
	// Format can be: single slot "1234" or range "100-200"
	var slotRanges []string
	for i := 2; i < len(notification); i++ {
		if slotRange, ok := notification[i].(string); ok {
			slotRanges = append(slotRanges, slotRange)
		}
	}

	if handlerCtx.Conn == nil {
		internal.Logger.Printf(ctx, logs.NoConnectionInHandlerContext("SMIGRATING"))
		return ErrInvalidNotification
	}

	conn, ok := handlerCtx.Conn.(*pool.Conn)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidConnectionTypeInHandlerContext("SMIGRATING", handlerCtx.Conn, handlerCtx))
		return ErrInvalidNotification
	}

	// Apply relaxed timeout to this specific connection
	if internal.LogLevel.InfoOrAbove() {
		internal.Logger.Printf(ctx, logs.SlotMigrating(conn.GetID(), seqID, slotRanges))
	}
	conn.SetRelaxedTimeout(snh.manager.config.RelaxedTimeout, snh.manager.config.RelaxedTimeout)
	return nil
}

// handleSMigrated processes SMIGRATED notifications.
// SMIGRATED indicates that a cluster slot has finished migrating to a different node.
// This is a cluster-level notification that triggers cluster state reload.
//
// Expected RESP3 format:
//
//	>3
//	+SMIGRATED
//	:SeqID
//	*<num_entries>       <- array of triplet arrays
//	  *3                 <- each triplet is a 3-element array
//	    +<source>        <- node from which slots are migrating FROM
//	    +<destination>   <- node to which slots are migrating TO
//	    +<slots>         <- comma-separated slots and/or ranges (e.g., "123,789-1000")
//
// A source and target endpoint may appear in multiple triplets.
// The notification is only processed if the connection's OriginalEndpoint matches one of the source endpoints.
//
// Note: Multiple connections may receive the same notification, so we deduplicate by SeqID before triggering reload.
// but we still process the notification on each connection to clear the relaxed timeout.
// In the case when the connection is from MOVED/ASK, the connection's original endpoint is not set,
// so we will not be able to match the source endpoint. In such case, we will trigger the reload callback with the first target endpoint.
func (snh *NotificationHandler) handleSMigrated(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	// Expected: ["SMIGRATED", SeqID, [[source, target, slots], ...]]
	// Minimum 3 elements: SMIGRATED, SeqID, and the array of triplets
	if len(notification) < 3 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATED", notification))
		return ErrInvalidNotification
	}

	// Extract SeqID (position 1)
	seqID, ok := notification[1].(int64)
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidSeqIDInSMigratedNotification(notification[1]))
		return ErrInvalidNotification
	}

	// Extract the array of triplets (position 2)
	triplets, ok := notification[2].([]interface{})
	if !ok {
		internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATED (triplets array)", notification[2]))
		return ErrInvalidNotification
	}

	if len(triplets) == 0 {
		internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATED (empty triplets)", notification))
		return ErrInvalidNotification
	}

	// Get the connection's original endpoint to check if this notification is relevant
	var connectionOriginalEndpoint string
	if snh.manager.options != nil {
		connectionOriginalEndpoint = snh.manager.options.GetOriginalEndpoint()
	}

	// Parse triplets and check if any source matches our connection's original endpoint
	var matchingTriplets []struct {
		source string
		target string
		slots  string
	}
	var allSlotRanges []string

	if connectionOriginalEndpoint != "" {
		for _, tripletInterface := range triplets {
			// Each triplet should be a 3-element array: [source, target, slots]
			triplet, ok := tripletInterface.([]interface{})
			if !ok || len(triplet) != 3 {
				internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATED (triplet format)", tripletInterface))
				continue
			}

			// Extract source endpoint
			source, ok := triplet[0].(string)
			if !ok {
				internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATED (source)", triplet[0]))
				continue
			}

			// Extract target endpoint
			target, ok := triplet[1].(string)
			if !ok {
				internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATED (target)", triplet[1]))
				continue
			}

			// Extract slots
			slots, ok := triplet[2].(string)
			if !ok {
				internal.Logger.Printf(ctx, logs.InvalidNotification("SMIGRATED (slots)", triplet[2]))
				continue
			}

			// Check if this triplet's source matches our connection's original endpoint
			if connectionOriginalEndpoint != "" && source == connectionOriginalEndpoint {
				matchingTriplets = append(matchingTriplets, struct {
					source string
					target string
					slots  string
				}{source, target, slots})
				slotRanges := strings.Split(slots, ",")
				allSlotRanges = append(allSlotRanges, slotRanges...)
			}
		}
	}

	// Only process if we found matching triplets (source matches our endpoint)
	if len(matchingTriplets) == 0 && connectionOriginalEndpoint != "" {
		// No matching source - this notification is not for us
		// Still clear relaxed timeout if we have a connection
		if handlerCtx.Conn != nil {
			conn, ok := handlerCtx.Conn.(*pool.Conn)
			if ok {
				conn.ClearRelaxedTimeout()
			}
		}
		return nil
	}

	// Deduplicate by SeqID - multiple connections may receive the same notification
	if snh.manager.MarkSMigratedSeqIDProcessed(seqID) {
		if internal.LogLevel.InfoOrAbove() {
			// Log with the first matching target for context
			internal.Logger.Printf(ctx, logs.SlotMigrated(seqID, matchingTriplets[0].target, allSlotRanges))
		}
		// if we have a matching triplet, use it, otherwise use the first triplet
		var target string
		target = matchingTriplets[0].target
		if target == "" {
			target = triplets[0].([]interface{})[1].(string)
		}
		// Trigger cluster state reload via callback
		// Pass the first matching target and all slot ranges from matching triplets

		snh.manager.TriggerClusterStateReload(ctx, target, allSlotRanges)
	}

	// clear relaxed timeout
	if handlerCtx.Conn != nil {
		conn, ok := handlerCtx.Conn.(*pool.Conn)
		if ok {
			conn.ClearRelaxedTimeout()
		}
	}

	return nil
}
