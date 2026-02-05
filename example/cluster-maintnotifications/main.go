package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

func main() {
	// Parse cluster addresses from environment or use defaults
	addrs := []string{"localhost:7000", "localhost:7001", "localhost:7002"}
	if envAddrs := os.Getenv("REDIS_CLUSTER_ADDRS"); envAddrs != "" {
		addrs = []string{envAddrs}
	}

	// Track cluster state reloads
	var reloadCount atomic.Int32

	// Create cluster client with maintnotifications enabled
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Protocol: 3, // RESP3 required for push notifications

		// Enable maintnotifications for seamless slot migration handling
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:           maintnotifications.ModeEnabled,
			RelaxedTimeout: 30 * time.Second, // Relax timeouts during migration
		},

		// Optional: Configure timeouts
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer client.Close()

	// Set up custom hook to track cluster state reloads
	// This demonstrates how to monitor SMIGRATED notifications
	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			// Add a hook to track notifications
			hook := &notificationTracker{
				onSMigrated: func(seqID int64, endpoints []string) {
					count := reloadCount.Add(1)
					log.Printf("SMIGRATED notification received (reload #%d): SeqID=%d, Endpoints=%v",
						count, seqID, endpoints)
				},
				onSMigrating: func(seqID int64, slots []string) {
					log.Printf("SMIGRATING notification received: SeqID=%d, Slots=%v", seqID, slots)
				},
			}
			manager.AddNotificationHook(hook)
		}
	})

	ctx := context.Background()

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis cluster: %v", err)
	}

	log.Println("Connected to Redis cluster with maintnotifications enabled")
	log.Println("The client will automatically handle SMIGRATING and SMIGRATED notifications")
	log.Println("Press Ctrl+C to exit")

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Run operations in background
	go runOperations(ctx, client)

	// Wait for shutdown signal
	<-sigChan
	log.Println("\nShutting down...")
	log.Printf("Total cluster state reloads: %d", reloadCount.Load())
}

// runOperations performs continuous Redis operations
func runOperations(ctx context.Context, client *redis.ClusterClient) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	counter := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			counter++
			key := fmt.Sprintf("test-key-%d", counter)
			value := fmt.Sprintf("value-%d", counter)

			// Perform SET operation
			if err := client.Set(ctx, key, value, 10*time.Second).Err(); err != nil {
				log.Printf("SET error: %v", err)
				continue
			}

			// Perform GET operation
			val, err := client.Get(ctx, key).Result()
			if err != nil {
				log.Printf("GET error: %v", err)
				continue
			}

			if val != value {
				log.Printf("Value mismatch: expected %s, got %s", value, val)
			}

			// Log progress every 10 operations
			if counter%10 == 0 {
				log.Printf("Completed %d operations", counter)
			}
		}
	}
}

// notificationTracker is a custom hook to track maintnotifications
type notificationTracker struct {
	onSMigrated  func(seqID int64, endpoints []string)
	onSMigrating func(seqID int64, slots []string)
}

// PreHook implements the NotificationHook interface
func (nt *notificationTracker) PreHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}) ([]interface{}, bool) {
	// Process the notification before the manager handles it
	switch notificationType {
	case maintnotifications.NotificationSMigrated:
		// New format: ["SMIGRATED", SeqID, count, [endpoint1, endpoint2, ...]]
		// Each endpoint is "host:port slot1,slot2,range1-range2"
		if len(notification) == 4 {
			seqID, _ := notification[1].(int64)

			// Extract endpoints array
			endpoints := make([]string, 0)
			if endpointsArray, ok := notification[3].([]interface{}); ok {
				for _, ep := range endpointsArray {
					if endpoint, ok := ep.(string); ok {
						endpoints = append(endpoints, endpoint)
					}
				}
			}

			if nt.onSMigrated != nil {
				nt.onSMigrated(seqID, endpoints)
			}
		}

	case maintnotifications.NotificationSMigrating:
		if len(notification) >= 3 {
			seqID, _ := notification[1].(int64)

			// Extract slot ranges
			slots := make([]string, 0, len(notification)-2)
			for i := 2; i < len(notification); i++ {
				if slot, ok := notification[i].(string); ok {
					slots = append(slots, slot)
				}
			}

			if nt.onSMigrating != nil {
				nt.onSMigrating(seqID, slots)
			}
		}
	}

	// Return the notification unmodified and continue processing
	return notification, true
}

// PostHook implements the NotificationHook interface
func (nt *notificationTracker) PostHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}, result error) {
	// Called after notification processing completes
	// Can be used to log errors or track completion
	if result != nil {
		log.Printf("Notification processing error for %s: %v", notificationType, result)
	}
}

