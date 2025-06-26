package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("Redis Go Client - General Push Notification System Demo")
	fmt.Println("======================================================")

	// Example 1: Basic push notification setup
	basicPushNotificationExample()

	// Example 2: Custom push notification handlers
	customHandlersExample()

	// Example 3: Multiple specific handlers
	multipleSpecificHandlersExample()

	// Example 4: Custom push notifications
	customPushNotificationExample()

	// Example 5: Multiple notification types
	multipleNotificationTypesExample()

	// Example 6: Processor API demonstration
	demonstrateProcessorAPI()
}

func basicPushNotificationExample() {
	fmt.Println("\n=== Basic Push Notification Example ===")

	// Create a Redis client with push notifications enabled
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,    // RESP3 required for push notifications
		PushNotifications: true, // Enable general push notification processing
	})
	defer client.Close()

	// Register a handler for custom notifications
	client.RegisterPushNotificationHandlerFunc("CUSTOM_EVENT", func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("Received CUSTOM_EVENT: %v\n", notification)
		return true
	})

	fmt.Println("✅ Push notifications enabled and handler registered")
	fmt.Println("   The client will now process any CUSTOM_EVENT push notifications")
}

func customHandlersExample() {
	fmt.Println("\n=== Custom Push Notification Handlers Example ===")

	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	// Register handlers for different notification types
	client.RegisterPushNotificationHandlerFunc("USER_LOGIN", func(ctx context.Context, notification []interface{}) bool {
		if len(notification) >= 3 {
			username := notification[1]
			timestamp := notification[2]
			fmt.Printf("🔐 User login: %v at %v\n", username, timestamp)
		}
		return true
	})

	client.RegisterPushNotificationHandlerFunc("CACHE_INVALIDATION", func(ctx context.Context, notification []interface{}) bool {
		if len(notification) >= 2 {
			cacheKey := notification[1]
			fmt.Printf("🗑️  Cache invalidated: %v\n", cacheKey)
		}
		return true
	})

	client.RegisterPushNotificationHandlerFunc("SYSTEM_ALERT", func(ctx context.Context, notification []interface{}) bool {
		if len(notification) >= 3 {
			alertLevel := notification[1]
			message := notification[2]
			fmt.Printf("🚨 System alert [%v]: %v\n", alertLevel, message)
		}
		return true
	})

	fmt.Println("✅ Multiple custom handlers registered:")
	fmt.Println("   - USER_LOGIN: Handles user authentication events")
	fmt.Println("   - CACHE_INVALIDATION: Handles cache invalidation events")
	fmt.Println("   - SYSTEM_ALERT: Handles system alert notifications")
}

func multipleSpecificHandlersExample() {
	fmt.Println("\n=== Multiple Specific Handlers Example ===")

	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	// Register specific handlers
	client.RegisterPushNotificationHandlerFunc("SPECIFIC_EVENT", func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("🎯 Specific handler for SPECIFIC_EVENT: %v\n", notification)
		return true
	})

	client.RegisterPushNotificationHandlerFunc("ANOTHER_EVENT", func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("🎯 Specific handler for ANOTHER_EVENT: %v\n", notification)
		return true
	})

	fmt.Println("✅ Specific handlers registered:")
	fmt.Println("   - SPECIFIC_EVENT handler will receive only SPECIFIC_EVENT notifications")
	fmt.Println("   - ANOTHER_EVENT handler will receive only ANOTHER_EVENT notifications")
	fmt.Println("   - Each notification type has a single dedicated handler")
}

func customPushNotificationExample() {
	fmt.Println("\n=== Custom Push Notifications Example ===")

	// Create a client with custom push notifications
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,    // RESP3 required
		PushNotifications: true, // Enable general push notifications
	})
	defer client.Close()

	// Register custom handlers for application events
	client.RegisterPushNotificationHandlerFunc("APPLICATION_EVENT", func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("📱 Application event: %v\n", notification)
		return true
	})

	fmt.Println("✅ Custom push notifications enabled:")
	fmt.Println("   - APPLICATION_EVENT notifications → Custom handler")
	fmt.Println("   - Each notification type has a single dedicated handler")
}

func multipleNotificationTypesExample() {
	fmt.Println("\n=== Multiple Notification Types Example ===")

	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	// Register handlers for Redis built-in notification types
	client.RegisterPushNotificationHandlerFunc(redis.PushNotificationPubSubMessage, func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("💬 Pub/Sub message: %v\n", notification)
		return true
	})

	client.RegisterPushNotificationHandlerFunc(redis.PushNotificationKeyspace, func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("🔑 Keyspace notification: %v\n", notification)
		return true
	})

	client.RegisterPushNotificationHandlerFunc(redis.PushNotificationKeyevent, func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("⚡ Key event notification: %v\n", notification)
		return true
	})

	// Register handlers for cluster notifications
	client.RegisterPushNotificationHandlerFunc(redis.PushNotificationMoving, func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("🚚 Cluster MOVING notification: %v\n", notification)
		return true
	})

	// Register handlers for custom application notifications
	client.RegisterPushNotificationHandlerFunc("METRICS_UPDATE", func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("📊 Metrics update: %v\n", notification)
		return true
	})

	client.RegisterPushNotificationHandlerFunc("CONFIG_CHANGE", func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("⚙️  Configuration change: %v\n", notification)
		return true
	})

	fmt.Println("✅ Multiple notification type handlers registered:")
	fmt.Println("   Redis built-in notifications:")
	fmt.Printf("   - %s: Pub/Sub messages\n", redis.PushNotificationPubSubMessage)
	fmt.Printf("   - %s: Keyspace notifications\n", redis.PushNotificationKeyspace)
	fmt.Printf("   - %s: Key event notifications\n", redis.PushNotificationKeyevent)
	fmt.Println("   Cluster notifications:")
	fmt.Printf("   - %s: Cluster slot migration\n", redis.PushNotificationMoving)
	fmt.Println("   Custom application notifications:")
	fmt.Println("   - METRICS_UPDATE: Application metrics")
	fmt.Println("   - CONFIG_CHANGE: Configuration updates")
}

func demonstrateProcessorAPI() {
	fmt.Println("\n=== Push Notification Processor API Example ===")

	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	// Get the push notification processor
	processor := client.GetPushNotificationProcessor()
	if processor == nil {
		log.Println("Push notification processor not available")
		return
	}

	fmt.Printf("✅ Push notification processor status: enabled=%v\n", processor.IsEnabled())

	// Get the registry to inspect registered handlers
	registry := processor.GetRegistry()
	commands := registry.GetRegisteredCommands()
	fmt.Printf("📋 Registered commands: %v\n", commands)

	// Register a handler using the processor directly
	processor.RegisterHandlerFunc("DIRECT_REGISTRATION", func(ctx context.Context, notification []interface{}) bool {
		fmt.Printf("🎯 Direct registration handler: %v\n", notification)
		return true
	})

	// Check if handlers are registered
	if registry.HasHandlers() {
		fmt.Println("✅ Push notification handlers are registered and ready")
	}

	// Demonstrate notification info parsing
	sampleNotification := []interface{}{"SAMPLE_EVENT", "arg1", "arg2", 123}
	info := redis.ParsePushNotificationInfo(sampleNotification)
	if info != nil {
		fmt.Printf("📄 Notification info - Command: %s, Args: %d\n", info.Command, len(info.Args))
	}
}
