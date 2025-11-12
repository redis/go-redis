package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func main() {
	ctx := context.Background()

	// Example 0: Explicitly disable maintenance notifications
	fmt.Println("=== Example 0: Explicitly Enabled ===")
	rdb0 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",

		// Explicitly disable maintenance notifications
		// This prevents the client from sending CLIENT MAINT_NOTIFICATIONS ON
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	})
	defer rdb0.Close()

	// Test the connection
	if err := rdb0.Ping(ctx).Err(); err != nil {
		fmt.Printf("Failed to connect: %v\n\n", err)
	}
	fmt.Println("When ModeEnabled, the client will return an error if the server doesn't support maintenance notifications.")
	fmt.Printf("ModeAuto will silently disable the feature.\n\n")

	// Example 1: Explicitly disable maintenance notifications
	fmt.Println("=== Example 1: Explicitly Disabled ===")
	rdb1 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",

		// Explicitly disable maintenance notifications
		// This prevents the client from sending CLIENT MAINT_NOTIFICATIONS ON
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer rdb1.Close()

	// Test the connection
	if err := rdb1.Ping(ctx).Err(); err != nil {
		fmt.Printf("Failed to connect: %v\n\n", err)
		return
	}
	fmt.Println("✓ Connected successfully (maintenance notifications disabled)")

	// Perform some operations
	if err := rdb1.Set(ctx, "example:key1", "value1", 0).Err(); err != nil {
		fmt.Printf("Failed to set key: %v\n\n", err)
		return
	}
	fmt.Println("✓ SET operation successful")

	val, err := rdb1.Get(ctx, "example:key1").Result()
	if err != nil {
		fmt.Printf("Failed to get key: %v\n\n", err)
		return
	}
	fmt.Printf("✓ GET operation successful: %s\n\n", val)

	// Example 2: Using nil config (defaults to ModeAuto)
	fmt.Printf("\n=== Example 2: Default Behavior (ModeAuto) ===\n")
	rdb2 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		// MaintNotifications: nil means ModeAuto (enabled for Redis Cloud)
	})
	defer rdb2.Close()

	if err := rdb2.Ping(ctx).Err(); err != nil {
		fmt.Printf("Failed to connect: %v\n\n", err)
		return
	}
	fmt.Println("✓ Connected successfully (maintenance notifications auto-enabled)")

	// Example 4: Comparing behavior with and without maintenance notifications
	fmt.Printf("\n=== Example 4: Performance Comparison ===\n")

	// Client with auto-enabled notifications
	startauto := time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test:auto:%d", i)
		if err := rdb2.Set(ctx, key, i, time.Minute).Err(); err != nil {
			fmt.Printf("Failed to set key: %v\n", err)
			return
		}
	}
	autoDuration := time.Since(startauto)
	fmt.Printf("✓ 1000 SET operations (auto): %v\n", autoDuration)

	// print pool stats
	fmt.Printf("Pool stats (auto): %+v\n", rdb2.PoolStats())

	// give the server a moment to take chill
	fmt.Println("---")
	time.Sleep(time.Second)

	// Client with disabled notifications
	start := time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test:disabled:%d", i)
		if err := rdb1.Set(ctx, key, i, time.Minute).Err(); err != nil {
			fmt.Printf("Failed to set key: %v\n", err)
			return
		}
	}
	disabledDuration := time.Since(start)
	fmt.Printf("✓ 1000 SET operations (disabled): %v\n", disabledDuration)
	fmt.Printf("Pool stats (disabled): %+v\n", rdb1.PoolStats())

	// performance comparison note
	fmt.Printf("\nNote: The pool stats and performance are identical because there is no background processing overhead.\n")
	fmt.Println("Since the server doesn't support maintenance notifications, there is no difference in behavior.")
	fmt.Printf("The only difference is that the \"ModeDisabled\" client doesn't send the CLIENT MAINT_NOTIFICATIONS ON command.\n\n")
	fmt.Println("p.s. reordering the execution here makes it look like there is a small performance difference, but it's just noise.")

	// Cleanup
	fmt.Printf("\n=== Cleanup ===\n")
	if err := rdb1.FlushDB(ctx).Err(); err != nil {
		fmt.Printf("Failed to flush DB: %v\n", err)
		return
	}
	fmt.Println("✓ Database flushed")

	fmt.Printf("\n=== Summary ===\n")
	fmt.Println("Maintenance notifications can be disabled by setting:")
	fmt.Println("  MaintNotifications: &maintnotifications.Config{")
	fmt.Println("    Mode: maintnotifications.ModeDisabled,")
	fmt.Println("  }")
	fmt.Printf("\nThis is useful when:\n")
	fmt.Println("  - Connecting to non-Redis Cloud instances")
	fmt.Println("  - You want to handle failovers manually")
	fmt.Println("  - You want to minimize client-side overhead")
	fmt.Println("  - The Redis server doesn't support CLIENT MAINT_NOTIFICATIONS")
	fmt.Printf("\nFor more information, see:\n")
	fmt.Println("  https://github.com/redis/go-redis/tree/master/maintnotifications")
}
