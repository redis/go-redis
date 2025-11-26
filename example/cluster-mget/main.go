package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	// Create a cluster client
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"localhost:16600",
			"localhost:16601",
			"localhost:16602",
			"localhost:16603",
			"localhost:16604",
			"localhost:16605",
		},
	})
	defer rdb.Close()

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("Failed to connect to Redis cluster: %v", err))
	}

	fmt.Println("✓ Connected to Redis cluster")

	// Define 10 keys and values
	keys := make([]string, 10)
	values := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
		values[i] = fmt.Sprintf("value%d", i)
	}

	// Set all 10 keys
	fmt.Println("\n=== Setting 10 keys ===")
	for i := 0; i < 10; i++ {
		err := rdb.Set(ctx, keys[i], values[i], 0).Err()
		if err != nil {
			panic(fmt.Sprintf("Failed to set %s: %v", keys[i], err))
		}
		fmt.Printf("✓ SET %s = %s\n", keys[i], values[i])
	}

	/*
	// Retrieve all keys using MGET
	fmt.Println("\n=== Retrieving keys with MGET ===")
	result, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to execute MGET: %v", err))
	}
	*/

	/*
	// Validate the results
	fmt.Println("\n=== Validating MGET results ===")
	allValid := true
	for i, val := range result {
		expectedValue := values[i]
		actualValue, ok := val.(string)

		if !ok {
			fmt.Printf("✗ %s: expected string, got %T\n", keys[i], val)
			allValid = false
			continue
		}

		if actualValue != expectedValue {
			fmt.Printf("✗ %s: expected '%s', got '%s'\n", keys[i], expectedValue, actualValue)
			allValid = false
		} else {
			fmt.Printf("✓ %s: %s\n", keys[i], actualValue)
		}
	}

	// Print summary
	fmt.Println("\n=== Summary ===")
	if allValid {
		fmt.Println("✓ All values retrieved successfully and match expected values!")
	} else {
		fmt.Println("✗ Some values did not match expected values")
	}
	*/

	// Clean up - delete the keys
	fmt.Println("\n=== Cleaning up ===")
	for _, key := range keys {
		if err := rdb.Del(ctx, key).Err(); err != nil {
			fmt.Printf("Warning: Failed to delete %s: %v\n", key, err)
		}
	}
	fmt.Println("✓ Cleanup complete")

	err := rdb.Set(ctx, "{tag}exists", "asdf",0).Err()
	if err != nil {
		panic(err)
	}
	val, err := rdb.Get(ctx, "{tag}nilkeykey1").Result()
	fmt.Printf("\nval: %+v err: %+v\n", val, err)
	valm, err := rdb.MGet(ctx, "{tag}nilkeykey1", "{tag}exists").Result()
	fmt.Printf("\nval: %+v err: %+v\n", valm, err)
}
