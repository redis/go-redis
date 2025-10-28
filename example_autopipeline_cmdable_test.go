package redis_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func ExampleAutoPipeliner_cmdable() {
	ctx := context.Background()
	
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Create an autopipeliner
	ap := client.AutoPipeline()
	defer ap.Close()

	// Use autopipeliner like a regular client - all commands are automatically batched!
	// No need to call Do() - you can use typed methods directly
	
	// String commands
	ap.Set(ctx, "name", "Alice", 0)
	ap.Set(ctx, "age", "30", 0)
	
	// Hash commands
	ap.HSet(ctx, "user:1", "name", "Bob", "email", "bob@example.com")
	
	// List commands
	ap.RPush(ctx, "tasks", "task1", "task2", "task3")
	
	// Set commands
	ap.SAdd(ctx, "tags", "go", "redis", "autopipeline")
	
	// Sorted set commands
	ap.ZAdd(ctx, "scores",
		redis.Z{Score: 100, Member: "player1"},
		redis.Z{Score: 200, Member: "player2"},
	)
	
	// Get results - commands are executed automatically when you access results
	name, _ := ap.Get(ctx, "name").Result()
	age, _ := ap.Get(ctx, "age").Result()
	user, _ := ap.HGetAll(ctx, "user:1").Result()
	tasks, _ := ap.LRange(ctx, "tasks", 0, -1).Result()
	tags, _ := ap.SMembers(ctx, "tags").Result()
	scores, _ := ap.ZRangeWithScores(ctx, "scores", 0, -1).Result()
	
	fmt.Println("Name:", name)
	fmt.Println("Age:", age)
	fmt.Println("User:", user)
	fmt.Println("Tasks:", tasks)
	fmt.Println("Tags count:", len(tags))
	fmt.Println("Scores count:", len(scores))
	
	// Output:
	// Name: Alice
	// Age: 30
	// User: map[email:bob@example.com name:Bob]
	// Tasks: [task1 task2 task3]
	// Tags count: 3
	// Scores count: 2
}

func ExampleAutoPipeliner_mixedUsage() {
	ctx := context.Background()
	
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// You can mix autopipelined commands with traditional pipelines
	
	// Autopipelined commands (batched automatically)
	ap.Set(ctx, "auto1", "value1", 0)
	ap.Set(ctx, "auto2", "value2", 0)
	
	// Traditional pipeline (explicit batching)
	pipe := ap.Pipeline()
	pipe.Set(ctx, "pipe1", "value1", 0)
	pipe.Set(ctx, "pipe2", "value2", 0)
	pipe.Exec(ctx)
	
	// Pipelined helper (convenience method)
	ap.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, "helper1", "value1", 0)
		pipe.Set(ctx, "helper2", "value2", 0)
		return nil
	})
	
	fmt.Println("All commands executed successfully")
	
	// Output:
	// All commands executed successfully
}

func ExampleAutoPipeliner_genericFunction() {
	ctx := context.Background()
	
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// AutoPipeliner implements Cmdable, so you can pass it to functions
	// that accept any Redis client type
	
	ap := client.AutoPipeline()
	defer ap.Close()
	
	// This function works with any Cmdable (Client, Pipeline, AutoPipeliner, etc.)
	setUserData := func(c redis.Cmdable, userID string, name string, email string) error {
		c.HSet(ctx, "user:"+userID, "name", name, "email", email)
		c.SAdd(ctx, "users", userID)
		return nil
	}
	
	// Use with autopipeliner - commands are batched automatically
	setUserData(ap, "123", "Alice", "alice@example.com")
	setUserData(ap, "456", "Bob", "bob@example.com")
	
	// Verify
	users, _ := ap.SMembers(ctx, "users").Result()
	fmt.Println("Users count:", len(users))
	
	// Output:
	// Users count: 2
}

