// Command multidb shows a minimal MultiDBClient: two member databases, one
// active at a time, with automatic failover between them.
//
// EXPERIMENTAL: the MultiDB API is subject to change.
//
// Start two Redis servers, e.g.
//
//	docker run --rm -p 6379:6379 redis
//	docker run --rm -p 6380:6379 redis
//
// then:
//
//	go run . -addr1 localhost:6379 -addr2 localhost:6380
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	addr1 := flag.String("addr1", "localhost:6379", "primary member address")
	addr2 := flag.String("addr2", "localhost:6380", "secondary member address")
	flag.Parse()

	// A deadline makes NewMultiDBClient retry the initial health checks until
	// InitialDBState is met or the deadline expires.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := redis.NewMultiDBClient(ctx, &redis.MultiDBOptions{
		// Members get ids 0 and 1, in slice order. The higher weight is
		// preferred.
		Clients: []redis.MultiDBClientConfig{
			{Options: &redis.Options{Addr: *addr1}, Weight: 2},
			{Options: &redis.Options{Addr: *addr2}, Weight: 1},
		},
		// Start when at least one member is healthy. The default (majority)
		// needs both of two members.
		InitialDBState: redis.InitialDBStateOneAvailable,
		OnActiveDatabaseChanged: func(from, to int) {
			fmt.Printf("active database changed: %d -> %d\n", from, to)
		},
	})
	if err != nil {
		log.Fatalf("NewMultiDBClient: %v", err)
	}
	defer client.Close()

	fmt.Println("active database:", client.ActiveDatabaseID())

	// Commands go to the active member.
	if err := client.Set(ctx, "multidb:example", "hello", 0).Err(); err != nil {
		log.Fatalf("SET: %v", err)
	}
	val, err := client.Get(ctx, "multidb:example").Result()
	if err != nil {
		log.Fatalf("GET: %v", err)
	}
	fmt.Println("GET multidb:example =", val)

	// Manual switch. SetActiveDatabase probes the target first and returns
	// redis.ErrTargetUnhealthy when it is down.
	target := 1
	if client.ActiveDatabaseID() == 1 {
		target = 0
	}
	if err := client.SetActiveDatabase(ctx, target); err != nil {
		fmt.Printf("SetActiveDatabase(%d): %v\n", target, err)
	} else {
		fmt.Println("active database:", client.ActiveDatabaseID())
	}
}
