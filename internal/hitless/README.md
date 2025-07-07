# Hitless Upgrade Package

This package implements hitless upgrade functionality for Redis cluster clients using the push notification architecture. It provides handlers for managing connection and pool state during Redis cluster upgrades.

## Quick Start

To enable hitless upgrades in your Redis client, simply set the configuration option:

```go
import "github.com/redis/go-redis/v9"

// Enable hitless upgrades with a simple configuration option
client := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs:           []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
    Protocol:        3,    // RESP3 required for push notifications
    HitlessUpgrades: true, // Enable hitless upgrades
})
defer client.Close()

// That's it! Use your client normally - hitless upgrades work automatically
ctx := context.Background()
client.Set(ctx, "key", "value", 0)
```