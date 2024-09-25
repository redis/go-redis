// EXAMPLE: bitmap_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_ping() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "pings:2024-01-01-00:00")
	// REMOVE_END

	// STEP_START ping
	res1, err := rdb.SetBit(ctx, "pings:2024-01-01-00:00", 123, 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> 0

	res2, err := rdb.GetBit(ctx, "pings:2024-01-01-00:00", 123).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> 1

	res3, err := rdb.GetBit(ctx, "pings:2024-01-01-00:00", 456).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> 0
	// STEP_END

	// Output:
	// 0
	// 1
	// 0
}

func ExampleClient_bitcount() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	_, err := rdb.SetBit(ctx, "pings:2024-01-01-00:00", 123, 1).Result()

	if err != nil {
		panic(err)
	}
	// REMOVE_END

	// STEP_START bitcount
	res4, err := rdb.BitCount(ctx, "pings:2024-01-01-00:00",
		&redis.BitCount{
			Start: 0,
			End:   456,
		}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> 1
	// STEP_END

	// Output:
	// 1
}
