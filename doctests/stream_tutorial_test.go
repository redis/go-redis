// EXAMPLE: stream_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

// REMOVE_START
func UNUSED(v ...interface{}) {}

// REMOVE_END

func ExampleClient_xadd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	// STEP_START xadd
	res1, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res1) // >>> 1692629576966-0

	res2, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.PrintLn(res2) // >>> 1692629594113-0

	res3, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res3) // >>> 1692629613374-0
	// STEP_END

	// REMOVE_START
	UNUSED(res1, res2, res3)
	// REMOVE_END

	xlen, err := rdb.XLen(ctx, "race:france").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(xlen) // >>> 3

	// Output:
	// 3
}
