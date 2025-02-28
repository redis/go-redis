// EXAMPLE: bitfield_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_bf() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "bike:1:stats")
	// REMOVE_END

	// STEP_START bf
	res1, err := rdb.BitField(ctx, "bike:1:stats",
		"set", "u32", "#0", "1000",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> [0]

	res2, err := rdb.BitField(ctx,
		"bike:1:stats",
		"incrby", "u32", "#0", "-50",
		"incrby", "u32", "#1", "1",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> [950 1]

	res3, err := rdb.BitField(ctx,
		"bike:1:stats",
		"incrby", "u32", "#0", "500",
		"incrby", "u32", "#1", "1",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> [1450 2]

	res4, err := rdb.BitField(ctx, "bike:1:stats",
		"get", "u32", "#0",
		"get", "u32", "#1",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> [1450 2]
	// STEP_END

	// Output:
	// [0]
	// [950 1]
	// [1450 2]
	// [1450 2]
}
