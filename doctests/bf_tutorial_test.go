// EXAMPLE: bf_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_bloom() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:models")
	// REMOVE_END

	// STEP_START bloom
	res1, err := rdb.BFReserve(ctx, "bikes:models", 0.01, 1000).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.BFAdd(ctx, "bikes:models", "Smoky Mountain Striker").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> true

	res3, err := rdb.BFExists(ctx, "bikes:models", "Smoky Mountain Striker").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> true

	res4, err := rdb.BFMAdd(ctx, "bikes:models",
		"Rocky Mountain Racer",
		"Cloudy City Cruiser",
		"Windy City Wippet",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> [true true true]

	res5, err := rdb.BFMExists(ctx, "bikes:models",
		"Rocky Mountain Racer",
		"Cloudy City Cruiser",
		"Windy City Wippet",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> [true true true]
	// STEP_END

	// Output:
	// OK
	// true
	// true
	// [true true true]
	// [true true true]
}
