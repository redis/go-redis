// EXAMPLE: hll_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_pfadd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes", "commuter_bikes", "all_bikes")
	// REMOVE_END

	// STEP_START pfadd
	res1, err := rdb.PFAdd(ctx, "bikes", "Hyperion", "Deimos", "Phoebe", "Quaoar").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // 1

	res2, err := rdb.PFCount(ctx, "bikes").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // 4

	res3, err := rdb.PFAdd(ctx, "commuter_bikes", "Salacia", "Mimas", "Quaoar").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // 1

	res4, err := rdb.PFMerge(ctx, "all_bikes", "bikes", "commuter_bikes").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // OK

	res5, err := rdb.PFCount(ctx, "all_bikes").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // 6
	// STEP_END

	// Output:
	// 1
	// 4
	// 1
	// OK
	// 6
}
