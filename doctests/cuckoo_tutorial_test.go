// EXAMPLE: cuckoo_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_cuckoo() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:models")
	// REMOVE_END

	// STEP_START cuckoo
	res1, err := rdb.CFReserve(ctx, "bikes:models", 1000000).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.CFAdd(ctx, "bikes:models", "Smoky Mountain Striker").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> true

	res3, err := rdb.CFExists(ctx, "bikes:models", "Smoky Mountain Striker").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> true

	res4, err := rdb.CFExists(ctx, "bikes:models", "Terrible Bike Name").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> false

	res5, err := rdb.CFDel(ctx, "bikes:models", "Smoky Mountain Striker").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> true
	// STEP_END

	// Output:
	// OK
	// true
	// true
	// false
	// true
}
