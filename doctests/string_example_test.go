// EXAMPLE: set_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END
func ExampleClient_set_get() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike:1")
	// REMOVE_END

	// STEP_START set_get
	res1, err := rdb.Set(ctx, "bike:1", "Deimos", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.Get(ctx, "bike:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> Deimos
	// STEP_END

	// Output:
	// OK
	// Deimos
}

func ExampleClient_setnx_xx() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Set(ctx, "bike:1", "Deimos", 0)
	// REMOVE_END

	// STEP_START setnx_xx
	res3, err := rdb.SetNX(ctx, "bike:1", "bike", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> false

	res4, err := rdb.Get(ctx, "bike:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> Deimos

	res5, err := rdb.SetXX(ctx, "bike:1", "bike", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> OK
	// STEP_END

	// Output:
	// false
	// Deimos
	// true
}

func ExampleClient_mset() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike:1", "bike:2", "bike:3")
	// REMOVE_END

	// STEP_START mset
	res6, err := rdb.MSet(ctx, "bike:1", "Deimos", "bike:2", "Ares", "bike:3", "Vanth").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> OK

	res7, err := rdb.MGet(ctx, "bike:1", "bike:2", "bike:3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> [Deimos Ares Vanth]
	// STEP_END

	// Output:
	// OK
	// [Deimos Ares Vanth]
}

func ExampleClient_incr() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "total_crashes")
	// REMOVE_END

	// STEP_START incr
	res8, err := rdb.Set(ctx, "total_crashes", "0", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> OK

	res9, err := rdb.Incr(ctx, "total_crashes").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> 1

	res10, err := rdb.IncrBy(ctx, "total_crashes", 10).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> 11
	// STEP_END

	// Output:
	// OK
	// 1
	// 11
}
