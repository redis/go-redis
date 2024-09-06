// EXAMPLE: set_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_set_get_all() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike:1")
	// REMOVE_END

	// STEP_START set_get_all
	hashFields := []string{
		"model", "Deimos",
		"brand", "Ergonom",
		"type", "Enduro bikes",
		"price", "4972",
	}

	res1, err := rdb.HSet(ctx, "bike:1", hashFields).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> 4

	res2, err := rdb.HGet(ctx, "bike:1", "model").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> Deimos

	res3, err := rdb.HGet(ctx, "bike:1", "price").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> 4972

	res4, err := rdb.HGetAll(ctx, "bike:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4)
	// >>> map[brand:Ergonom model:Deimos price:4972 type:Enduro bikes]
	// STEP_END

	// Output:
	// 4
	// Deimos
	// 4972
	// map[brand:Ergonom model:Deimos price:4972 type:Enduro bikes]
}

func ExampleClient_hmget() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike:1")
	// REMOVE_END

	hashFields := []string{
		"model", "Deimos",
		"brand", "Ergonom",
		"type", "Enduro bikes",
		"price", "4972",
	}

	_, err := rdb.HSet(ctx, "bike:1", hashFields).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START hmget
	res5, err := rdb.HMGet(ctx, "bike:1", "model", "price").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> [Deimos 4972]
	// STEP_END

	// Output:
	// [Deimos 4972]
}

func ExampleClient_hincrby() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike:1")
	// REMOVE_END

	hashFields := []string{
		"model", "Deimos",
		"brand", "Ergonom",
		"type", "Enduro bikes",
		"price", "4972",
	}

	_, err := rdb.HSet(ctx, "bike:1", hashFields).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START hincrby
	res6, err := rdb.HIncrBy(ctx, "bike:1", "price", 100).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> 5072

	res7, err := rdb.HIncrBy(ctx, "bike:1", "price", -100).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> 4972
	// STEP_END

	// Output:
	// 5072
	// 4972
}

func ExampleClient_incrby_get_mget() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike:1:stats")
	// REMOVE_END

	// STEP_START incrby_get_mget
	res8, err := rdb.HIncrBy(ctx, "bike:1:stats", "rides", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> 1

	res9, err := rdb.HIncrBy(ctx, "bike:1:stats", "rides", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> 2

	res10, err := rdb.HIncrBy(ctx, "bike:1:stats", "rides", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> 3

	res11, err := rdb.HIncrBy(ctx, "bike:1:stats", "crashes", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> 1

	res12, err := rdb.HIncrBy(ctx, "bike:1:stats", "owners", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> 1

	res13, err := rdb.HGet(ctx, "bike:1:stats", "rides").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13) // >>> 3

	res14, err := rdb.HMGet(ctx, "bike:1:stats", "crashes", "owners").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14) // >>> [1 1]
	// STEP_END

	// Output:
	// 1
	// 2
	// 3
	// 1
	// 1
	// 3
	// [1 1]
}
