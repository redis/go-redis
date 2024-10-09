// EXAMPLE: cmds_hash
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_hset() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "myhash")
	// REMOVE_END

	// STEP_START hset
	res1, err := rdb.HSet(ctx, "myhash", "field1", "Hello").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> 1

	res2, err := rdb.HGet(ctx, "myhash", "field1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> Hello

	res3, err := rdb.HSet(ctx, "myhash",
		"field2", "Hi",
		"field3", "World",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> 2

	res4, err := rdb.HGet(ctx, "myhash", "field2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> Hi

	res5, err := rdb.HGet(ctx, "myhash", "field3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> World

	res6, err := rdb.HGetAll(ctx, "myhash").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6)
	// >>> map[field1:Hello field2:Hi field3:World]
	// STEP_END

	// Output:
	// 1
	// Hello
	// 2
	// Hi
	// World
	// map[field1:Hello field2:Hi field3:World]
}

func ExampleClient_hget() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "myhash")
	// REMOVE_END

	// STEP_START hget
	res7, err := rdb.HSet(ctx, "myhash", "field1", "foo").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> 1

	res8, err := rdb.HGet(ctx, "myhash", "field1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> foo

	res9, err := rdb.HGet(ctx, "myhash", "field2").Result()

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(res9) // >>> <empty string>
	// STEP_END

	// Output:
	// 1
	// foo
	// redis: nil
}
