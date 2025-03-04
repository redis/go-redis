// EXAMPLE: cmds_hash
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"sort"

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
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
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

	keys := make([]string, 0, len(res6))

	for key, _ := range res6 {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		fmt.Printf("Key: %v, value: %v\n", key, res6[key])
	}
	// >>> Key: field1, value: Hello
	// >>> Key: field2, value: Hi
	// >>> Key: field3, value: World
	// STEP_END

	// Output:
	// 1
	// Hello
	// 2
	// Hi
	// World
	// Key: field1, value: Hello
	// Key: field2, value: Hi
	// Key: field3, value: World
}

func ExampleClient_hget() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
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

func ExampleClient_hgetall() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "myhash")
	// REMOVE_END

	// STEP_START hgetall
	hGetAllResult1, err := rdb.HSet(ctx, "myhash",
		"field1", "Hello",
		"field2", "World",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(hGetAllResult1) // >>> 2

	hGetAllResult2, err := rdb.HGetAll(ctx, "myhash").Result()

	if err != nil {
		panic(err)
	}

	keys := make([]string, 0, len(hGetAllResult2))

	for key, _ := range hGetAllResult2 {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		fmt.Printf("Key: %v, value: %v\n", key, hGetAllResult2[key])
	}
	// >>> Key: field1, value: Hello
	// >>> Key: field2, value: World
	// STEP_END

	// Output:
	// 2
	// Key: field1, value: Hello
	// Key: field2, value: World
}

func ExampleClient_hvals() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "myhash")
	// REMOVE_END

	// STEP_START hvals
	hValsResult1, err := rdb.HSet(ctx, "myhash",
		"field1", "Hello",
		"field2", "World",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(hValsResult1) // >>> 2

	hValsResult2, err := rdb.HVals(ctx, "myhash").Result()

	if err != nil {
		panic(err)
	}

	sort.Strings(hValsResult2)

	fmt.Println(hValsResult2) // >>> [Hello World]
	// STEP_END

	// Output:
	// 2
	// [Hello World]
}
