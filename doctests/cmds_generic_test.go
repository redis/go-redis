// EXAMPLE: cmds_generic
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_del_cmd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "key1", "key2", "key3")
	// REMOVE_END

	// STEP_START del
	delResult1, err := rdb.Set(ctx, "key1", "Hello", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(delResult1) // >>> OK

	delResult2, err := rdb.Set(ctx, "key2", "World", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(delResult2) // >>> OK

	delResult3, err := rdb.Del(ctx, "key1", "key2", "key3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(delResult3) // >>> 2
	// STEP_END

	// Output:
	// OK
	// OK
	// 2
}

func ExampleClient_expire_cmd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "mykey")
	// REMOVE_END

	// STEP_START expire
	expireResult1, err := rdb.Set(ctx, "mykey", "Hello", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(expireResult1) // >>> OK

	expireResult2, err := rdb.Expire(ctx, "mykey", 10*time.Second).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(expireResult2) // >>> true

	expireResult3, err := rdb.TTL(ctx, "mykey").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(math.Round(expireResult3.Seconds())) // >>> 10

	expireResult4, err := rdb.Set(ctx, "mykey", "Hello World", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(expireResult4) // >>> OK

	expireResult5, err := rdb.TTL(ctx, "mykey").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(expireResult5) // >>> -1ns

	expireResult6, err := rdb.ExpireXX(ctx, "mykey", 10*time.Second).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(expireResult6) // >>> false

	expireResult7, err := rdb.TTL(ctx, "mykey").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(expireResult7) // >>> -1ns

	expireResult8, err := rdb.ExpireNX(ctx, "mykey", 10*time.Second).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(expireResult8) // >>> true

	expireResult9, err := rdb.TTL(ctx, "mykey").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(math.Round(expireResult9.Seconds())) // >>> 10
	// STEP_END

	// Output:
	// OK
	// true
	// 10
	// OK
	// -1ns
	// false
	// -1ns
	// true
	// 10
}

func ExampleClient_ttl_cmd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "mykey")
	// REMOVE_END

	// STEP_START ttl
	ttlResult1, err := rdb.Set(ctx, "mykey", "Hello", 10*time.Second).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(ttlResult1) // >>> OK

	ttlResult2, err := rdb.TTL(ctx, "mykey").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(math.Round(ttlResult2.Seconds())) // >>> 10
	// STEP_END

	// Output:
	// OK
	// 10
}
