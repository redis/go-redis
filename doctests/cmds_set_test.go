// EXAMPLE: cmds_set
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_sadd_cmd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "myset")
	// REMOVE_END

	// STEP_START sadd
	sAddResult1, err := rdb.SAdd(ctx, "myset", "Hello").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(sAddResult1) // >>> 1

	sAddResult2, err := rdb.SAdd(ctx, "myset", "World").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(sAddResult2) // >>> 1

	sAddResult3, err := rdb.SAdd(ctx, "myset", "World").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(sAddResult3) // >>> 0

	sMembersResult, err := rdb.SMembers(ctx, "myset").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(sMembersResult) // >>> [Hello World]
	// STEP_END

	// Output:
	// 1
	// 1
	// 0
	// [Hello World]
}

func ExampleClient_smembers_cmd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "myset")
	// REMOVE_END

	// STEP_START smembers
	sAddResult, err := rdb.SAdd(ctx, "myset", "Hello", "World").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(sAddResult) // >>> 2

	sMembersResult, err := rdb.SMembers(ctx, "myset").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(sMembersResult) // >>> [Hello World]
	// STEP_END

	// Output:
	// 2
	// [Hello World]
}
