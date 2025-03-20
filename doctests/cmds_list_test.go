// EXAMPLE: cmds_list
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_cmd_llen() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "mylist")
	// REMOVE_END

	// STEP_START llen
	lPushResult1, err := rdb.LPush(ctx, "mylist", "World").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPushResult1) // >>> 1

	lPushResult2, err := rdb.LPush(ctx, "mylist", "Hello").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPushResult2) // >>> 2

	lLenResult, err := rdb.LLen(ctx, "mylist").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lLenResult) // >>> 2
	// STEP_END

	// Output:
	// 1
	// 2
	// 2
}
func ExampleClient_cmd_lpop() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "mylist")
	// REMOVE_END

	// STEP_START lpop
	RPushResult, err := rdb.RPush(ctx,
		"mylist", "one", "two", "three", "four", "five",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(RPushResult) // >>> 5

	lPopResult, err := rdb.LPop(ctx, "mylist").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPopResult) // >>> one

	lPopCountResult, err := rdb.LPopCount(ctx, "mylist", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPopCountResult) // >>> [two three]

	lRangeResult, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult) // >>> [four five]
	// STEP_END

	// Output:
	// 5
	// one
	// [two three]
	// [four five]
}

func ExampleClient_cmd_lpush() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "mylist")
	// REMOVE_END

	// STEP_START lpush
	lPushResult1, err := rdb.LPush(ctx, "mylist", "World").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPushResult1) // >>> 1

	lPushResult2, err := rdb.LPush(ctx, "mylist", "Hello").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPushResult2) // >>> 2

	lRangeResult, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult) // >>> [Hello World]
	// STEP_END

	// Output:
	// 1
	// 2
	// [Hello World]
}

func ExampleClient_cmd_lrange() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "mylist")
	// REMOVE_END

	// STEP_START lrange
	RPushResult, err := rdb.RPush(ctx, "mylist",
		"one", "two", "three",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(RPushResult) // >>> 3

	lRangeResult1, err := rdb.LRange(ctx, "mylist", 0, 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult1) // >>> [one]

	lRangeResult2, err := rdb.LRange(ctx, "mylist", -3, 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult2) // >>> [one two three]

	lRangeResult3, err := rdb.LRange(ctx, "mylist", -100, 100).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult3) // >>> [one two three]

	lRangeResult4, err := rdb.LRange(ctx, "mylist", 5, 10).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult4) // >>> []
	// STEP_END

	// Output:
	// 3
	// [one]
	// [one two three]
	// [one two three]
	// []
}

func ExampleClient_cmd_rpop() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "mylist")
	// REMOVE_END

	// STEP_START rpop
	rPushResult, err := rdb.RPush(ctx, "mylist",
		"one", "two", "three", "four", "five",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPushResult) // >>> 5

	rPopResult, err := rdb.RPop(ctx, "mylist").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPopResult) // >>> five

	rPopCountResult, err := rdb.RPopCount(ctx, "mylist", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPopCountResult) // >>> [four three]

	lRangeResult, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult) // >>> [one two]
	// STEP_END

	// Output:
	// 5
	// five
	// [four three]
	// [one two]
}

func ExampleClient_cmd_rpush() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "mylist")
	// REMOVE_END

	// STEP_START rpush
	rPushResult1, err := rdb.RPush(ctx, "mylist", "Hello").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPushResult1) // >>> 1

	rPushResult2, err := rdb.RPush(ctx, "mylist", "World").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPushResult2) // >>> 2

	lRangeResult, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult) // >>> [Hello World]
	// STEP_END

	// Output:
	// 1
	// 2
	// [Hello World]
}
