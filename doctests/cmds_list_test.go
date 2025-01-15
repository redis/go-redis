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
	lLenResult1, err := rdb.LPush(ctx, "mylist", "World").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lLenResult1) // >>> 1

	lLenResult2, err := rdb.LPush(ctx, "mylist", "Hello").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lLenResult2) // >>> 2

	lLenResult3, err := rdb.LLen(ctx, "mylist").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lLenResult3) // >>> 2
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
	lPopResult1, err := rdb.RPush(ctx,
		"mylist", "one", "two", "three", "four", "five",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPopResult1) // >>> 5

	lPopResult2, err := rdb.LPop(ctx, "mylist").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPopResult2) // >>> one

	lPopResult3, err := rdb.LPopCount(ctx, "mylist", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPopResult3) // >>> [two three]

	lPopResult4, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPopResult4) // >>> [four five]
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

	lPushResult3, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lPushResult3) // >>> [Hello World]
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
	lRangeResult1, err := rdb.RPush(ctx, "mylist",
		"one", "two", "three",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult1) // >>> 3

	lRangeResult2, err := rdb.LRange(ctx, "mylist", 0, 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult2) // >>> [one]

	lRangeResult3, err := rdb.LRange(ctx, "mylist", -3, 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult3) // >>> [one two three]

	lRangeResult4, err := rdb.LRange(ctx, "mylist", -100, 100).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult4) // >>> [one two three]

	lRangeResult5, err := rdb.LRange(ctx, "mylist", 5, 10).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(lRangeResult5) // >>> []
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
	rPopResult1, err := rdb.RPush(ctx, "mylist",
		"one", "two", "three", "four", "five",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPopResult1) // >>> 5

	rPopResult2, err := rdb.RPop(ctx, "mylist").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPopResult2) // >>> five

	rPopResult3, err := rdb.RPopCount(ctx, "mylist", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPopResult3) // >>> [four three]

	rPopResult4, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPopResult4) // >>> [one two]
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

	rPushResult3, err := rdb.LRange(ctx, "mylist", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(rPushResult3) // >>> [Hello World]
	// STEP_END

	// Output:
	// 1
	// 2
	// [Hello World]
}
