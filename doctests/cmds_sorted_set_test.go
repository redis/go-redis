// EXAMPLE: cmds_sorted_set
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_zadd_cmd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "myzset")
	// REMOVE_END

	// STEP_START zadd
	zAddResult1, err := rdb.ZAdd(ctx, "myzset",
		redis.Z{Member: "one", Score: 1},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zAddResult1) // >>> 1

	zAddResult2, err := rdb.ZAdd(ctx, "myzset",
		redis.Z{Member: "uno", Score: 1},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zAddResult2)

	zAddResult3, err := rdb.ZAdd(ctx, "myzset",
		redis.Z{Member: "two", Score: 2},
		redis.Z{Member: "three", Score: 3},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zAddResult3) // >>> 2

	zAddResult4, err := rdb.ZRangeWithScores(ctx, "myzset", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zAddResult4) // >>> [{1 one} {1 uno} {2 two} {3 three}]
	// STEP_END

	// Output:
	// 1
	// 1
	// 2
	// [{1 one} {1 uno} {2 two} {3 three}]
}

func ExampleClient_zrange1() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "myzset")
	// REMOVE_END

	// STEP_START zrange1
	zrangeResult1, err := rdb.ZAdd(ctx, "myzset",
		redis.Z{Member: "one", Score: 1},
		redis.Z{Member: "two", Score: 2},
		redis.Z{Member: "three", Score: 3},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zrangeResult1) // >>> 3

	zrangeResult2, err := rdb.ZRange(ctx, "myzset", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zrangeResult2) // >>> [one two three]

	zrangeResult3, err := rdb.ZRange(ctx, "myzset", 2, 3).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zrangeResult3) // >>> [three]

	zrangeResult4, err := rdb.ZRange(ctx, "myzset", -2, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zrangeResult4) // >>> [two three]
	// STEP_END

	// Output:
	// 3
	// [one two three]
	// [three]
	// [two three]
}

func ExampleClient_zrange2() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "myzset")
	// REMOVE_END

	// STEP_START zrange2
	zRangeResult5, err := rdb.ZAdd(ctx, "myzset",
		redis.Z{Member: "one", Score: 1},
		redis.Z{Member: "two", Score: 2},
		redis.Z{Member: "three", Score: 3},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zRangeResult5) // >>> 3

	zRangeResult6, err := rdb.ZRangeWithScores(ctx, "myzset", 0, 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zRangeResult6) // >>> [{1 one} {2 two}]
	// STEP_END

	// Output:
	// 3
	// [{1 one} {2 two}]
}

func ExampleClient_zrange3() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "myzset")
	// REMOVE_END

	// STEP_START zrange3
	zRangeResult7, err := rdb.ZAdd(ctx, "myzset",
		redis.Z{Member: "one", Score: 1},
		redis.Z{Member: "two", Score: 2},
		redis.Z{Member: "three", Score: 3},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zRangeResult7) // >>> 3

	zRangeResult8, err := rdb.ZRangeArgs(ctx,
		redis.ZRangeArgs{
			Key:     "myzset",
			ByScore: true,
			Start:   "(1",
			Stop:    "+inf",
			Offset:  1,
			Count:   1,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(zRangeResult8) // >>> [three]
	// STEP_END

	// Output:
	// 3
	// [three]
}
