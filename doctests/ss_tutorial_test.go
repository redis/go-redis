// EXAMPLE: ss_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END
func ExampleClient_zadd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	// STEP_START zadd
	res1, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 10},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> 1

	res2, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Castilla", Score: 12},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> 1

	res3, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 10},
		redis.Z{Member: "Sam-Bodden", Score: 8},
		redis.Z{Member: "Royce", Score: 10},
		redis.Z{Member: "Ford", Score: 6},
		redis.Z{Member: "Prickett", Score: 14},
		redis.Z{Member: "Castilla", Score: 12},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> 4
	// STEP_END

	// Output:
	// 1
	// 1
	// 4
}

func ExampleClient_zrange() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	_, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 10},
		redis.Z{Member: "Sam-Bodden", Score: 8},
		redis.Z{Member: "Royce", Score: 10},
		redis.Z{Member: "Ford", Score: 6},
		redis.Z{Member: "Prickett", Score: 14},
		redis.Z{Member: "Castilla", Score: 12},
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START zrange
	res4, err := rdb.ZRange(ctx, "racer_scores", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4)
	// >>> [Ford Sam-Bodden Norem Royce Castilla Prickett]

	res5, err := rdb.ZRevRange(ctx, "racer_scores", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5)
	// >>> [Prickett Castilla Royce Norem Sam-Bodden Ford]
	// STEP_END

	// Output:
	// [Ford Sam-Bodden Norem Royce Castilla Prickett]
	// [Prickett Castilla Royce Norem Sam-Bodden Ford]
}

func ExampleClient_zrangewithscores() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	_, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 10},
		redis.Z{Member: "Sam-Bodden", Score: 8},
		redis.Z{Member: "Royce", Score: 10},
		redis.Z{Member: "Ford", Score: 6},
		redis.Z{Member: "Prickett", Score: 14},
		redis.Z{Member: "Castilla", Score: 12},
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START zrange_withscores
	res6, err := rdb.ZRangeWithScores(ctx, "racer_scores", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6)
	// >>> [{6 Ford} {8 Sam-Bodden} {10 Norem} {10 Royce} {12 Castilla} {14 Prickett}]
	// STEP_END

	// Output:
	// [{6 Ford} {8 Sam-Bodden} {10 Norem} {10 Royce} {12 Castilla} {14 Prickett}]
}

func ExampleClient_zrangebyscore() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	_, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 10},
		redis.Z{Member: "Sam-Bodden", Score: 8},
		redis.Z{Member: "Royce", Score: 10},
		redis.Z{Member: "Ford", Score: 6},
		redis.Z{Member: "Prickett", Score: 14},
		redis.Z{Member: "Castilla", Score: 12},
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START zrangebyscore
	res7, err := rdb.ZRangeByScore(ctx, "racer_scores",
		&redis.ZRangeBy{Min: "-inf", Max: "10"},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7)
	// >>> [Ford Sam-Bodden Norem Royce]
	// STEP_END

	// Output:
	// [Ford Sam-Bodden Norem Royce]
}

func ExampleClient_zremrangebyscore() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	_, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 10},
		redis.Z{Member: "Sam-Bodden", Score: 8},
		redis.Z{Member: "Royce", Score: 10},
		redis.Z{Member: "Ford", Score: 6},
		redis.Z{Member: "Prickett", Score: 14},
		redis.Z{Member: "Castilla", Score: 12},
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START zremrangebyscore
	res8, err := rdb.ZRem(ctx, "racer_scores", "Castilla").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> 1

	res9, err := rdb.ZRemRangeByScore(ctx, "racer_scores", "-inf", "9").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> 2

	res10, err := rdb.ZRange(ctx, "racer_scores", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10)
	// >>> [Norem Royce Prickett]
	// STEP_END

	// Output:
	// 1
	// 2
	// [Norem Royce Prickett]
}

func ExampleClient_zrank() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	_, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 10},
		redis.Z{Member: "Royce", Score: 10},
		redis.Z{Member: "Prickett", Score: 14},
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START zrank
	res11, err := rdb.ZRank(ctx, "racer_scores", "Norem").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> 0

	res12, err := rdb.ZRevRank(ctx, "racer_scores", "Norem").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> 2
	// STEP_END

	// Output:
	// 0
	// 2
}

func ExampleClient_zaddlex() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	_, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 0},
		redis.Z{Member: "Royce", Score: 0},
		redis.Z{Member: "Prickett", Score: 0},
	).Result()

	// STEP_START zadd_lex
	res13, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Norem", Score: 0},
		redis.Z{Member: "Sam-Bodden", Score: 0},
		redis.Z{Member: "Royce", Score: 0},
		redis.Z{Member: "Ford", Score: 0},
		redis.Z{Member: "Prickett", Score: 0},
		redis.Z{Member: "Castilla", Score: 0},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13) // >>> 3

	res14, err := rdb.ZRange(ctx, "racer_scores", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14)
	// >>> [Castilla Ford Norem Prickett Royce Sam-Bodden]

	res15, err := rdb.ZRangeByLex(ctx, "racer_scores", &redis.ZRangeBy{
		Min: "[A", Max: "[L",
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res15) // >>> [Castilla Ford]
	// STEP_END

	// Output:
	// 3
	// [Castilla Ford Norem Prickett Royce Sam-Bodden]
	// [Castilla Ford]
}

func ExampleClient_leaderboard() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "racer_scores")
	// REMOVE_END

	// STEP_START leaderboard
	res16, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Wood", Score: 100},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res16) // >>> 1

	res17, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Henshaw", Score: 100},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res17) // >>> 1

	res18, err := rdb.ZAdd(ctx, "racer_scores",
		redis.Z{Member: "Henshaw", Score: 150},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res18) // >>> 0

	res19, err := rdb.ZIncrBy(ctx, "racer_scores", 50, "Wood").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res19) // >>> 150

	res20, err := rdb.ZIncrBy(ctx, "racer_scores", 50, "Henshaw").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> 200
	// STEP_END

	// Output:
	// 1
	// 1
	// 0
	// 150
	// 200
}
