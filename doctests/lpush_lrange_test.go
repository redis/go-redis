// EXAMPLE: lpush_and_lrange
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func ExampleLPushLRange() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// HIDE_END

	// REMOVE_START
	errFlush := rdb.FlushDB(ctx).Err() // Clear the database before each test
	if errFlush != nil {
		panic(errFlush)
	}
	// REMOVE_END

	listSize, err := rdb.LPush(ctx, "my_bikes", "bike:1", "bike:2").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(listSize)

	value, err := rdb.LRange(ctx, "my_bikes", 0, -1).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	// HIDE_START

	// Output: 2
	// [bike:2 bike:1]
}

// HIDE_END
