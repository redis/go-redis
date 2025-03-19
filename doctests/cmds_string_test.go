// EXAMPLE: cmds_string
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_cmd_incr() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "mykey")
	// REMOVE_END

	// STEP_START incr
	incrResult1, err := rdb.Set(ctx, "mykey", "10", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(incrResult1) // >>> OK

	incrResult2, err := rdb.Incr(ctx, "mykey").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(incrResult2) // >>> 11

	incrResult3, err := rdb.Get(ctx, "mykey").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(incrResult3) // >>> 11
	// STEP_END

	// Output:
	// OK
	// 11
	// 11
}
