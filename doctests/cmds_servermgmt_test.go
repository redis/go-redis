// EXAMPLE: cmds_servermgmt
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_cmd_flushall() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// STEP_START flushall
	// REMOVE_START
	rdb.Set(ctx, "testkey1", "1", 0)
	rdb.Set(ctx, "testkey2", "2", 0)
	rdb.Set(ctx, "testkey3", "3", 0)
	// REMOVE_END
	flushAllResult1, err := rdb.FlushAll(ctx).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(flushAllResult1) // >>> OK

	flushAllResult2, err := rdb.Keys(ctx, "*").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(flushAllResult2) // >>> []
	// STEP_END

	// Output:
	// OK
	// []
}

func ExampleClient_cmd_info() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// STEP_START info
	infoResult, err := rdb.Info(ctx).Result()

	if err != nil {
		panic(err)
	}

	// Check the first 8 characters (the full info string contains
	// much more text than this).
	fmt.Println(infoResult[:8]) // >>> # Server
	// STEP_END

	// Output:
	// # Server
}
