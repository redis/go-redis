// EXAMPLE: set_and_get
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func ExampleSetGet() {
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

	err := rdb.Set(ctx, "bike:1", "Process 134", 0).Err()
	if err != nil {
		panic(err)
	}

	fmt.Println("OK")

	value, err := rdb.Get(ctx, "bike:1").Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("The name of the bike is %s", value)
	// HIDE_START

	// Output: OK
	// The name of the bike is Process 134
}

// HIDE_END
