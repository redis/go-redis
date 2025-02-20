// EXAMPLE: cms_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_cms() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "bikes:profit")
	// REMOVE_END

	// STEP_START cms
	res1, err := rdb.CMSInitByProb(ctx, "bikes:profit", 0.001, 0.002).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.CMSIncrBy(ctx, "bikes:profit",
		"Smoky Mountain Striker", 100,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> [100]

	res3, err := rdb.CMSIncrBy(ctx, "bikes:profit",
		"Rocky Mountain Racer", 200,
		"Cloudy City Cruiser", 150,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> [200 150]

	res4, err := rdb.CMSQuery(ctx, "bikes:profit",
		"Smoky Mountain Striker",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> [100]

	res5, err := rdb.CMSInfo(ctx, "bikes:profit").Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Width: %v, Depth: %v, Count: %v",
		res5.Width, res5.Depth, res5.Count)
	// >>> Width: 2000, Depth: 9, Count: 450
	// STEP_END

	// Output:
	// OK
	// [100]
	// [200 150]
	// [100]
	// Width: 2000, Depth: 9, Count: 450
}
