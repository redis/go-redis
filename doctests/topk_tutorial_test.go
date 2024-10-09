// EXAMPLE: topk_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_topk() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:keywords")
	// REMOVE_END

	// STEP_START topk
	res1, err := rdb.TopKReserve(ctx, "bikes:keywords", 5).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.TopKAdd(ctx, "bikes:keywords",
		"store",
		"seat",
		"handlebars",
		"handles",
		"pedals",
		"tires",
		"store",
		"seat",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> [     handlebars  ]

	res3, err := rdb.TopKList(ctx, "bikes:keywords").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // [store seat pedals tires handles]

	res4, err := rdb.TopKQuery(ctx, "bikes:keywords", "store", "handlebars").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // [true false]
	// STEP_END

	// Output:
	// OK
	// [     handlebars  ]
	// [store seat pedals tires handles]
	// [true false]
}
