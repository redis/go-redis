// EXAMPLE: tdigest_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_tdigstart() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "racer_ages", "bikes:sales")
	// REMOVE_END

	// STEP_START tdig_start
	res1, err := rdb.TDigestCreate(ctx, "bikes:sales").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.TDigestAdd(ctx, "bikes:sales", 21).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> OK

	res3, err := rdb.TDigestAdd(ctx, "bikes:sales",
		150, 95, 75, 34,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> OK

	// STEP_END

	// Output:
	// OK
	// OK
	// OK
}

func ExampleClient_tdigcdf() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "racer_ages", "bikes:sales")
	// REMOVE_END

	// STEP_START tdig_cdf
	res4, err := rdb.TDigestCreate(ctx, "racer_ages").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> OK

	res5, err := rdb.TDigestAdd(ctx, "racer_ages",
		45.88, 44.2, 58.03, 19.76, 39.84, 69.28,
		50.97, 25.41, 19.27, 85.71, 42.63,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> OK

	res6, err := rdb.TDigestRank(ctx, "racer_ages", 50).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> [7]

	res7, err := rdb.TDigestRank(ctx, "racer_ages", 50, 40).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> [7 4]
	// STEP_END

	// Output:
	// OK
	// OK
	// [7]
	// [7 4]
}

func ExampleClient_tdigquant() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "racer_ages")
	// REMOVE_END

	_, err := rdb.TDigestCreate(ctx, "racer_ages").Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.TDigestAdd(ctx, "racer_ages",
		45.88, 44.2, 58.03, 19.76, 39.84, 69.28,
		50.97, 25.41, 19.27, 85.71, 42.63,
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START tdig_quant
	res8, err := rdb.TDigestQuantile(ctx, "racer_ages", 0.5).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> [44.2]

	res9, err := rdb.TDigestByRank(ctx, "racer_ages", 4).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> [42.63]
	// STEP_END

	// Output:
	// [44.2]
	// [42.63]
}

func ExampleClient_tdigmin() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "racer_ages")
	// REMOVE_END

	_, err := rdb.TDigestCreate(ctx, "racer_ages").Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.TDigestAdd(ctx, "racer_ages",
		45.88, 44.2, 58.03, 19.76, 39.84, 69.28,
		50.97, 25.41, 19.27, 85.71, 42.63,
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START tdig_min
	res10, err := rdb.TDigestMin(ctx, "racer_ages").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> 19.27

	res11, err := rdb.TDigestMax(ctx, "racer_ages").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> 85.71
	// STEP_END

	// Output:
	// 19.27
	// 85.71
}

func ExampleClient_tdigreset() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "racer_ages")
	// REMOVE_END
	_, err := rdb.TDigestCreate(ctx, "racer_ages").Result()

	if err != nil {
		panic(err)
	}

	// STEP_START tdig_reset
	res12, err := rdb.TDigestReset(ctx, "racer_ages").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> OK
	// STEP_END

	// Output:
	// OK
}
