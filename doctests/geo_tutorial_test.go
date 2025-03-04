// EXAMPLE: geo_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_geoadd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "bikes:rentable")
	// REMOVE_END

	// STEP_START geoadd
	res1, err := rdb.GeoAdd(ctx, "bikes:rentable",
		&redis.GeoLocation{
			Longitude: -122.27652,
			Latitude:  37.805186,
			Name:      "station:1",
		}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> 1

	res2, err := rdb.GeoAdd(ctx, "bikes:rentable",
		&redis.GeoLocation{
			Longitude: -122.2674626,
			Latitude:  37.8062344,
			Name:      "station:2",
		}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> 1

	res3, err := rdb.GeoAdd(ctx, "bikes:rentable",
		&redis.GeoLocation{
			Longitude: -122.2469854,
			Latitude:  37.8104049,
			Name:      "station:3",
		}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> 1
	// STEP_END

	// Output:
	// 1
	// 1
	// 1
}

func ExampleClient_geosearch() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "bikes:rentable")

	_, err := rdb.GeoAdd(ctx, "bikes:rentable",
		&redis.GeoLocation{
			Longitude: -122.27652,
			Latitude:  37.805186,
			Name:      "station:1",
		}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.GeoAdd(ctx, "bikes:rentable",
		&redis.GeoLocation{
			Longitude: -122.2674626,
			Latitude:  37.8062344,
			Name:      "station:2",
		}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.GeoAdd(ctx, "bikes:rentable",
		&redis.GeoLocation{
			Longitude: -122.2469854,
			Latitude:  37.8104049,
			Name:      "station:3",
		}).Result()

	if err != nil {
		panic(err)
	}
	// REMOVE_END

	// STEP_START geosearch
	res4, err := rdb.GeoSearch(ctx, "bikes:rentable",
		&redis.GeoSearchQuery{
			Longitude:  -122.27652,
			Latitude:   37.805186,
			Radius:     5,
			RadiusUnit: "km",
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> [station:1 station:2 station:3]
	// STEP_END

	// Output:
	// [station:1 station:2 station:3]
}
