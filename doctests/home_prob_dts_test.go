// EXAMPLE: home_prob_dts
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_probabilistic_datatypes() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.FlushDB(ctx)
	rdb.Del(ctx,
		"recorded_users", "other_users",
		"group:1", "group:2", "both_groups",
		"items_sold",
		"male_heights", "female_heights", "all_heights",
		"top_3_songs")
	// REMOVE_END

	// STEP_START bloom
	res1, err := rdb.BFMAdd(
		ctx,
		"recorded_users",
		"andy", "cameron", "david", "michelle",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> [true true true true]

	res2, err := rdb.BFExists(ctx,
		"recorded_users", "cameron",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> true

	res3, err := rdb.BFExists(ctx, "recorded_users", "kaitlyn").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> false
	// STEP_END

	// STEP_START cuckoo
	res4, err := rdb.CFAdd(ctx, "other_users", "paolo").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> true

	res5, err := rdb.CFAdd(ctx, "other_users", "kaitlyn").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> true

	res6, err := rdb.CFAdd(ctx, "other_users", "rachel").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> true

	res7, err := rdb.CFMExists(ctx,
		"other_users", "paolo", "rachel", "andy",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> [true true false]

	res8, err := rdb.CFDel(ctx, "other_users", "paolo").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> true

	res9, err := rdb.CFExists(ctx, "other_users", "paolo").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> false
	// STEP_END

	// STEP_START hyperloglog
	res10, err := rdb.PFAdd(
		ctx,
		"group:1",
		"andy", "cameron", "david",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> 1

	res11, err := rdb.PFCount(ctx, "group:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> 3

	res12, err := rdb.PFAdd(ctx,
		"group:2",
		"kaitlyn", "michelle", "paolo", "rachel",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> 1

	res13, err := rdb.PFCount(ctx, "group:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13) // >>> 4

	res14, err := rdb.PFMerge(
		ctx,
		"both_groups",
		"group:1", "group:2",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14) // >>> OK

	res15, err := rdb.PFCount(ctx, "both_groups").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res15) // >>> 7
	// STEP_END

	// STEP_START cms
	// Specify that you want to keep the counts within 0.01
	// (0.1%) of the true value with a 0.005 (0.05%) chance
	// of going outside this limit.
	res16, err := rdb.CMSInitByProb(ctx, "items_sold", 0.01, 0.005).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res16) // >>> OK

	// The parameters for `CMSIncrBy()` are two lists. The count
	// for each item in the first list is incremented by the
	// value at the same index in the second list.
	res17, err := rdb.CMSIncrBy(ctx, "items_sold",
		"bread", 300,
		"tea", 200,
		"coffee", 200,
		"beer", 100,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res17) // >>> [300 200 200 100]

	res18, err := rdb.CMSIncrBy(ctx, "items_sold",
		"bread", 100,
		"coffee", 150,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res18) // >>> [400 350]

	res19, err := rdb.CMSQuery(ctx,
		"items_sold",
		"bread", "tea", "coffee", "beer",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res19) // >>> [400 200 350 100]
	// STEP_END

	// STEP_START tdigest
	res20, err := rdb.TDigestCreate(ctx, "male_heights").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> OK

	res21, err := rdb.TDigestAdd(ctx, "male_heights",
		175.5, 181, 160.8, 152, 177, 196, 164,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res21) // >>> OK

	res22, err := rdb.TDigestMin(ctx, "male_heights").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(res22) // >>> 152

	res23, err := rdb.TDigestMax(ctx, "male_heights").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res23) // >>> 196

	res24, err := rdb.TDigestQuantile(ctx, "male_heights", 0.75).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res24) // >>> [181]

	// Note that the CDF value for 181 is not exactly
	// 0.75. Both values are estimates.
	res25, err := rdb.TDigestCDF(ctx, "male_heights", 181).Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("%.4f\n", res25[0]) // >>> 0.7857

	res26, err := rdb.TDigestCreate(ctx, "female_heights").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res26) // >>> OK

	res27, err := rdb.TDigestAdd(ctx, "female_heights",
		155.5, 161, 168.5, 170, 157.5, 163, 171,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res27) // >>> OK

	res28, err := rdb.TDigestQuantile(ctx, "female_heights", 0.75).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res28) // >>> [170]

	res29, err := rdb.TDigestMerge(ctx, "all_heights",
		nil,
		"male_heights", "female_heights",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res29) // >>> OK

	res30, err := rdb.TDigestQuantile(ctx, "all_heights", 0.75).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res30) // >>> [175.5]
	// STEP_END

	// STEP_START topk
	// Create a TopK filter that keeps track of the top 3 items
	res31, err := rdb.TopKReserve(ctx, "top_3_songs", 3).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res31) // >>> OK

	// Add some items to the filter
	res32, err := rdb.TopKIncrBy(ctx,
		"top_3_songs",
		"Starfish Trooper", 3000,
		"Only one more time", 1850,
		"Rock me, Handel", 1325,
		"How will anyone know?", 3890,
		"Average lover", 4098,
		"Road to everywhere", 770,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res32)
	// >>> [   Rock me, Handel Only one more time ]

	res33, err := rdb.TopKList(ctx, "top_3_songs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res33)
	// >>> [Average lover How will anyone know? Starfish Trooper]

	// Query the count for specific items
	res34, err := rdb.TopKQuery(
		ctx,
		"top_3_songs",
		"Starfish Trooper", "Road to everywhere",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res34) // >>> [true false]
	// STEP_END

	// Output:
	// [true true true true]
	// true
	// false
	// true
	// true
	// true
	// [true true false]
	// true
	// false
	// 1
	// 3
	// 1
	// 4
	// OK
	// 7
	// OK
	// [300 200 200 100]
	// [400 350]
	// [400 200 350 100]
	// OK
	// OK
	// 152
	// 196
	// [181]
	// 0.7857
	// OK
	// OK
	// [170]
	// OK
	// [175.5]
	// OK
	// [   Rock me, Handel Only one more time ]
	// [Average lover How will anyone know? Starfish Trooper]
	// [true false]
}
