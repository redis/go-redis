// EXAMPLE: time_series_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_timeseries_create() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "thermometer:1", "thermometer:2", "thermometer:3")
	// REMOVE_END

	// STEP_START create
	res1, err := rdb.TSCreate(ctx, "thermometer:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.Type(ctx, "thermometer:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> TSDB-TYPE

	res3, err := rdb.TSInfo(ctx, "thermometer:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3["totalSamples"]) // >>> 0
	// STEP_END

	// STEP_START create_retention
	res4, err := rdb.TSAddWithArgs(
		ctx,
		"thermometer:2",
		1, 10.8,
		&redis.TSOptions{
			Retention: 100,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> 1

	res5, err := rdb.TSInfo(ctx, "thermometer:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5["retentionTime"]) // >>> 100
	// STEP_END

	// STEP_START create_labels
	res6, err := rdb.TSAddWithArgs(
		ctx,
		"thermometer:3",
		1, 10.4,
		&redis.TSOptions{
			Labels: map[string]string{"location": "UK", "type": "Mercury"},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> 1

	res7, err := rdb.TSInfo(ctx, "thermometer:3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7["labels"])
	// >>> map[location:UK type:Mercury]
	// STEP_END

	// Output:
	// OK
	// TSDB-TYPE
	// 0
	// 1
	// 100
	// 1
	// map[location:UK type:Mercury]
}

func ExampleClient_timeseries_add() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "thermometer:1", "thermometer:2")
	rdb.TSCreate(ctx, "thermometer:1")
	rdb.TSCreate(ctx, "thermometer:2")
	// REMOVE_END

	// STEP_START madd
	res1, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"thermometer:1", 1, 9.2},
		{"thermometer:1", 2, 9.9},
		{"thermometer:2", 2, 10.3},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> [1 2 2]
	// STEP_END

	// STEP_START get
	// The last recorded temperature for thermometer:2
	// was 10.3 at time 2.
	res2, err := rdb.TSGet(ctx, "thermometer:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2)
	// >>> {2 10.3}
	// STEP_END

	// Output:
	// [1 2 2]
	// {2 10.3}
}

func ExampleClient_timeseries_range() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "rg:1")
	// REMOVE_END

	// STEP_START range
	// Add 5 data points to a time series named "rg:1".
	res1, err := rdb.TSCreate(ctx, "rg:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"rg:1", 0, 18},
		{"rg:1", 1, 14},
		{"rg:1", 2, 22},
		{"rg:1", 3, 18},
		{"rg:1", 4, 24},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> [0 1 2 3 4]

	// Retrieve all the data points in ascending order.
	// Note: use 0 and `math.MaxInt64` instead of - and +
	// to denote the minimum and maximum possible timestamps.
	res3, err := rdb.TSRange(ctx, "rg:1",
		0, math.MaxInt64,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3)
	// >>> [{0 18} {1 14} {2 22} {3 18} {4 24}]

	// Retrieve data points up to time 1 (inclusive).
	res4, err := rdb.TSRange(ctx, "rg:1", 0, 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4)
	// >>> [{0 18} {1 14}]

	// Retrieve data points from time 3 onwards.
	res5, err := rdb.TSRange(ctx, "rg:1", 3, math.MaxInt64).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5)
	// >>> [{3 18} {4 24}]

	// Retrieve all the data points in descending order.
	res6, err := rdb.TSRevRange(ctx, "rg:1", 0, math.MaxInt64).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6)
	// >>> [{4 24} {3 18} {2 22} {1 14} {0 18}]

	// Retrieve data points up to time 1 (inclusive), but return them
	// in descending order.
	res7, err := rdb.TSRevRange(ctx, "rg:1", 0, 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7)
	// >>> [{1 14} {0 18}]
	// STEP_END

	// STEP_START range_filter
	res8, err := rdb.TSRangeWithArgs(
		ctx,
		"rg:1",
		0, math.MaxInt64,
		&redis.TSRangeOptions{
			FilterByTS: []int{0, 2, 4},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> [{0 18} {2 22} {4 24}]

	res9, err := rdb.TSRevRangeWithArgs(
		ctx,
		"rg:1",
		0, math.MaxInt64,
		&redis.TSRevRangeOptions{
			FilterByTS:    []int{0, 2, 4},
			FilterByValue: []int{20, 25},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> [{4 24} {2 22}]

	res10, err := rdb.TSRevRangeWithArgs(
		ctx,
		"rg:1",
		0, math.MaxInt64,
		&redis.TSRevRangeOptions{
			FilterByTS:    []int{0, 2, 4},
			FilterByValue: []int{22, 22},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> [{2 22}]
	// STEP_END

	// Output:
	// OK
	// [0 1 2 3 4]
	// [{0 18} {1 14} {2 22} {3 18} {4 24}]
	// [{0 18} {1 14}]
	// [{3 18} {4 24}]
	// [{4 24} {3 18} {2 22} {1 14} {0 18}]
	// [{1 14} {0 18}]
	// [{0 18} {2 22} {4 24}]
	// [{4 24} {2 22}]
	// [{2 22}]
}

func ExampleClient_timeseries_query_multi() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "rg:2", "rg:3", "rg:4")
	// REMOVE_END

	// STEP_START query_multi
	// Create three new "rg:" time series (two in the US
	// and one in the UK, with different units) and add some
	// data points.
	res20, err := rdb.TSCreateWithArgs(ctx, "rg:2", &redis.TSOptions{
		Labels: map[string]string{"location": "us", "unit": "cm"},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> OK

	res21, err := rdb.TSCreateWithArgs(ctx, "rg:3", &redis.TSOptions{
		Labels: map[string]string{"location": "us", "unit": "in"},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res21) // >>> OK

	res22, err := rdb.TSCreateWithArgs(ctx, "rg:4", &redis.TSOptions{
		Labels: map[string]string{"location": "uk", "unit": "mm"},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res22) // >>> OK

	res23, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"rg:2", 0, 1.8},
		{"rg:3", 0, 0.9},
		{"rg:4", 0, 25},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res23) // >>> [0 0 0]

	res24, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"rg:2", 1, 2.1},
		{"rg:3", 1, 0.77},
		{"rg:4", 1, 18},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res24) // >>> [1 1 1]

	res25, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"rg:2", 2, 2.3},
		{"rg:3", 2, 1.1},
		{"rg:4", 2, 21},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res25) // >>> [2 2 2]

	res26, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"rg:2", 3, 1.9},
		{"rg:3", 3, 0.81},
		{"rg:4", 3, 19},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res26) // >>> [3 3 3]

	res27, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"rg:2", 4, 1.78},
		{"rg:3", 4, 0.74},
		{"rg:4", 4, 23},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res27) // >>> [4 4 4]

	// Retrieve the last data point from each US time series.
	res28, err := rdb.TSMGet(ctx, []string{"location=us"}).Result()

	if err != nil {
		panic(err)
	}

	res28Keys := slices.Collect(maps.Keys(res28))
	sort.Strings(res28Keys)

	for _, k := range res28Keys {
		labels := res28[k][0].(map[interface{}]interface{})

		labelKeys := make([]string, 0, len(labels))

		for lk := range labels {
			labelKeys = append(labelKeys, lk.(string))
		}

		sort.Strings(labelKeys)

		fmt.Printf("%v:\n", k)

		for _, lk := range labelKeys {
			fmt.Printf("  %v: %v\n", lk, labels[lk])
		}

		fmt.Printf("  %v\n", res28[k][1])
	}
	// >>> rg:2:
	// >>>   {4 1.78}
	// >>> rg:3:
	// >>>   {4 0.74}

	// Retrieve the same data points, but include the `unit`
	// label in the results.
	res29, err := rdb.TSMGetWithArgs(
		ctx,
		[]string{"location=us"},
		&redis.TSMGetOptions{
			SelectedLabels: []interface{}{"unit"},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	res29Keys := slices.Collect(maps.Keys(res29))
	sort.Strings(res29Keys)

	for _, k := range res29Keys {
		labels := res29[k][0].(map[interface{}]interface{})

		labelKeys := make([]string, 0, len(labels))

		for lk := range labels {
			labelKeys = append(labelKeys, lk.(string))
		}

		sort.Strings(labelKeys)

		fmt.Printf("%v:\n", k)

		for _, lk := range labelKeys {
			fmt.Printf("  %v: %v\n", lk, labels[lk])
		}

		fmt.Printf("  %v\n", res29[k][1])
	}

	// >>> rg:2:
	// >>>   unit: cm
	// >>>   [4 1.78]
	// >>> rg:3:
	// >>>   unit: in
	// >>>   [4 0.74]

	// Retrieve data points up to time 2 (inclusive) from all
	// time series that use millimeters as the unit. Include all
	// labels in the results.
	// Note that the `aggregators` field is empty if no
	// aggregators are specified.
	res30, err := rdb.TSMRangeWithArgs(
		ctx,
		0, 2,
		[]string{"unit=mm"},
		&redis.TSMRangeOptions{
			WithLabels: true,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	res30Keys := slices.Collect(maps.Keys(res30))
	sort.Strings(res30Keys)

	for _, k := range res30Keys {
		labels := res30[k][0].(map[interface{}]interface{})
		labelKeys := make([]string, 0, len(labels))

		for lk := range labels {
			labelKeys = append(labelKeys, lk.(string))
		}

		sort.Strings(labelKeys)

		fmt.Printf("%v:\n", k)

		for _, lk := range labelKeys {
			fmt.Printf("  %v: %v\n", lk, labels[lk])
		}

		fmt.Printf("  Aggregators: %v\n", res30[k][1])
		fmt.Printf("  %v\n", res30[k][2])
	}
	// >>> rg:4:
	// >>>   location: uk
	// >>>   unit: mm
	// >>>   Aggregators: map[aggregators:[]]
	// >>>   [{0 25} {1 18} {2 21}]

	// Retrieve data points from time 1 to time 3 (inclusive) from
	// all time series that use centimeters or millimeters as the unit,
	// but only return the `location` label. Return the results
	// in descending order of timestamp.
	res31, err := rdb.TSMRevRangeWithArgs(
		ctx,
		1, 3,
		[]string{"unit=(cm,mm)"},
		&redis.TSMRevRangeOptions{
			SelectedLabels: []interface{}{"location"},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	res31Keys := slices.Collect(maps.Keys(res31))
	sort.Strings(res31Keys)

	for _, k := range res31Keys {
		labels := res31[k][0].(map[interface{}]interface{})
		labelKeys := make([]string, 0, len(labels))

		for lk := range labels {
			labelKeys = append(labelKeys, lk.(string))
		}

		sort.Strings(labelKeys)

		fmt.Printf("%v:\n", k)

		for _, lk := range labelKeys {
			fmt.Printf("  %v: %v\n", lk, labels[lk])
		}

		fmt.Printf("  Aggregators: %v\n", res31[k][1])
		fmt.Printf("  %v\n", res31[k][2])
	}
	// >>> rg:2:
	// >>>   location: us
	// >>>   Aggregators: map[aggregators:[]]
	// >>>   [{3 1.9} {2 2.3} {1 2.1}]
	// >>> rg:4:
	// >>>   location: uk
	// >>>   Aggregators: map[aggregators:[]]
	// >>>   [{3 19} {2 21} {1 18}]
	// STEP_END

	// Output:
	// OK
	// OK
	// OK
	// [0 0 0]
	// [1 1 1]
	// [2 2 2]
	// [3 3 3]
	// [4 4 4]
	// rg:2:
	//   [4 1.78]
	// rg:3:
	//   [4 0.74]
	// rg:2:
	//   unit: cm
	//   [4 1.78]
	// rg:3:
	//   unit: in
	//   [4 0.74]
	// rg:4:
	//   location: uk
	//   unit: mm
	//   Aggregators: map[aggregators:[]]
	//   [[0 25] [1 18] [2 21]]
	// rg:2:
	//   location: us
	//   Aggregators: map[aggregators:[]]
	//   [[3 1.9] [2 2.3] [1 2.1]]
	// rg:4:
	//   location: uk
	//   Aggregators: map[aggregators:[]]
	//   [[3 19] [2 21] [1 18]]
}

func ExampleClient_timeseries_aggregation() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "rg:2")
	// REMOVE_END

	// Setup data for aggregation example
	_, err := rdb.TSCreateWithArgs(ctx, "rg:2", &redis.TSOptions{
		Labels: map[string]string{"location": "us", "unit": "cm"},
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.TSMAdd(ctx, [][]interface{}{
		{"rg:2", 0, 1.8},
		{"rg:2", 1, 2.1},
		{"rg:2", 2, 2.3},
		{"rg:2", 3, 1.9},
		{"rg:2", 4, 1.78},
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START agg
	res32, err := rdb.TSRangeWithArgs(
		ctx,
		"rg:2",
		0, math.MaxInt64,
		&redis.TSRangeOptions{
			Aggregator:     redis.Avg,
			BucketDuration: 2,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res32)
	// >>> [{0 1.9500000000000002} {2 2.0999999999999996} {4 1.78}]
	// STEP_END

	// Output:
	// [{0 1.9500000000000002} {2 2.0999999999999996} {4 1.78}]
}
func ExampleClient_timeseries_agg_bucket() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "sensor3")
	// REMOVE_END

	// STEP_START agg_bucket
	res1, err := rdb.TSCreate(ctx, "sensor3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.TSMAdd(ctx, [][]interface{}{
		{"sensor3", 10, 1000},
		{"sensor3", 20, 2000},
		{"sensor3", 30, 3000},
		{"sensor3", 40, 4000},
		{"sensor3", 50, 5000},
		{"sensor3", 60, 6000},
		{"sensor3", 70, 7000},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> [10 20 30 40 50 60 70]

	res3, err := rdb.TSRangeWithArgs(
		ctx,
		"sensor3",
		10, 70,
		&redis.TSRangeOptions{
			Aggregator:     redis.Min,
			BucketDuration: 25,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> [{0 1000} {25 3000} {50 5000}]

	res4, err := rdb.TSRangeWithArgs(
		ctx,
		"sensor3",
		10, 70,
		&redis.TSRangeOptions{
			Aggregator:     redis.Min,
			BucketDuration: 25,
			Align:          "START",
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> [{10 1000} {35 4000} {60 6000}]
	// STEP_END

	// Output:
	// OK
	// [10 20 30 40 50 60 70]
	// [{0 1000} {25 3000} {50 5000}]
	// [{10 1000} {35 4000} {60 6000}]
}

func ExampleClient_timeseries_downsampling() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "wind:1", "wind:2", "wind:3", "wind:4")
	// REMOVE_END

	// STEP_START downsampling
	res1, err := rdb.TSCreate(ctx, "wind:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.TSCreate(ctx, "wind:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> OK

	res3, err := rdb.TSCreateRule(ctx, "wind:1", "wind:2", redis.Avg, 3600000).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> OK

	res4, err := rdb.TSCreate(ctx, "wind:3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> OK

	res5, err := rdb.TSCreateRule(ctx, "wind:1", "wind:3", redis.Max, 3600000).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> OK

	res6, err := rdb.TSCreate(ctx, "wind:4").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> OK

	res7, err := rdb.TSCreateRule(ctx, "wind:1", "wind:4", redis.Min, 3600000).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(res7) // >>> OK

	samples := [][]interface{}{
		{"wind:1", 1000, 15},
		{"wind:1", 1500, 16},
		{"wind:1", 2000, 17},
		{"wind:1", 2500, 14},
		{"wind:1", 3000, 13},
		{"wind:1", 3600000 + 1000, 20},
		{"wind:1", 3600000 + 1500, 22},
		{"wind:1", 3600000 + 2000, 24},
		{"wind:1", 3600000 + 2500, 19},
		{"wind:1", 3600000 + 3000, 18},
	}

	_, err = rdb.TSMAdd(ctx, samples).Result()

	if err != nil {
		panic(err)
	}

	res8, err := rdb.TSRange(ctx, "wind:2", math.MinInt64, math.MaxInt64).Result()

	if err != nil {
		panic(err)
	}

	fmt.Print("Hourly average wind speed: ")
	for _, dp := range res8 {
		fmt.Printf("(%.0f, %.1f) ", float64(dp.Timestamp), dp.Value)
	}
	fmt.Println() // >>> Hourly average wind speed: (0, 15.0) (3600000, 20.6)

	res9, err := rdb.TSRange(ctx, "wind:3", math.MinInt64, math.MaxInt64).Result()
	if err != nil {
		panic(err)
	}
	fmt.Print("Hourly maximum wind speed: ")
	for _, dp := range res9 {
		fmt.Printf("(%.0f, %.1f) ", float64(dp.Timestamp), dp.Value)
	}
	fmt.Println() // >>> Hourly maximum wind speed: (0, 17.0) (3600000, 24.0)

	res10, err := rdb.TSRange(ctx, "wind:4", math.MinInt64, math.MaxInt64).Result()
	if err != nil {
		panic(err)
	}
	fmt.Print("Hourly minimum wind speed: ")
	for _, dp := range res10 {
		fmt.Printf("(%.0f, %.1f) ", float64(dp.Timestamp), dp.Value)
	}
	fmt.Println() // >>> Hourly minimum wind speed: (0, 13.0) (3600000, 18.0)
	// STEP_END

	// Output:
	// OK
	// OK
	// OK
	// OK
	// OK
	// OK
	// OK
	// Hourly average wind speed: (0, 15.0) (3600000, 20.6)
	// Hourly maximum wind speed: (0, 17.0) (3600000, 24.0)
	// Hourly minimum wind speed: (0, 13.0) (3600000, 18.0)
}

func ExampleClient_timeseries_compaction() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "hyg:1", "hyg:compacted")
	// REMOVE_END

	// STEP_START compaction
	res1, err := rdb.TSCreate(ctx, "hyg:1").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(res1) // >>> OK

	res2, err := rdb.TSCreate(ctx, "hyg:compacted").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(res2) // >>> OK

	res3, err := rdb.TSCreateRuleWithArgs(ctx, "hyg:1", "hyg:compacted", redis.Twa, 60000, &redis.TSCreateRuleOptions{}).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(res3) // >>> OK

	samples := [][]interface{}{
		{"hyg:1", 1000, 70},
		{"hyg:1", 15000, 72},
		{"hyg:1", 29000, 75},
		{"hyg:1", 40000, 68},
		{"hyg:1", 55000, 65},
		{"hyg:1", 60000 + 1000, 63},
		{"hyg:1", 60000 + 15000, 60},
		{"hyg:1", 60000 + 29000, 58},
		{"hyg:1", 60000 + 40000, 62},
		{"hyg:1", 60000 + 55000, 65},
	}

	_, err = rdb.TSMAdd(ctx, samples).Result()
	if err != nil {
		panic(err)
	}

	res4, err := rdb.TSRange(ctx, "hyg:compacted", math.MinInt64, math.MaxInt64).Result()
	if err != nil {
		panic(err)
	}
	fmt.Print("Compacted humidity data (time-weighted average): ")
	for _, dp := range res4 {
		fmt.Printf("(%.0f, %.1f) ", float64(dp.Timestamp), dp.Value)
	}
	fmt.Println() // >>> Compacted humidity data (time-weighted average): (0, 70.5) (60000, 61.5)
	// STEP_END

	// Output:
	// OK
	// OK
	// OK
	// Compacted humidity data (time-weighted average): (0, 70.5) (60000, 61.5)
}
