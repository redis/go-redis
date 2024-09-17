// EXAMPLE: stream_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

// REMOVE_START
func UNUSED(v ...interface{}) {}

// REMOVE_END

func ExampleClient_xadd() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	// STEP_START xadd
	res1, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res1) // >>> 1692632086370-0

	res2, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.PrintLn(res2) // >>> 1692632094485-0

	res3, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res3) // >>> 1692632102976-0
	// STEP_END

	// REMOVE_START
	UNUSED(res1, res2, res3)
	// REMOVE_END

	xlen, err := rdb.XLen(ctx, "race:france").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(xlen) // >>> 3

	// Output:
	// 3
}

func ExampleClient_xrange() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xrange
	res4, err := rdb.XRangeN(ctx, "race:france", "1691765278160-0", "+", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4)
	// >>> [{1692632086370-0 map[location_id:1 position:1 rider:Castilla...
	// STEP_END

	// Output:
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]
}

func ExampleClient_xreadblock() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END
	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xread_block
	res5, err := rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{"race:france", "0"},
		Count:   100,
		Block:   300,
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5)
	// >>> // [{race:france [{1692632086370-0 map[location_id:1 position:1...
	// STEP_END

	// Output:
	// [{race:france [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]} {1692632102976-0 map[location_id:1 position:2 rider:Prickett speed:29.7]}]}]
}

func ExampleClient_xadd2() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	// STEP_START xadd_2
	res6, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
	}).Result()

	if err != nil {
		panic(err)
	}

	//fmt.Println(res6) // >>> 1692632147973-0
	// STEP_END

	// REMOVE_START
	UNUSED(res6)
	// REMOVE_END

	xlen, err := rdb.XLen(ctx, "race:france").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(xlen) // >>> 1
	// Output:
	// 1
}

func ExampleClient_xlen() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}
	// STEP_START xlen
	res7, err := rdb.XLen(ctx, "race:france").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> 4
	// STEP_END

	// Output:
	// 4
}

func ExampleClient_xaddid() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:usa")
	// REMOVE_END

	// STEP_START xadd_id
	res8, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:usa",
		Values: map[string]interface{}{
			"racer": "Castilla",
		},
		ID: "0-1",
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> 0-1

	res9, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:usa",
		Values: map[string]interface{}{
			"racer": "Norem",
		},
		ID: "0-2",
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> 0-2
	// STEP_END

	// Output:
	// 0-1
	// 0-2
}

func ExampleClient_xaddbadid() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:usa")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:usa",
		Values: map[string]interface{}{

			"racer": "Castilla",
		},
		ID: "0-1",
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xadd_bad_id
	res10, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Values: map[string]interface{}{
			"racer": "Prickett",
		},
		ID: "0-1",
	}).Result()

	if err != nil {
		fmt.Println(err)
		// >>> ERR The ID specified in XADD is equal or smaller than the target stream top item
	}

	fmt.Println(res10)
	// STEP_END

	// Output:
	// ERR The ID specified in XADD is equal or smaller than the target stream top item
}

func ExampleClient_xadd7() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:usa")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:usa",
		Values: map[string]interface{}{
			"racer": "Castilla",
		},
		ID: "0-1",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:usa",
		Values: map[string]interface{}{
			"racer": "Norem",
		},
		ID: "0-2",
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xadd_7
	res11, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:usa",
		Values: map[string]interface{}{
			"racer": "Prickett",
		},
		ID: "0-*",
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> 0-3
	// STEP_END

	// Output:
	// 0-3
}

func ExampleClient_xrangeall() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}
	// STEP_START xrange_all
	res12, err := rdb.XRange(ctx, "race:france", "-", "+").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12)
	// >>> [{1692632086370-0 map[location_id:1 position:1 rider:Castilla...
	// STEP_END

	// Output:
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]} {1692632102976-0 map[location_id:1 position:2 rider:Prickett speed:29.7]} {1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
}

func ExampleClient_xrangetime() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}
	// STEP_START xrange_time
	res13, err := rdb.XRange(ctx, "race:france",
		"1692632086369", "1692632086371",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13)
	// >>> [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]}]
	// STEP_END

	// Output:
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]}]
}

func ExampleClient_xrangestep1() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}
	// STEP_START xrange_step_1
	res14, err := rdb.XRangeN(ctx, "race:france", "-", "+", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14)
	// >>> [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]
	// STEP_END

	// Output:
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]
}

func ExampleClient_xrangestep2() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}
	// STEP_START xrange_step_2
	res15, err := rdb.XRangeN(ctx, "race:france",
		"(1692632094485-0", "+", 2,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res15)
	// >>> [{1692632102976-0 map[location_id:1 position:2 rider:Prickett speed:29.7]} {1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
	// STEP_END

	// Output:
	// [{1692632102976-0 map[location_id:1 position:2 rider:Prickett speed:29.7]} {1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
}

func ExampleClient_xrangeempty() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xrange_empty
	res16, err := rdb.XRangeN(ctx, "race:france",
		"(1692632147973-0", "+", 2,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res16)
	// >>> []
	// STEP_END

	// Output:
	// []
}

func ExampleClient_xrevrange() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xrevrange
	res17, err := rdb.XRevRangeN(ctx, "race:france", "+", "-", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res17)
	// >>> [{1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
	// STEP_END

	// Output:
	// [{1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
}

func ExampleClient_xread() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:france")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       30.2,
			"position":    1,
			"location_id": 1,
		},
		ID: "1692632086370-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Norem",
			"speed":       28.8,
			"position":    3,
			"location_id": 1,
		},
		ID: "1692632094485-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Prickett",
			"speed":       29.7,
			"position":    2,
			"location_id": 1,
		},
		ID: "1692632102976-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:france",
		Values: map[string]interface{}{
			"rider":       "Castilla",
			"speed":       29.9,
			"position":    1,
			"location_id": 2,
		},
		ID: "1692632147973-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xread
	res18, err := rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{"race:france", "0"},
		Count:   2,
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res18)
	// >>> [{race:france [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]}]
	// STEP_END

	// Output:
	// [{race:france [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]}]
}
