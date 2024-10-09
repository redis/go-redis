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

func ExampleClient_racefrance1() {
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

	// STEP_START xlen
	res7, err := rdb.XLen(ctx, "race:france").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> 4
	// STEP_END

	// REMOVE_START
	UNUSED(res6)
	// REMOVE_END

	// Output:
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]
	// [{race:france [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]} {1692632102976-0 map[location_id:1 position:2 rider:Prickett speed:29.7]}]}]
	// 4
}

func ExampleClient_raceusa() {
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

	// STEP_START xadd_bad_id
	res10, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Values: map[string]interface{}{
			"racer": "Prickett",
		},
		ID: "0-1",
	}).Result()

	if err != nil {
		// fmt.Println(err)
		// >>> ERR The ID specified in XADD is equal or smaller than the target stream top item
	}
	// STEP_END

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

	// REMOVE_START
	UNUSED(res10)
	// REMOVE_END

	// Output:
	// 0-1
	// 0-2
	// 0-3
}

func ExampleClient_racefrance2() {
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

	// STEP_START xrange_step_1
	res14, err := rdb.XRangeN(ctx, "race:france", "-", "+", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14)
	// >>> [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]
	// STEP_END

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

	// STEP_START xrevrange
	res17, err := rdb.XRevRangeN(ctx, "race:france", "+", "-", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res17)
	// >>> [{1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
	// STEP_END

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
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]} {1692632102976-0 map[location_id:1 position:2 rider:Prickett speed:29.7]} {1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]}]
	// [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]
	// [{1692632102976-0 map[location_id:1 position:2 rider:Prickett speed:29.7]} {1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
	// []
	// [{1692632147973-0 map[location_id:2 position:1 rider:Castilla speed:29.9]}]
	// [{race:france [{1692632086370-0 map[location_id:1 position:1 rider:Castilla speed:30.2]} {1692632094485-0 map[location_id:1 position:3 rider:Norem speed:28.8]}]}]
}

func ExampleClient_xgroupcreate() {
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

	// STEP_START xgroup_create
	res19, err := rdb.XGroupCreate(ctx, "race:france", "france_riders", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res19) // >>> OK
	// STEP_END

	// Output:
	// OK
}

func ExampleClient_xgroupcreatemkstream() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:italy")
	// REMOVE_END

	// STEP_START xgroup_create_mkstream
	res20, err := rdb.XGroupCreateMkStream(ctx,
		"race:italy", "italy_riders", "$",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> OK
	// STEP_END

	// Output:
	// OK
}

func ExampleClient_xgroupread() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:italy")
	// REMOVE_END

	_, err := rdb.XGroupCreateMkStream(ctx,
		"race:italy", "italy_riders", "$",
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xgroup_read
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Castilla"},
	}).Result()
	// >>> 1692632639151-0

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Royce"},
	}).Result()
	// >>> 1692632647899-0

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Sam-Bodden"},
	}).Result()
	// >>> 1692632662819-0

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Prickett"},
	}).Result()
	// >>> 1692632670501-0

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Norem"},
	}).Result()
	// >>> 1692632678249-0

	if err != nil {
		panic(err)
	}

	// fmt.Println(res25)

	res21, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{"race:italy", ">"},
		Group:    "italy_riders",
		Consumer: "Alice",
		Count:    1,
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res21)
	// >>> [{race:italy [{1692632639151-0 map[rider:Castilla]}]}]
	// STEP_END

	// REMOVE_START
	UNUSED(res21)
	// REMOVE_END

	xlen, err := rdb.XLen(ctx, "race:italy").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(xlen)

	// Output:
	// 5
}

func ExampleClient_raceitaly() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:italy")
	rdb.XGroupDestroy(ctx, "race:italy", "italy_riders")
	// REMOVE_END

	_, err := rdb.XGroupCreateMkStream(ctx,
		"race:italy", "italy_riders", "$",
	).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Castilla"},
		ID:     "1692632639151-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Royce"},
		ID:     "1692632647899-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Sam-Bodden"},
		ID:     "1692632662819-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Prickett"},
		ID:     "1692632670501-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		Values: map[string]interface{}{"rider": "Norem"},
		ID:     "1692632678249-0",
	}).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{"race:italy", ">"},
		Group:    "italy_riders",
		Consumer: "Alice",
		Count:    1,
	}).Result()

	if err != nil {
		panic(err)
	}
	// STEP_START xgroup_read_id
	res22, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{"race:italy", "0"},
		Group:    "italy_riders",
		Consumer: "Alice",
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res22)
	// >>> [{race:italy [{1692632639151-0 map[rider:Castilla]}]}]
	// STEP_END

	// STEP_START xack
	res23, err := rdb.XAck(ctx,
		"race:italy", "italy_riders", "1692632639151-0",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res23) // >>> 1

	res24, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{"race:italy", "0"},
		Group:    "italy_riders",
		Consumer: "Alice",
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res24)
	// >>> [{race:italy []}]
	// STEP_END

	// STEP_START xgroup_read_bob
	res25, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{"race:italy", ">"},
		Group:    "italy_riders",
		Consumer: "Bob",
		Count:    2,
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res25)
	// >>> [{race:italy [{1692632647899-0 map[rider:Royce]} {1692632662819-0 map[rider:Sam-Bodden]}]}]

	// STEP_END

	// STEP_START xpending
	res26, err := rdb.XPending(ctx, "race:italy", "italy_riders").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res26)
	// >>> &{2 1692632647899-0 1692632662819-0 map[Bob:2]}
	// STEP_END

	// STEP_START xpending_plus_minus
	res27, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "race:italy",
		Group:  "italy_riders",
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res27)
	// >>> [{1692632647899-0 Bob 0s 1} {1692632662819-0 Bob 0s 1}]
	// STEP_END

	// STEP_START xrange_pending
	res28, err := rdb.XRange(ctx, "race:italy",
		"1692632647899-0", "1692632647899-0",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res28) // >>> [{1692632647899-0 map[rider:Royce]}]
	// STEP_END

	// STEP_START xclaim
	res29, err := rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   "race:italy",
		Group:    "italy_riders",
		Consumer: "Alice",
		MinIdle:  0,
		Messages: []string{"1692632647899-0"},
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res29)
	// STEP_END

	// STEP_START xautoclaim
	res30, res30a, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   "race:italy",
		Group:    "italy_riders",
		Consumer: "Alice",
		Start:    "0-0",
		Count:    1,
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res30)  // >>> [{1692632647899-0 map[rider:Royce]}]
	fmt.Println(res30a) // >>> 1692632662819-0
	// STEP_END

	// STEP_START xautoclaim_cursor
	res31, res31a, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   "race:italy",
		Group:    "italy_riders",
		Consumer: "Lora",
		Start:    "(1692632662819-0",
		Count:    1,
	}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res31)  // >>> []
	fmt.Println(res31a) // >>> 0-0
	// STEP_END

	// STEP_START xinfo
	res32, err := rdb.XInfoStream(ctx, "race:italy").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res32)
	// >>> &{5 1 2 1 1692632678249-0 0-0 5 {1692632639151-0 map[rider:Castilla]} {1692632678249-0 map[rider:Norem]} 1692632639151-0}
	// STEP_END

	// STEP_START xinfo_groups
	res33, err := rdb.XInfoGroups(ctx, "race:italy").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res33)
	// >>> [{italy_riders 3 2 1692632662819-0 3 2}]
	// STEP_END

	// STEP_START xinfo_consumers
	res34, err := rdb.XInfoConsumers(ctx, "race:italy", "italy_riders").Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res34)
	// >>> [{Alice 1 1ms 1ms} {Bob 1 2ms 2ms} {Lora 0 1ms -1ms}]
	// STEP_END

	// STEP_START maxlen
	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		MaxLen: 2,
		Values: map[string]interface{}{"rider": "Jones"},
	},
	).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		MaxLen: 2,
		Values: map[string]interface{}{"rider": "Wood"},
	},
	).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		MaxLen: 2,
		Values: map[string]interface{}{"rider": "Henshaw"},
	},
	).Result()

	if err != nil {
		panic(err)
	}

	res35, err := rdb.XLen(ctx, "race:italy").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res35) // >>> 2

	res36, err := rdb.XRange(ctx, "race:italy", "-", "+").Result()

	if err != nil {
		panic(err)
	}

	// fmt.Println(res36)
	// >>> [{1726649529170-1 map[rider:Wood]} {1726649529171-0 map[rider:Henshaw]}]
	// STEP_END

	// STEP_START xtrim
	res37, err := rdb.XTrimMaxLen(ctx, "race:italy", 10).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res37) // >>> 0
	// STEP_END

	// STEP_START xtrim2
	res38, err := rdb.XTrimMaxLenApprox(ctx, "race:italy", 10, 20).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res38) // >>> 0
	// STEP_END

	// REMOVE_START
	UNUSED(res27, res34, res36)
	// REMOVE_END

	// Output:
	// [{race:italy [{1692632639151-0 map[rider:Castilla]}]}]
	// 1
	// [{race:italy []}]
	// [{race:italy [{1692632647899-0 map[rider:Royce]} {1692632662819-0 map[rider:Sam-Bodden]}]}]
	// &{2 1692632647899-0 1692632662819-0 map[Bob:2]}
	// [{1692632647899-0 map[rider:Royce]}]
	// [{1692632647899-0 map[rider:Royce]}]
	// [{1692632647899-0 map[rider:Royce]}]
	// 1692632662819-0
	// []
	// 0-0
	// &{5 1 2 1 1692632678249-0 0-0 5 {1692632639151-0 map[rider:Castilla]} {1692632678249-0 map[rider:Norem]} 1692632639151-0}
	// [{italy_riders 3 2 1692632662819-0 3 2}]
	// 2
	// 0
	// 0
}

func ExampleClient_xdel() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "race:italy")
	// REMOVE_END

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		MaxLen: 2,
		Values: map[string]interface{}{"rider": "Wood"},
		ID:     "1692633198206-0",
	},
	).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "race:italy",
		MaxLen: 2,
		Values: map[string]interface{}{"rider": "Henshaw"},
		ID:     "1692633208557-0",
	},
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START xdel
	res39, err := rdb.XRangeN(ctx, "race:italy", "-", "+", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res39)
	// >>> [{1692633198206-0 map[rider:Wood]} {1692633208557-0 map[rider:Henshaw]}]

	res40, err := rdb.XDel(ctx, "race:italy", "1692633208557-0").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res40) // 1

	res41, err := rdb.XRangeN(ctx, "race:italy", "-", "+", 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res41)
	// >>> [{1692633198206-0 map[rider:Wood]}]
	// STEP_END

	// Output:
	// [{1692633198206-0 map[rider:Wood]} {1692633208557-0 map[rider:Henshaw]}]
	// 1
	// [{1692633198206-0 map[rider:Wood]}]
}
