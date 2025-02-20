// EXAMPLE: list_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_queue() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START queue
	res1, err := rdb.LPush(ctx, "bikes:repairs", "bike:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> 1

	res2, err := rdb.LPush(ctx, "bikes:repairs", "bike:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> 2

	res3, err := rdb.RPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> bike:1

	res4, err := rdb.RPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> bike:2
	// STEP_END

	// Output:
	// 1
	// 2
	// bike:1
	// bike:2
}

func ExampleClient_stack() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START stack
	res5, err := rdb.LPush(ctx, "bikes:repairs", "bike:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> 1

	res6, err := rdb.LPush(ctx, "bikes:repairs", "bike:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> 2

	res7, err := rdb.LPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> bike:2

	res8, err := rdb.LPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> bike:1
	// STEP_END

	// Output:
	// 1
	// 2
	// bike:2
	// bike:1
}

func ExampleClient_llen() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START llen
	res9, err := rdb.LLen(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> 0
	// STEP_END

	// Output:
	// 0
}

func ExampleClient_lmove_lrange() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	rdb.Del(ctx, "bikes:finished")
	// REMOVE_END

	// STEP_START lmove_lrange
	res10, err := rdb.LPush(ctx, "bikes:repairs", "bike:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> 1

	res11, err := rdb.LPush(ctx, "bikes:repairs", "bike:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> 2

	res12, err := rdb.LMove(ctx, "bikes:repairs", "bikes:finished", "LEFT", "LEFT").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> bike:2

	res13, err := rdb.LRange(ctx, "bikes:repairs", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13) // >>> [bike:1]

	res14, err := rdb.LRange(ctx, "bikes:finished", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14) // >>> [bike:2]
	// STEP_END

	// Output:
	// 1
	// 2
	// bike:2
	// [bike:1]
	// [bike:2]
}

func ExampleClient_lpush_rpush() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START lpush_rpush
	res15, err := rdb.RPush(ctx, "bikes:repairs", "bike:1").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res15) // >>> 1

	res16, err := rdb.RPush(ctx, "bikes:repairs", "bike:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res16) // >>> 2

	res17, err := rdb.LPush(ctx, "bikes:repairs", "bike:important_bike").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res17) // >>> 3

	res18, err := rdb.LRange(ctx, "bikes:repairs", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res18) // >>> [bike:important_bike bike:1 bike:2]
	// STEP_END

	// Output:
	// 1
	// 2
	// 3
	// [bike:important_bike bike:1 bike:2]
}

func ExampleClient_variadic() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START variadic
	res19, err := rdb.RPush(ctx, "bikes:repairs", "bike:1", "bike:2", "bike:3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res19) // >>> 3

	res20, err := rdb.LPush(ctx, "bikes:repairs", "bike:important_bike", "bike:very_important_bike").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> 5

	res21, err := rdb.LRange(ctx, "bikes:repairs", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res21) // >>> [bike:very_important_bike bike:important_bike bike:1 bike:2 bike:3]
	// STEP_END

	// Output:
	// 3
	// 5
	// [bike:very_important_bike bike:important_bike bike:1 bike:2 bike:3]
}

func ExampleClient_lpop_rpop() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START lpop_rpop
	res22, err := rdb.RPush(ctx, "bikes:repairs", "bike:1", "bike:2", "bike:3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res22) // >>> 3

	res23, err := rdb.RPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res23) // >>> bike:3

	res24, err := rdb.LPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res24) // >>> bike:1

	res25, err := rdb.RPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res25) // >>> bike:2

	res26, err := rdb.RPop(ctx, "bikes:repairs").Result()

	if err != nil {
		fmt.Println(err) // >>> redis: nil
	}

	fmt.Println(res26) // >>> <empty string>

	// STEP_END

	// Output:
	// 3
	// bike:3
	// bike:1
	// bike:2
	// redis: nil
	//
}

func ExampleClient_ltrim() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START ltrim
	res27, err := rdb.RPush(ctx, "bikes:repairs", "bike:1", "bike:2", "bike:3", "bike:4", "bike:5").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res27) // >>> 5

	res28, err := rdb.LTrim(ctx, "bikes:repairs", 0, 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res28) // >>> OK

	res29, err := rdb.LRange(ctx, "bikes:repairs", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res29) // >>> [bike:1 bike:2 bike:3]
	// STEP_END

	// Output:
	// 5
	// OK
	// [bike:1 bike:2 bike:3]
}

func ExampleClient_ltrim_end_of_list() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START ltrim_end_of_list
	res30, err := rdb.RPush(ctx, "bikes:repairs", "bike:1", "bike:2", "bike:3", "bike:4", "bike:5").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res30) // >>> 5

	res31, err := rdb.LTrim(ctx, "bikes:repairs", -3, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res31) // >>> OK

	res32, err := rdb.LRange(ctx, "bikes:repairs", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res32) // >>> [bike:3 bike:4 bike:5]
	// STEP_END

	// Output:
	// 5
	// OK
	// [bike:3 bike:4 bike:5]
}

func ExampleClient_brpop() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START brpop
	res33, err := rdb.RPush(ctx, "bikes:repairs", "bike:1", "bike:2").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res33) // >>> 2

	res34, err := rdb.BRPop(ctx, 1, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res34) // >>> [bikes:repairs bike:2]

	res35, err := rdb.BRPop(ctx, 1, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res35) // >>> [bikes:repairs bike:1]

	res36, err := rdb.BRPop(ctx, 1, "bikes:repairs").Result()

	if err != nil {
		fmt.Println(err) // >>> redis: nil
	}

	fmt.Println(res36) // >>> []
	// STEP_END

	// Output:
	// 2
	// [bikes:repairs bike:2]
	// [bikes:repairs bike:1]
	// redis: nil
	// []
}

func ExampleClient_rule1() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "new_bikes")
	// REMOVE_END

	// STEP_START rule_1
	res37, err := rdb.Del(ctx, "new_bikes").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res37) // >>> 0

	res38, err := rdb.LPush(ctx, "new_bikes", "bike:1", "bike:2", "bike:3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res38) // >>> 3
	// STEP_END

	// Output:
	// 0
	// 3
}

func ExampleClient_rule11() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "new_bikes")
	// REMOVE_END

	// STEP_START rule_1.1
	res39, err := rdb.Set(ctx, "new_bikes", "bike:1", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res39) // >>> OK

	res40, err := rdb.Type(ctx, "new_bikes").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res40) // >>> string

	res41, err := rdb.LPush(ctx, "new_bikes", "bike:2", "bike:3").Result()

	if err != nil {
		fmt.Println(err)
		// >>> WRONGTYPE Operation against a key holding the wrong kind of value
	}

	fmt.Println(res41)
	// STEP_END

	// Output:
	// OK
	// string
	// WRONGTYPE Operation against a key holding the wrong kind of value
	// 0
}

func ExampleClient_rule2() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START rule_2
	res42, err := rdb.LPush(ctx, "bikes:repairs", "bike:1", "bike:2", "bike:3").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res42) // >>> 3

	res43, err := rdb.Exists(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res43) // >>> 1

	res44, err := rdb.LPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res44) // >>> bike:3

	res45, err := rdb.LPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res45) // >>> bike:2

	res46, err := rdb.LPop(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res46) // >>> bike:1

	res47, err := rdb.Exists(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res47) // >>> 0
	// STEP_END

	// Output:
	// 3
	// 1
	// bike:3
	// bike:2
	// bike:1
	// 0
}

func ExampleClient_rule3() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START rule_3
	res48, err := rdb.Del(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res48) // >>> 0

	res49, err := rdb.LLen(ctx, "bikes:repairs").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res49) // >>> 0

	res50, err := rdb.LPop(ctx, "bikes:repairs").Result()

	if err != nil {
		fmt.Println(err) // >>> redis: nil
	}

	fmt.Println(res50) // >>> <empty string>
	// STEP_END

	// Output:
	// 0
	// 0
	// redis: nil
	//
}

func ExampleClient_ltrim1() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:repairs")
	// REMOVE_END

	// STEP_START ltrim.1
	res51, err := rdb.LPush(ctx, "bikes:repairs", "bike:1", "bike:2", "bike:3", "bike:4", "bike:5").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res51) // >>> 5

	res52, err := rdb.LTrim(ctx, "bikes:repairs", 0, 2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res52) // >>> OK

	res53, err := rdb.LRange(ctx, "bikes:repairs", 0, -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res53) // >>> [bike:5 bike:4 bike:3]
	// STEP_END

	// Output:
	// 5
	// OK
	// [bike:5 bike:4 bike:3]
}
