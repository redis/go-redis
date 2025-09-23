// EXAMPLE: bitmap_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_ping() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "pings:2024-01-01-00:00")
	// REMOVE_END

	// STEP_START ping
	res1, err := rdb.SetBit(ctx, "pings:2024-01-01-00:00", 123, 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> 0

	res2, err := rdb.GetBit(ctx, "pings:2024-01-01-00:00", 123).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> 1

	res3, err := rdb.GetBit(ctx, "pings:2024-01-01-00:00", 456).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> 0
	// STEP_END

	// Output:
	// 0
	// 1
	// 0
}

func ExampleClient_bitcount() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	_, err := rdb.SetBit(ctx, "pings:2024-01-01-00:00", 123, 1).Result()

	if err != nil {
		panic(err)
	}
	// REMOVE_END

	// STEP_START bitcount
	res4, err := rdb.BitCount(ctx, "pings:2024-01-01-00:00",
		&redis.BitCount{
			Start: 0,
			End:   456,
		}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> 1
	// STEP_END

	// Output:
	// 1
}

func ExampleClient_bitop_setup() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "A", "B", "C", "R")
	// REMOVE_END

	// STEP_START bitop_setup
	rdb.SetBit(ctx, "A", 0, 1)
	rdb.SetBit(ctx, "A", 1, 1)
	rdb.SetBit(ctx, "A", 3, 1)
	rdb.SetBit(ctx, "A", 4, 1)
	ba, _ := rdb.Get(ctx, "A").Bytes()
	fmt.Printf("%08b\n", ba[0])
	// >>> 11011000

	rdb.SetBit(ctx, "B", 3, 1)
	rdb.SetBit(ctx, "B", 4, 1)
	rdb.SetBit(ctx, "B", 7, 1)
	bb, _ := rdb.Get(ctx, "B").Bytes()
	fmt.Printf("%08b\n", bb[0])
	// >>> 00011001

	rdb.SetBit(ctx, "C", 1, 1)
	rdb.SetBit(ctx, "C", 2, 1)
	rdb.SetBit(ctx, "C", 4, 1)
	rdb.SetBit(ctx, "C", 5, 1)
	bc, _ := rdb.Get(ctx, "C").Bytes()
	fmt.Printf("%08b\n", bc[0])
	// >>> 01101100
	// STEP_END

	// Output:
	// 11011000
	// 00011001
	// 01101100
}

func ExampleClient_bitop_ops() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	// start with fresh database
	rdb.FlushDB(ctx)
	rdb.Del(ctx, "A", "B", "C", "R")
	// REMOVE_END

	// HIDE_START
	rdb.SetBit(ctx, "A", 0, 1)
	rdb.SetBit(ctx, "A", 1, 1)
	rdb.SetBit(ctx, "A", 3, 1)
	rdb.SetBit(ctx, "A", 4, 1)
	rdb.SetBit(ctx, "B", 3, 1)
	rdb.SetBit(ctx, "B", 4, 1)
	rdb.SetBit(ctx, "B", 7, 1)
	rdb.SetBit(ctx, "C", 1, 1)
	rdb.SetBit(ctx, "C", 2, 1)
	rdb.SetBit(ctx, "C", 4, 1)
	rdb.SetBit(ctx, "C", 5, 1)
	// HIDE_END

	// STEP_START bitop_and
	rdb.BitOpAnd(ctx, "R", "A", "B", "C")
	br, _ := rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 00001000
	// STEP_END

	// STEP_START bitop_or
	rdb.BitOpOr(ctx, "R", "A", "B", "C")
	br, _ = rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 11111101
	// STEP_END

	// STEP_START bitop_xor
	rdb.BitOpXor(ctx, "R", "A", "B")
	br, _ = rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 11000001
	// STEP_END

	// STEP_START bitop_not
	rdb.BitOpNot(ctx, "R", "A")
	br, _ = rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 00100111
	// STEP_END

	// STEP_START bitop_diff
	rdb.BitOpDiff(ctx, "R", "A", "B", "C")
	br, _ = rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 10000000
	// STEP_END

	// STEP_START bitop_diff1
	rdb.BitOpDiff1(ctx, "R", "A", "B", "C")
	br, _ = rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 00100101
	// STEP_END

	// STEP_START bitop_andor
	rdb.BitOpAndOr(ctx, "R", "A", "B", "C")
	br, _ = rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 01011000
	// STEP_END

	// STEP_START bitop_one
	rdb.BitOpOne(ctx, "R", "A", "B", "C")
	br, _ = rdb.Get(ctx, "R").Bytes()
	fmt.Printf("%08b\n", br[0])
	// >>> 10100101
	// STEP_END

	// Output:
	// 00001000
	// 11111101
	// 11000001
	// 00100111
	// 10000000
	// 00100101
	// 01011000
	// 10100101
}
