// EXAMPLE: pipe_trans_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_transactions() {
	// STEP_START basic_trans
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "RateCounter")
	// REMOVE_END

	setResult, err := rdb.Set(ctx, "RateCounter", 0, 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(setResult) // >>> OK

	trans := rdb.TxPipeline()

	// The values of `incrResult` and `expResult` are not available
	// until the transaction has finished executing.
	incrResult := trans.Incr(ctx, "RateCounter")
	expResult := trans.Expire(ctx, "RateCounter", time.Second*10)

	cmdsExecuted, err := trans.Exec(ctx)

	if err != nil {
		panic(err)
	}

	// Values are now available.
	fmt.Println(incrResult.Val()) // >>> 1
	fmt.Println(expResult.Val())  // >>> true

	// You can also use the array of command data returned
	// by the `Exec()` call.
	fmt.Println(len(cmdsExecuted)) // >>> 2

	fmt.Printf("%v: %v\n",
		cmdsExecuted[0].Name(),
		cmdsExecuted[0].(*redis.IntCmd).Val(),
	)
	// >>> incr: 1

	fmt.Printf("%v: %v\n",
		cmdsExecuted[1].Name(),
		cmdsExecuted[1].(*redis.BoolCmd).Val(),
	)
	// >>> expire: true
	// STEP_END

	// Output:
	// OK
	// 1
	// true
	// 2
	// incr: 1
	// expire: true
}
