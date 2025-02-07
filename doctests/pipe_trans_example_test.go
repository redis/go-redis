// EXAMPLE: pipe_trans_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_transactions() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})
	// REMOVE_START
	for i := 0; i < 5; i++ {
		rdb.Del(ctx, fmt.Sprintf("seat:%d", i))
	}

	rdb.Del(ctx, "counter:1", "counter:2", "counter:3", "shellpath")
	// REMOVE_END

	// STEP_START basic_pipe
	pipe := rdb.Pipeline()

	for i := 0; i < 5; i++ {
		pipe.Set(ctx, fmt.Sprintf("seat:%v", i), fmt.Sprintf("#%v", i), 0)
	}

	cmds, err := pipe.Exec(ctx)

	if err != nil {
		panic(err)
	}

	for _, c := range cmds {
		fmt.Printf("%v;", c.(*redis.StatusCmd).Val())
	}

	fmt.Println("")
	// >>> OK;OK;OK;OK;OK;

	pipe = rdb.Pipeline()

	get0Result := pipe.Get(ctx, "seat:0")
	get3Result := pipe.Get(ctx, "seat:3")
	get4Result := pipe.Get(ctx, "seat:4")

	cmds, err = pipe.Exec(ctx)

	// The results are available only after the pipeline
	// has finished executing.
	fmt.Println(get0Result.Val()) // >>> #0
	fmt.Println(get3Result.Val()) // >>> #3
	fmt.Println(get4Result.Val()) // >>> #4
	// STEP_END

	// STEP_START basic_pipe_pipelined
	var pd0Result *redis.StatusCmd
	var pd3Result *redis.StatusCmd
	var pd4Result *redis.StatusCmd

	cmds, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pd0Result = (*redis.StatusCmd)(pipe.Get(ctx, "seat:0"))
		pd3Result = (*redis.StatusCmd)(pipe.Get(ctx, "seat:3"))
		pd4Result = (*redis.StatusCmd)(pipe.Get(ctx, "seat:4"))
		return nil
	})

	if err != nil {
		panic(err)
	}

	// The results are available only after the pipeline
	// has finished executing.
	fmt.Println(pd0Result.Val()) // >>> #0
	fmt.Println(pd3Result.Val()) // >>> #3
	fmt.Println(pd4Result.Val()) // >>> #4
	// STEP_END

	// STEP_START basic_trans
	trans := rdb.TxPipeline()

	trans.IncrBy(ctx, "counter:1", 1)
	trans.IncrBy(ctx, "counter:2", 2)
	trans.IncrBy(ctx, "counter:3", 3)

	cmds, err = trans.Exec(ctx)

	for _, c := range cmds {
		fmt.Println(c.(*redis.IntCmd).Val())
	}
	// >>> 1
	// >>> 2
	// >>> 3
	// STEP_END

	// STEP_START basic_trans_txpipelined
	var tx1Result *redis.IntCmd
	var tx2Result *redis.IntCmd
	var tx3Result *redis.IntCmd

	cmds, err = rdb.TxPipelined(ctx, func(trans redis.Pipeliner) error {
		tx1Result = trans.IncrBy(ctx, "counter:1", 1)
		tx2Result = trans.IncrBy(ctx, "counter:2", 2)
		tx3Result = trans.IncrBy(ctx, "counter:3", 3)
		return nil
	})

	if err != nil {
		panic(err)
	}

	fmt.Println(tx1Result.Val()) // >>> 2
	fmt.Println(tx2Result.Val()) // >>> 4
	fmt.Println(tx3Result.Val()) // >>> 6
	// STEP_END

	// STEP_START trans_watch
	// Set initial value of `shellpath`.
	rdb.Set(ctx, "shellpath", "/usr/syscmds/", 0)

	const maxRetries = 1000

	// Retry if the key has been changed.
	for i := 0; i < maxRetries; i++ {
		err := rdb.Watch(ctx,
			func(tx *redis.Tx) error {
				currentPath, err := rdb.Get(ctx, "shellpath").Result()
				newPath := currentPath + ":/usr/mycmds/"

				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, "shellpath", newPath, 0)
					return nil
				})

				return err
			},
			"shellpath",
		)

		if err == nil {
			// Success.
			break
		} else if err == redis.TxFailedErr {
			// Optimistic lock lost. Retry the transaction.
			continue
		} else {
			// Panic for any other error.
			panic(err)
		}
	}

	fmt.Println(rdb.Get(ctx, "shellpath").Val())
	// >>> /usr/syscmds/:/usr/mycmds/
	// STEP_END

	// Output:
	// OK;OK;OK;OK;OK;
	// #0
	// #3
	// #4
	// #0
	// #3
	// #4
	// 1
	// 2
	// 3
	// 2
	// 4
	// 6
	// /usr/syscmds/:/usr/mycmds/
}
