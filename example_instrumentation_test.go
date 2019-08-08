package redis_test

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v7"
)

type redisHook struct{}

var _ redis.Hook = redisHook{}

func (redisHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	fmt.Printf("starting processing: <%s>\n", cmd)
	return ctx, nil
}

func (redisHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	fmt.Printf("finished processing: <%s>\n", cmd)
	return nil
}

func (redisHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	fmt.Printf("pipeline starting processing: %v\n", cmds)
	return ctx, nil
}

func (redisHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	fmt.Printf("pipeline finished processing: %v\n", cmds)
	return nil
}

func Example_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisHook{})

	rdb.Ping()
	// Output: starting processing: <ping: >
	// finished processing: <ping: PONG>
}

func ExamplePipeline_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisHook{})

	rdb.Pipelined(func(pipe redis.Pipeliner) error {
		pipe.Ping()
		pipe.Ping()
		return nil
	})
	// Output: pipeline starting processing: [ping:  ping: ]
	// pipeline finished processing: [ping: PONG ping: PONG]
}
