package redis_test

import (
	"context"
	"fmt"
	"net"

	"github.com/redis/go-redis/v9"
)

type redisHook struct{}

var _ redis.Hook = redisHook{}

func (redisHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		fmt.Printf("dialing %s %s\n", network, addr)
		conn, err := hook(ctx, network, addr)
		fmt.Printf("finished dialing %s %s\n", network, addr)
		return conn, err
	}
}

func (redisHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		fmt.Printf("starting processing: <%s>\n", cmd)
		err := hook(ctx, cmd)
		fmt.Printf("finished processing: <%s>\n", cmd)
		return err
	}
}

func (redisHook) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		fmt.Printf("pipeline starting processing: %v\n", cmds)
		err := hook(ctx, cmds)
		fmt.Printf("pipeline finished processing: %v\n", cmds)
		return err
	}
}

func Example_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisHook{})

	rdb.Ping(ctx)
	// Output: starting processing: <ping: >
	// dialing tcp :6379
	// finished dialing tcp :6379
	// finished processing: <ping: PONG>
}

func ExamplePipeline_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisHook{})

	rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Ping(ctx)
		pipe.Ping(ctx)
		return nil
	})
	// Output: pipeline starting processing: [ping:  ping: ]
	// dialing tcp :6379
	// finished dialing tcp :6379
	// pipeline finished processing: [ping: PONG ping: PONG]
}

func ExampleClient_Watch_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisHook{})

	rdb.Watch(ctx, func(tx *redis.Tx) error {
		tx.Ping(ctx)
		tx.Ping(ctx)
		return nil
	}, "foo")
	// Output:
	// starting processing: <watch foo: >
	// dialing tcp :6379
	// finished dialing tcp :6379
	// finished processing: <watch foo: OK>
	// starting processing: <ping: >
	// finished processing: <ping: PONG>
	// starting processing: <ping: >
	// finished processing: <ping: PONG>
	// starting processing: <unwatch: >
	// finished processing: <unwatch: OK>
}
