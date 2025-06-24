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
		fmt.Printf("starting processing: <%v>\n", cmd.Args())
		err := hook(ctx, cmd)
		fmt.Printf("finished processing: <%v>\n", cmd.Args())
		return err
	}
}

func (redisHook) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		names := make([]string, 0, len(cmds))
		for _, cmd := range cmds {
			names = append(names, fmt.Sprintf("%v", cmd.Args()))
		}
		fmt.Printf("pipeline starting processing: %v\n", names)
		err := hook(ctx, cmds)
		fmt.Printf("pipeline finished processing: %v\n", names)
		return err
	}
}

func Example_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr:            ":6379",
		DisableIdentity: true,
	})
	rdb.AddHook(redisHook{})

	rdb.Ping(ctx)
	// Output:
	// starting processing: <[ping]>
	// dialing tcp :6379
	// finished dialing tcp :6379
	// starting processing: <[hello 3]>
	// finished processing: <[hello 3]>
	// finished processing: <[ping]>
}

func ExamplePipeline_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr:            ":6379",
		DisableIdentity: true,
	})
	rdb.AddHook(redisHook{})

	rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Ping(ctx)
		pipe.Ping(ctx)
		return nil
	})
	// Output:
	// pipeline starting processing: [[ping] [ping]]
	// dialing tcp :6379
	// finished dialing tcp :6379
	// starting processing: <[hello 3]>
	// finished processing: <[hello 3]>
	// pipeline finished processing: [[ping] [ping]]
}

func ExampleClient_Watch_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr:            ":6379",
		DisableIdentity: true,
	})
	rdb.AddHook(redisHook{})

	rdb.Watch(ctx, func(tx *redis.Tx) error {
		tx.Ping(ctx)
		tx.Ping(ctx)
		return nil
	}, "foo")
	// Output:
	// starting processing: <[watch foo]>
	// dialing tcp :6379
	// finished dialing tcp :6379
	// starting processing: <[hello 3]>
	// finished processing: <[hello 3]>
	// finished processing: <[watch foo]>
	// starting processing: <[ping]>
	// finished processing: <[ping]>
	// starting processing: <[ping]>
	// finished processing: <[ping]>
	// starting processing: <[unwatch]>
	// finished processing: <[unwatch]>
}
