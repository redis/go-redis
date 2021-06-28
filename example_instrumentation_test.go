package redis_test

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/internal/pool"
)

type contextKey string

var key = contextKey("foo")

type redisHook struct{}
type redisFullHook struct {
	redisHook
}

var _ redis.Hook = redisHook{}
var _ redis.Hook = redisFullHook{}

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

func (redisFullHook) BeforeConnect(ctx context.Context) context.Context {
	fmt.Printf("before connect")

	if v := ctx.Value(key); v != nil {
		fmt.Printf(" %v\n", v)
	} else {
		fmt.Printf("\n")
	}

	return ctx
}

func (redisFullHook) AfterConnect(ctx context.Context, event pool.ConnectEvent) {
	fmt.Printf("after connect: %v", event.Err)
	if v := ctx.Value(key); v != nil {
		fmt.Printf(" %v\n", v)
	} else {
		fmt.Printf("\n")
	}
}

func Example_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisHook{})

	rdb.Ping(ctx)
	// Output: starting processing: <ping: >
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
	// finished processing: <watch foo: OK>
	// starting processing: <ping: >
	// finished processing: <ping: PONG>
	// starting processing: <ping: >
	// finished processing: <ping: PONG>
	// starting processing: <unwatch: >
	// finished processing: <unwatch: OK>
}

func ExamplePool_instrumentation() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisFullHook{})

	rdb.Ping(ctx)
	// Output:
	// starting processing: <ping: >
	// before connect
	// after connect: <nil>
	// finished processing: <ping: PONG>
}

func ExamplePool_instrumentation_connect_error() {
	invalidAddr := "0.0.0.1:6379"
	rdb := redis.NewClient(&redis.Options{
		Addr:       invalidAddr,
		MaxRetries: -1,
	})
	rdb.AddHook(redisFullHook{})

	rdb.Ping(ctx)
	// Output:
	// starting processing: <ping: >
	// before connect
	// after connect: dial tcp 0.0.0.1:6379: connect: no route to host
	// finished processing: <ping: dial tcp 0.0.0.1:6379: connect: no route to host>
}

func ExamplePool_instrumentation_context_wiring() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisFullHook{})

	ctx := context.WithValue(context.Background(), key, "bar")
	rdb.Ping(ctx)
	// Output:
	// starting processing: <ping: >
	// before connect bar
	// after connect: <nil> bar
	// finished processing: <ping: PONG>
}

func ExamplePool_instrumentation_mulitple_hooks() {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisFullHook{})
	rdb.AddHook(redisFullHook{})

	rdb.Ping(ctx)
	// Output:
	// starting processing: <ping: >
	// starting processing: <ping: >
	// before connect
	// before connect
	// after connect: <nil>
	// after connect: <nil>
	// finished processing: <ping: PONG>
	// finished processing: <ping: PONG>
}
