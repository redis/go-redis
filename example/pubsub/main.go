package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var consStopped = false

func main() {
	wg := &sync.WaitGroup{}
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	_ = rdb.FlushDB(ctx).Err()

	go func() {
		for {
			time.Sleep(2 * time.Second)
			fmt.Printf("pool stats: %+v\n", rdb.PoolStats())
		}
	}()
	err := rdb.Ping(ctx).Err()
	if err != nil {
		panic(err)
	}
	if err := rdb.Set(ctx, "prods", "0", 0).Err(); err != nil {
		panic(err)
	}
	if err := rdb.Set(ctx, "cons", "0", 0).Err(); err != nil {
		panic(err)
	}
	if err := rdb.Set(ctx, "cntr", "0", 0).Err(); err != nil {
		panic(err)
	}
	if err := rdb.Set(ctx, "recs", "0", 0).Err(); err != nil {
		panic(err)
	}
	fmt.Println("cntr", rdb.Get(ctx, "cntr").Val())
	fmt.Println("recs", rdb.Get(ctx, "recs").Val())
	subCtx, cancelSubCtx := context.WithCancel(ctx)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go subscribe(subCtx, rdb, "test", i, wg)
	}
	time.Sleep(time.Second)
	cancelSubCtx()
	time.Sleep(time.Second)
	subCtx, cancelSubCtx = context.WithCancel(ctx)
	for i := 0; i < 10; i++ {
		if err := rdb.Incr(ctx, "prods").Err(); err != nil {
			panic(err)
		}
		wg.Add(1)
		go floodThePool(subCtx, rdb, wg)
	}

	for i := 0; i < 100; i++ {
		if err := rdb.Incr(ctx, "cons").Err(); err != nil {
			panic(err)
		}
		wg.Add(1)
		go subscribe(subCtx, rdb, "test2", i, wg)
	}
	time.Sleep(10 * time.Second)
	fmt.Println("canceling")
	cancelSubCtx()
	wg.Wait()
	cntr, err := rdb.Get(ctx, "cntr").Result()
	recs, err := rdb.Get(ctx, "recs").Result()
	prods, err := rdb.Get(ctx, "prods").Result()
	cons, err := rdb.Get(ctx, "cons").Result()
	fmt.Printf("cntr: %s\n", cntr)
	fmt.Printf("recs: %s\n", recs)
	fmt.Printf("prods: %s\n", prods)
	fmt.Printf("cons: %s\n", cons)
	time.Sleep(2 * time.Second)
}

func floodThePool(ctx context.Context, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("floodThePool stopping")
			consStopped = true
			return
		default:
		}
		err := rdb.Publish(ctx, "test2", "hello").Err()
		if err != nil {
			log.Println("publish error:", err)
		}

		err = rdb.Incr(ctx, "cntr").Err()
		if err != nil {
			log.Println("incr error:", err)
		}
		time.Sleep(10 * time.Nanosecond)
	}
}

func subscribe(ctx context.Context, rdb *redis.Client, topic string, subscriberId int, wg *sync.WaitGroup) {
	defer wg.Done()
	defer fmt.Printf("subscriber %d stopping\n", subscriberId)
	rec := rdb.Subscribe(ctx, topic)
	recChan := rec.Channel()
	for {
		select {
		case <-ctx.Done():
			rec.Close()
			if subscriberId == 199 {
				fmt.Printf("subscriber %d done\n", subscriberId)
			}
			return
		default:
			select {
			case <-ctx.Done():
				rec.Close()
				if subscriberId == 199 {
					fmt.Printf("subscriber %d done\n", subscriberId)
				}
				return
			case msg := <-recChan:
				err := rdb.Incr(ctx, "recs").Err()
				if err != nil {
					log.Println("incr error:", err)
				}
				if consStopped {
					fmt.Printf("subscriber %d received %s\n", subscriberId, msg.Payload)
				}
				if subscriberId == 199 {
					fmt.Printf("subscriber %d received %s\n", subscriberId, msg.Payload)
				}
			}
		}
	}
}
