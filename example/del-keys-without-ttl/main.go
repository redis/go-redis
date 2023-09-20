package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})

	_ = rdb.Set(ctx, "key_with_ttl", "bar", time.Minute).Err()
	_ = rdb.Set(ctx, "key_without_ttl_1", "", 0).Err()
	_ = rdb.Set(ctx, "key_without_ttl_2", "", 0).Err()

	checker := NewKeyChecker(rdb, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := rdb.Scan(ctx, 0, "", 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}

	deleted := checker.Stop()
	fmt.Println("deleted", deleted, "keys", "in", time.Since(start))
}

type KeyChecker struct {
	rdb       *redis.Client
	batchSize int
	ch        chan string
	delCh     chan string
	wg        sync.WaitGroup
	deleted   int
	logger    *zap.Logger
}

func NewKeyChecker(rdb *redis.Client, batchSize int) *KeyChecker {
	return &KeyChecker{
		rdb:       rdb,
		batchSize: batchSize,
		ch:        make(chan string, batchSize),
		delCh:     make(chan string, batchSize),
		logger:    zap.L(),
	}
}

func (c *KeyChecker) Add(key string) {
	c.ch <- key
}

func (c *KeyChecker) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.del(ctx); err != nil {
			panic(err)
		}
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.delCh)

		keys := make([]string, 0, c.batchSize)

		for key := range c.ch {
			keys = append(keys, key)
			if len(keys) < cap(keys) {
				continue
			}

			if err := c.checkKeys(ctx, keys); err != nil {
				c.logger.Error("checkKeys failed", zap.Error(err))
			}
			keys = keys[:0]
		}

		if len(keys) > 0 {
			if err := c.checkKeys(ctx, keys); err != nil {
				c.logger.Error("checkKeys failed", zap.Error(err))
			}
			keys = nil
		}
	}()
}

func (c *KeyChecker) Stop() int {
	close(c.ch)
	c.wg.Wait()
	return c.deleted
}

func (c *KeyChecker) checkKeys(ctx context.Context, keys []string) error {
	cmds, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			pipe.TTL(ctx, key)
		}
		return nil
	})
	if err != nil {
		return err
	}

	for i, cmd := range cmds {
		d, err := cmd.(*redis.DurationCmd).Result()
		if err != nil {
			return err
		}
		if d == -1 {
			c.delCh <- keys[i]
		}
	}

	return nil
}

func (c *KeyChecker) del(ctx context.Context) error {
	pipe := c.rdb.Pipeline()

	for key := range c.delCh {
		fmt.Printf("deleting %s...\n", key)
		pipe.Del(ctx, key)
		c.deleted++

		if pipe.Len() < c.batchSize {
			continue
		}

		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}
