package redis_test

import (
	"github.com/go-redis/redis"
	"testing"
)

var (
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", //
		Password: "",                // no password set
		DB:       0,
		PoolSize: 3,
	})
)

func TestCmdable_SlowLogGet(t *testing.T) {
	y, e := client.SlowLogGet(10).Result()
	if e != nil {
		t.FailNow()
	}
	if y == nil {
		println("no slow log found")
	}
}

func TestCmdable_SlowLogLen(t *testing.T) {
	r, err := client.SlowLogLen().Result()
	if err != nil {
		t.FailNow()
	}
	if r < 0 {
		t.FailNow()
	}
}

func TestCmdable_SlowLogReset(t *testing.T) {
	x, err := client.SlowLogReset().Result()
	if err != nil {
		t.FailNow()
	}
	if !x {
		t.FailNow()
	}
}
