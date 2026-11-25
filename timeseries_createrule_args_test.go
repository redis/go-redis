package redis_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestTSCreateRuleWithArgsAlignment(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer c.Close()
	p := c.Pipeline()
	cmd := p.TSCreateRuleWithArgs(context.Background(), "source", "dest", redis.Avg, 3600000, &redis.TSCreateRuleOptions{AlignTimestamp: 1800000})
	want := []interface{}{"TS.CREATERULE", "source", "dest", "AGGREGATION", "AVG", 3600000, int64(1800000)}
	if !reflect.DeepEqual(cmd.Args(), want) {
		t.Fatalf("got %v, want %v", cmd.Args(), want)
	}
}
