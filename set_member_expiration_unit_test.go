package redis_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestSetMemberExpirationArgs(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	at := time.UnixMilli(1893553445123)

	pipe := client.Pipeline()
	tests := []struct {
		cmd  redis.Cmder
		want []interface{}
	}{
		{pipe.SExpire(ctx, "s", 10*time.Second, "a", "b"), []interface{}{"sexpire", "s", int64(10), "MEMBERS", 2, "a", "b"}},
		{pipe.SExpireWithArgs(ctx, "s", 10*time.Second, redis.SExpireArgs{GT: true}, "a"), []interface{}{"sexpire", "s", int64(10), "GT", "MEMBERS", 1, "a"}},
		{pipe.SPExpire(ctx, "s", 1500*time.Millisecond, "a"), []interface{}{"spexpire", "s", int64(1500), "MEMBERS", 1, "a"}},
		{pipe.SPExpireWithArgs(ctx, "s", time.Second, redis.SExpireArgs{NX: true}, "a"), []interface{}{"spexpire", "s", int64(1000), "NX", "MEMBERS", 1, "a"}},
		{pipe.SExpireAt(ctx, "s", at, "a"), []interface{}{"sexpireat", "s", at.Unix(), "MEMBERS", 1, "a"}},
		{pipe.SExpireAtWithArgs(ctx, "s", at, redis.SExpireArgs{LT: true}, "a"), []interface{}{"sexpireat", "s", at.Unix(), "LT", "MEMBERS", 1, "a"}},
		{pipe.SPExpireAt(ctx, "s", at, "a"), []interface{}{"spexpireat", "s", at.UnixMilli(), "MEMBERS", 1, "a"}},
		{pipe.SPExpireAtWithArgs(ctx, "s", at, redis.SExpireArgs{XX: true}, "a"), []interface{}{"spexpireat", "s", at.UnixMilli(), "XX", "MEMBERS", 1, "a"}},
		{pipe.STTL(ctx, "s", "a", "b"), []interface{}{"sttl", "s", "MEMBERS", 2, "a", "b"}},
		{pipe.SPTTL(ctx, "s", "a"), []interface{}{"spttl", "s", "MEMBERS", 1, "a"}},
		{pipe.SExpireTime(ctx, "s", "a"), []interface{}{"sexpiretime", "s", "MEMBERS", 1, "a"}},
		{pipe.SPExpireTime(ctx, "s", "a"), []interface{}{"spexpiretime", "s", "MEMBERS", 1, "a"}},
		{pipe.SPersist(ctx, "s", "a"), []interface{}{"spersist", "s", "MEMBERS", 1, "a"}},
		{pipe.SAddEX(ctx, "s", "a"), []interface{}{"saddex", "s", "MEMBERS", 1, "a"}},
		{pipe.SAddEXWithArgs(ctx, "s", &redis.SAddEXOptions{
			Condition: redis.SAddEXMNX, ExpirationType: redis.SAddEXExpirationEX, ExpirationVal: 60,
		}, "a", "b"), []interface{}{"saddex", "s", "MNX", "EX", int64(60), "MEMBERS", 2, "a", "b"}},
		{pipe.SAddEXWithArgs(ctx, "s", &redis.SAddEXOptions{
			ExpirationType: redis.SAddEXExpirationKEEPTTL,
		}, "a"), []interface{}{"saddex", "s", "KEEPTTL", "MEMBERS", 1, "a"}},
	}
	for _, tt := range tests {
		if got := tt.cmd.Args(); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("args = %v, want %v", got, tt.want)
		}
	}
}
