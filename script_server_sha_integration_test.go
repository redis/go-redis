package redis_test

import (
	"context"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestNewScriptServerSHA_Integration_ReloadAfterScriptFlush(t *testing.T) {
	ctx := context.Background()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	s := redis.NewScriptServerSHA("return 1")

	if err := s.Run(ctx, rdb, nil).Err(); err != nil {
		t.Fatalf("first Run failed: %v", err)
	}

	if err := rdb.Do(ctx, "SCRIPT", "FLUSH").Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH failed: %v", err)
	}

	if err := s.Run(ctx, rdb, nil).Err(); err != nil {
		t.Fatalf("second Run after flush failed: %v", err)
	}
}

func TestNewScriptServerSHA_Integration_RunRO(t *testing.T) {
	ctx := context.Background()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	s := redis.NewScriptServerSHA("return 1")

	if err := s.RunRO(ctx, rdb, nil).Err(); err != nil {
		t.Fatalf("RunRO failed: %v", err)
	}
}
