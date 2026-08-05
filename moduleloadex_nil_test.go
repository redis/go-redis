package redis

import (
	"context"
	"testing"
)

func TestModuleLoadexNilConfig(t *testing.T) {
	rdb := NewClient(&Options{Addr: "localhost:6379"})
	cmd := rdb.ModuleLoadex(context.Background(), nil)
	if cmd.Err() == nil {
		t.Fatal("expected error for nil config")
	}
	if cmd.Err().Error() != "redis: ModuleLoadex nil config" {
		t.Fatalf("got %v", cmd.Err())
	}
}
