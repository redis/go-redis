package redis_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestStringCmdScanUint(t *testing.T) {
	cmd := redis.NewStringResult("4294967296", nil)
	got := uint(42)
	err := cmd.Scan(&got)
	if strconv.IntSize == 32 {
		if !errors.Is(err, strconv.ErrRange) {
			t.Fatalf("Scan: got error %v, want %v", err, strconv.ErrRange)
		}
		if got != 42 {
			t.Fatalf("Scan changed destination on error: got %d, want 42", got)
		}
	} else {
		if err != nil {
			t.Fatalf("Scan: unexpected error %v", err)
		}
		if uint64(got) != 4294967296 {
			t.Fatalf("Scan: got %d, want 4294967296", got)
		}
	}
}
