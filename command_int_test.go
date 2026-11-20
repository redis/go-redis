package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// An integer reply that does not fit in an int, which happens where int is 32
// bits, must give Cmd.Int the range error it gives for the same number in a
// bulk string, not a truncated value.
func TestCmdIntFromIntegerReplyOutOfRange(t *testing.T) {
	read := func(t *testing.T, reply string) *Cmd {
		t.Helper()
		cmd := NewCmd(context.Background())
		if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
			t.Fatalf("readReply(%q): %v", reply, err)
		}
		return cmd
	}

	for _, n := range []int64{
		math.MaxInt32, math.MinInt32,
		math.MaxInt32 + 1, math.MinInt32 - 1,
		math.MaxInt64, math.MinInt64,
	} {
		s := strconv.FormatInt(n, 10)
		integer := ":" + s + "\r\n"
		bulk := "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"

		got, err := read(t, integer).Int()
		want, wantErr := read(t, bulk).Int()
		if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
			t.Errorf("Int() for %q = %v, %v; want %v, %v", integer, got, err, want, wantErr)
		}
		if int64(int(n)) != n && !errors.Is(err, strconv.ErrRange) {
			t.Errorf("Int() for %q = %v, %v; want strconv.ErrRange", integer, got, err)
		}
	}
}
