package redis

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// RESP3 sends a double reply, for example from ZSCORE, as a float64 value.
// The float helpers of Cmd must accept it, as they accept the RESP2 bulk
// string.
func TestCmdFloatFromDoubleReply(t *testing.T) {
	read := func(t *testing.T, reply string) *Cmd {
		t.Helper()
		cmd := NewCmd(context.Background())
		if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
			t.Fatalf("readReply(%q): %v", reply, err)
		}
		return cmd
	}

	for _, reply := range []string{",1.5\r\n", "$3\r\n1.5\r\n"} {
		cmd := read(t, reply)
		if got, err := cmd.Float64(); err != nil || got != 1.5 {
			t.Errorf("Float64() for %q = %v, %v; want 1.5, nil", reply, got, err)
		}
		if got, err := cmd.Float32(); err != nil || got != 1.5 {
			t.Errorf("Float32() for %q = %v, %v; want 1.5, nil", reply, got, err)
		}
	}

	for _, reply := range []string{"*2\r\n,1.5\r\n,2\r\n", "*2\r\n$3\r\n1.5\r\n$1\r\n2\r\n"} {
		cmd := read(t, reply)
		if got, err := cmd.Float64Slice(); err != nil || !reflect.DeepEqual(got, []float64{1.5, 2}) {
			t.Errorf("Float64Slice() for %q = %v, %v; want [1.5 2], nil", reply, got, err)
		}
		if got, err := cmd.Float32Slice(); err != nil || !reflect.DeepEqual(got, []float32{1.5, 2}) {
			t.Errorf("Float32Slice() for %q = %v, %v; want [1.5 2], nil", reply, got, err)
		}
	}
}

// A double outside the float32 range must give the same range error from
// Float32 for a RESP3 double as for a RESP2 bulk string. An infinity sent by
// the server is not an error.
func TestCmdFloat32FromDoubleReplyOutOfRange(t *testing.T) {
	for _, reply := range []string{",1e100\r\n", "$5\r\n1e100\r\n"} {
		cmd := NewCmd(context.Background())
		if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
			t.Fatalf("readReply(%q): %v", reply, err)
		}
		if got, err := cmd.Float32(); !errors.Is(err, strconv.ErrRange) {
			t.Errorf("Float32() for %q = %v, %v; want strconv.ErrRange", reply, got, err)
		}
	}

	cmd := NewCmd(context.Background())
	if err := cmd.readReply(proto.NewReader(strings.NewReader(",inf\r\n"))); err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if got, err := cmd.Float32(); err != nil || !math.IsInf(float64(got), 1) {
		t.Errorf("Float32() for inf = %v, %v; want +Inf, nil", got, err)
	}
}
