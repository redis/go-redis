package redis_test

import (
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestStringCmdScanBool(t *testing.T) {
	for _, input := range []string{"1", "0", "true", "false", "TRUE", "False", "invalid"} {
		t.Run(input, func(t *testing.T) {
			cmd := redis.NewStringResult(input, nil)
			want, wantErr := cmd.Bool()
			got := true
			err := cmd.Scan(&got)
			if wantErr != nil {
				if !errors.Is(err, strconv.ErrSyntax) || !got {
					t.Fatalf("Scan(%q): got (%v, %v), want unchanged destination and syntax error", input, got, err)
				}
			} else if err != nil || got != want {
				t.Fatalf("Scan(%q): got (%v, %v), Bool returned (%v, nil)", input, got, err, want)
			}
		})
	}
}

func TestStringSliceCmdScanBool(t *testing.T) {
	cmd := redis.NewStringSliceResult([]string{"true", "false", "1", "0"}, nil)
	var got []bool
	if err := cmd.ScanSlice(&got); err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false, true, false}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanSlice: got %v, want %v", got, want)
	}
}
