package redis_test

import (
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestStringCmdScanBool(t *testing.T) {
	for _, input := range []string{"1", "0", "true", "false", "TRUE", "False", "", "2", "yes", "true ", " true", "invalid"} {
		t.Run(input, func(t *testing.T) {
			cmd := redis.NewStringResult(input, nil)
			want, wantErr := cmd.Bool()
			got := true
			err := cmd.Scan(&got)
			if wantErr != nil {
				// Scan preserves v9's false, nil fallback; Bool remains strict.
				if err != nil || got {
					t.Fatalf("Scan(%q): got (%v, %v), want (false, nil)", input, got, err)
				}
			} else if err != nil || got != want {
				t.Fatalf("Scan(%q): got (%v, %v), Bool returned (%v, nil)", input, got, err, want)
			}
		})
	}
}

func TestStringSliceCmdScanBool(t *testing.T) {
	cmd := redis.NewStringSliceResult([]string{"true", "false", "1", "0", "", "yes", "invalid", "TRUE"}, nil)
	var got []bool
	if err := cmd.ScanSlice(&got); err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false, true, false, false, false, false, true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanSlice: got %v, want %v", got, want)
	}
}
