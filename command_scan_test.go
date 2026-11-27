package redis_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

type scanStringSlice []string

func (s *scanStringSlice) ScanRedis(value string) error {
	*s = strings.Split(value, ",")
	return nil
}

type scanByteSlice []byte

func TestHashScanSliceFields(t *testing.T) {
	type stringsField struct {
		Value []string `redis:"value"`
	}
	type intsField struct {
		Value []int `redis:"value"`
	}
	type bytesField struct {
		Value []byte `redis:"value"`
	}
	type namedBytesField struct {
		Value scanByteSlice `redis:"value"`
	}
	type scannerField struct {
		Value scanStringSlice `redis:"value"`
	}
	type unmatchedField struct {
		Value []string `redis:"other"`
	}
	type scannable interface {
		Scan(interface{}) error
		SetErr(error)
	}

	for _, method := range []string{"HGetAll", "HMGet"} {
		t.Run(method, func(t *testing.T) {
			newCmd := func() scannable {
				if method == "HGetAll" {
					cmd := redis.NewMapStringStringCmd(context.Background(), "hgetall", "key")
					cmd.SetVal(map[string]string{"value": "a,b"})
					return cmd
				}
				cmd := redis.NewSliceCmd(context.Background(), "hmget", "key", "value")
				cmd.SetVal([]interface{}{"a,b"})
				return cmd
			}

			tests := []struct {
				name    string
				dst     interface{}
				want    interface{}
				wantErr string
			}{
				{
					name:    "strings",
					dst:     &stringsField{Value: []string{"old"}},
					want:    &stringsField{Value: []string{"old"}},
					wantErr: "redis.Scan(unsupported []string)",
				},
				{
					name:    "nil strings",
					dst:     &stringsField{},
					want:    &stringsField{},
					wantErr: "redis.Scan(unsupported []string)",
				},
				{
					name:    "ints",
					dst:     &intsField{Value: []int{42}},
					want:    &intsField{Value: []int{42}},
					wantErr: "redis.Scan(unsupported []int)",
				},
				{
					name: "bytes",
					dst:  &bytesField{Value: []byte("old")},
					want: &bytesField{Value: []byte("a,b")},
				},
				{
					name: "named bytes",
					dst:  &namedBytesField{Value: scanByteSlice("old")},
					want: &namedBytesField{Value: scanByteSlice("a,b")},
				},
				{
					name: "custom scanner",
					dst:  &scannerField{Value: scanStringSlice{"old"}},
					want: &scannerField{Value: scanStringSlice{"a", "b"}},
				},
				{
					name: "unmatched slice",
					dst:  &unmatchedField{Value: []string{"old"}},
					want: &unmatchedField{Value: []string{"old"}},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					err := newCmd().Scan(tt.dst)
					if tt.wantErr == "" {
						if err != nil {
							t.Fatalf("Scan: %v", err)
						}
					} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
						t.Fatalf("Scan error = %v, want %q", err, tt.wantErr)
					}
					if !reflect.DeepEqual(tt.dst, tt.want) {
						t.Fatalf("Scan destination = %+v, want %+v", tt.dst, tt.want)
					}
				})
			}

			t.Run("command error", func(t *testing.T) {
				cmd := newCmd()
				wantErr := errors.New("command failed")
				cmd.SetErr(wantErr)
				dst := stringsField{Value: []string{"old"}}
				if err := cmd.Scan(&dst); err != wantErr {
					t.Fatalf("Scan error = %v, want original command error", err)
				}
				if !reflect.DeepEqual(dst.Value, []string{"old"}) {
					t.Fatalf("Scan changed destination: %+v", dst)
				}
			})
		})
	}
}

func TestHMGetScanContinuesAfterUnsupportedSlice(t *testing.T) {
	dst := struct {
		Before string   `redis:"before"`
		Values []string `redis:"values"`
		After  string   `redis:"after"`
	}{Before: "old", Values: []string{"old"}, After: "old"}
	cmd := redis.NewSliceCmd(context.Background(), "hmget", "key", "before", "values", "after")
	cmd.SetVal([]interface{}{"updated", "a,b", "updated"})
	if err := cmd.Scan(&dst); err == nil || !strings.Contains(err.Error(), "redis.Scan(unsupported []string)") {
		t.Fatalf("Scan error = %v, want unsupported slice error", err)
	}
	if dst.Before != "updated" || !reflect.DeepEqual(dst.Values, []string{"old"}) || dst.After != "updated" {
		t.Fatalf("unexpected scan result: %+v", dst)
	}
}

var (
	errScanFirst  = errors.New("first field")
	errScanSecond = errors.New("second field")
)

type scanFirst string

func (s *scanFirst) ScanRedis(string) error { return errScanFirst }

type scanSecond string

func (s *scanSecond) ScanRedis(string) error { return errScanSecond }

// Several failing fields from HGETALL: every error stays reachable through
// errors.Is, ordered by key, and the result is the same on every call even
// though the reply is a map.
func TestMapStringStringCmdScanJoinsFieldErrors(t *testing.T) {
	type hash struct {
		A scanFirst  `redis:"a"`
		B scanSecond `redis:"b"`
		C string     `redis:"c"`
	}

	val := map[string]string{"a": "x", "b": "y", "c": "3"}
	var first string
	for i := 0; i < 20; i++ {
		var h hash
		err := redis.NewMapStringStringResult(val, nil).Scan(&h)
		if !errors.Is(err, errScanFirst) || !errors.Is(err, errScanSecond) {
			t.Fatalf("got %v, want both field errors", err)
		}
		if !strings.HasPrefix(err.Error(), errScanFirst.Error()) {
			t.Fatalf("got %q, want the error of the smallest key first", err)
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("got %q, want the same error as before: %q", err, first)
		}
		if h.C != "3" {
			t.Fatalf("got C=%q, want 3", h.C)
		}
	}
}

// One failing field from HGETALL: the scanner's error is returned as is.
func TestMapStringStringCmdScanSingleErrorAsIs(t *testing.T) {
	type hash struct {
		A scanFirst `redis:"a"`
		C string    `redis:"c"`
	}
	var h hash
	err := redis.NewMapStringStringResult(map[string]string{"a": "x", "c": "3"}, nil).Scan(&h)
	if err != errScanFirst {
		t.Fatalf("got %v, want the scanner's error unchanged", err)
	}
}

func TestMapStringStringCmdScanBadField(t *testing.T) {
	type hash struct {
		A string `redis:"a"`
		B string `redis:"b"`
		C string `redis:"c"`
		D string `redis:"d"`
		N int    `redis:"n"`
	}

	val := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "n": "abc"}
	for i := 0; i < 20; i++ {
		var h hash
		if err := redis.NewMapStringStringResult(val, nil).Scan(&h); err == nil {
			t.Fatal("expected an error for field n")
		}
		if want := (hash{A: "1", B: "2", C: "3", D: "4"}); h != want {
			t.Fatalf("got %+v, want %+v", h, want)
		}
	}
}

func TestMapStringStringCmdScanBadFieldsSameError(t *testing.T) {
	type hash struct {
		A string `redis:"a"`
		M int    `redis:"m"`
		N int    `redis:"n"`
	}

	val := map[string]string{"a": "1", "m": "xyz", "n": "abc"}
	var first string
	for i := 0; i < 20; i++ {
		var h hash
		err := redis.NewMapStringStringResult(val, nil).Scan(&h)
		if err == nil || !strings.Contains(err.Error(), "hash.M") {
			t.Fatalf("got %v, want the error for field m", err)
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("got %q, want the same error as before: %q", err, first)
		}
		if h.A != "1" {
			t.Fatalf("got A=%q, want 1", h.A)
		}
	}
}
