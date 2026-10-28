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

func TestHMGetScanStopsAtUnsupportedSlice(t *testing.T) {
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
	if dst.Before != "updated" || !reflect.DeepEqual(dst.Values, []string{"old"}) || dst.After != "old" {
		t.Fatalf("unexpected partial scan: %+v", dst)
	}
}
