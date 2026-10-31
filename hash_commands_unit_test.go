package redis

import (
	"context"
	"reflect"
	"testing"
)

// TestHGetEXWithArgsArgs checks that HGetEXWithArgs builds HGETEX with and
// without options. Nil options send the same command as HGetEX.
func TestHGetEXWithArgsArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		options *HGetEXOptions
		want    []interface{}
	}{
		{
			name: "nil_options",
			want: []interface{}{"HGETEX", "key", "FIELDS", 2, "f1", "f2"},
		},
		{
			name:    "empty_options",
			options: &HGetEXOptions{},
			want:    []interface{}{"HGETEX", "key", "FIELDS", 2, "f1", "f2"},
		},
		{
			name:    "ex",
			options: &HGetEXOptions{ExpirationType: HGetEXExpirationEX, ExpirationVal: 60},
			want:    []interface{}{"HGETEX", "key", "EX", int64(60), "FIELDS", 2, "f1", "f2"},
		},
		{
			name:    "persist",
			options: &HGetEXOptions{ExpirationType: HGetEXExpirationPERSIST},
			want:    []interface{}{"HGETEX", "key", "PERSIST", "FIELDS", 2, "f1", "f2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			cmd := captureCmdable(&captured).HGetEXWithArgs(ctx, "key", tt.options, "f1", "f2")
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}

// TestHSetEXWithArgsArgs checks that HSetEXWithArgs builds HSETEX with and
// without options. Nil options send the same command as HSetEX.
func TestHSetEXWithArgsArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		options *HSetEXOptions
		want    []interface{}
	}{
		{
			name: "nil_options",
			want: []interface{}{"HSETEX", "key", "FIELDS", 1, "f1", "v1"},
		},
		{
			name:    "empty_options",
			options: &HSetEXOptions{},
			want:    []interface{}{"HSETEX", "key", "FIELDS", 1, "f1", "v1"},
		},
		{
			name:    "fnx_ex",
			options: &HSetEXOptions{Condition: HSetEXFNX, ExpirationType: HSetEXExpirationEX, ExpirationVal: 60},
			want:    []interface{}{"HSETEX", "key", "FNX", "EX", int64(60), "FIELDS", 1, "f1", "v1"},
		},
		{
			name:    "keepttl",
			options: &HSetEXOptions{ExpirationType: HSetEXExpirationKEEPTTL},
			want:    []interface{}{"HSETEX", "key", "KEEPTTL", "FIELDS", 1, "f1", "v1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			cmd := captureCmdable(&captured).HSetEXWithArgs(ctx, "key", tt.options, "f1", "v1")
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}
