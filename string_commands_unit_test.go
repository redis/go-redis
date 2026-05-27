package redis

import (
	"context"
	"reflect"
	"testing"
)

// captureCmdable returns a cmdable that records the Cmder passed to it without
// dispatching the command. Used to assert that command builders produce the
// expected argument list.
func captureCmdable(out *Cmder) cmdable {
	return func(_ context.Context, cmd Cmder) error {
		*out = cmd
		return nil
	}
}

func TestIncrEXInt_Args(t *testing.T) {
	tests := []struct {
		name string
		args IncrEXIntArgs
		want []interface{}
	}{
		{
			name: "default",
			args: IncrEXIntArgs{},
			want: []interface{}{"increx", "key"},
		},
		{
			name: "byint",
			args: IncrEXIntArgs{By: 4, HasBy: true},
			want: []interface{}{"increx", "key", "byint", int64(4)},
		},
		{
			name: "byint_zero_explicit",
			args: IncrEXIntArgs{By: 0, HasBy: true},
			want: []interface{}{"increx", "key", "byint", int64(0)},
		},
		{
			name: "lbound_ubound",
			args: IncrEXIntArgs{
				By: 2, HasBy: true,
				LBound: 0, HasLBound: true,
				UBound: 20, HasUBound: true,
			},
			want: []interface{}{"increx", "key", "byint", int64(2), "lbound", int64(0), "ubound", int64(20)},
		},
		{
			name: "lbound_zero_is_sent",
			args: IncrEXIntArgs{
				LBound: 0, HasLBound: true,
			},
			want: []interface{}{"increx", "key", "lbound", int64(0)},
		},
		{
			name: "ex_and_enx",
			args: IncrEXIntArgs{
				By: 2, HasBy: true,
				Expiration: &ExpirationOption{Mode: EX, Value: 60},
				ENX:        true,
			},
			want: []interface{}{"increx", "key", "byint", int64(2), "ex", int64(60), "enx"},
		},
		{
			name: "px",
			args: IncrEXIntArgs{Expiration: &ExpirationOption{Mode: PX, Value: 1500}},
			want: []interface{}{"increx", "key", "px", int64(1500)},
		},
		{
			name: "exat",
			args: IncrEXIntArgs{Expiration: &ExpirationOption{Mode: EXAT, Value: 1753265054}},
			want: []interface{}{"increx", "key", "exat", int64(1753265054)},
		},
		{
			name: "pxat",
			args: IncrEXIntArgs{Expiration: &ExpirationOption{Mode: PXAT, Value: 1753265054014}},
			want: []interface{}{"increx", "key", "pxat", int64(1753265054014)},
		},
		{
			name: "persist",
			args: IncrEXIntArgs{Expiration: &ExpirationOption{Mode: PERSIST}},
			want: []interface{}{"increx", "key", "persist"},
		},
		{
			name: "saturate_with_lbound",
			args: IncrEXIntArgs{
				By: 1, HasBy: true,
				LBound: 10, HasLBound: true,
				Saturate: true,
			},
			want: []interface{}{"increx", "key", "byint", int64(1), "lbound", int64(10), "saturate"},
		},
		{
			name: "saturate_with_ubound",
			args: IncrEXIntArgs{
				By: 5, HasBy: true,
				UBound: 12, HasUBound: true,
				Saturate: true,
			},
			want: []interface{}{"increx", "key", "byint", int64(5), "ubound", int64(12), "saturate"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := c.IncrEXInt(context.Background(), "key", tt.args)
			if cmd == nil {
				t.Fatalf("IncrEXInt returned nil")
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}

func TestIncrEXFloat_Args(t *testing.T) {
	tests := []struct {
		name string
		args IncrEXFloatArgs
		want []interface{}
	}{
		{
			name: "default_zero_by",
			args: IncrEXFloatArgs{},
			want: []interface{}{"increx", "key", "byfloat", float64(0)},
		},
		{
			name: "byfloat",
			args: IncrEXFloatArgs{By: 0.25},
			want: []interface{}{"increx", "key", "byfloat", 0.25},
		},
		{
			name: "bounds_and_saturate",
			args: IncrEXFloatArgs{
				By:     0.7,
				UBound: 2, HasUBound: true,
				Saturate: true,
			},
			want: []interface{}{"increx", "key", "byfloat", 0.7, "ubound", float64(2), "saturate"},
		},
		{
			name: "lbound_ubound",
			args: IncrEXFloatArgs{
				By:     0.5,
				LBound: -1.5, HasLBound: true,
				UBound: 1.5, HasUBound: true,
			},
			want: []interface{}{"increx", "key", "byfloat", 0.5, "lbound", -1.5, "ubound", 1.5},
		},
		{
			name: "ex_and_enx",
			args: IncrEXFloatArgs{
				By:         1.5,
				Expiration: &ExpirationOption{Mode: EX, Value: 30},
				ENX:        true,
			},
			want: []interface{}{"increx", "key", "byfloat", 1.5, "ex", int64(30), "enx"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := c.IncrEXFloat(context.Background(), "key", tt.args)
			if cmd == nil {
				t.Fatalf("IncrEXFloat returned nil")
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}
