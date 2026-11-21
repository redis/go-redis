package redis

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// pairsFromArgs turns flat HSET-style args after the key into a string→value map.
func pairsFromArgs(t *testing.T, args []interface{}) map[string]interface{} {
	t.Helper()
	if len(args)%2 != 0 {
		t.Fatalf("expected even number of field/value args, got %d: %#v", len(args), args)
	}
	out := make(map[string]interface{}, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		k, ok := args[i].(string)
		if !ok {
			t.Fatalf("field at %d is %T, want string: %#v", i, args[i], args[i])
		}
		out[k] = args[i+1]
	}
	return out
}

func TestMapStringIntArgsFlattening(t *testing.T) {
	ctx := context.Background()
	var captured Cmder
	cmd := captureCmdable(&captured).HSet(ctx, "key", map[string]int{"a": 1, "b": 2})
	args := cmd.Args()
	if len(args) != 6 {
		t.Fatalf("map[string]int: expected 6 args, got %d: %#v", len(args), args)
	}
	if args[0] != "hset" || args[1] != "key" {
		t.Fatalf("unexpected prefix: %#v", args[:2])
	}
	got := pairsFromArgs(t, args[2:])
	if got["a"] != 1 || got["b"] != 2 {
		t.Fatalf("map[string]int pairs mismatch: %#v", got)
	}
}

func TestAppendArgTypedStringKeyMaps(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		arg  interface{}
		want map[string]interface{}
	}{
		{
			name: "map_string_int64",
			arg:  map[string]int64{"n": 9},
			want: map[string]interface{}{"n": int64(9)},
		},
		{
			name: "map_string_float64",
			arg:  map[string]float64{"score": 1.5},
			want: map[string]interface{}{"score": 1.5},
		},
		{
			name: "map_string_bool",
			arg:  map[string]bool{"on": true},
			want: map[string]interface{}{"on": true},
		},
		{
			name: "map_string_string",
			arg:  map[string]string{"k": "v"},
			want: map[string]interface{}{"k": "v"},
		},
		{
			name: "map_string_interface",
			arg:  map[string]interface{}{"k": 3},
			want: map[string]interface{}{"k": 3},
		},
		{
			name: "nil_typed_map",
			arg:  map[string]int(nil),
			want: map[string]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			cmd := captureCmdable(&captured).HSet(ctx, "key", tt.arg)
			args := cmd.Args()
			if args[0] != "hset" || args[1] != "key" {
				t.Fatalf("unexpected prefix: %#v", args[:2])
			}
			got := pairsFromArgs(t, args[2:])
			if len(got) != len(tt.want) {
				t.Fatalf("len mismatch got=%#v want=%#v", got, tt.want)
			}
			for k, wv := range tt.want {
				if got[k] != wv {
					t.Fatalf("field %q: got %#v (%T), want %#v (%T)", k, got[k], got[k], wv, wv)
				}
			}
		})
	}
}

func TestAppendArgNonStringKeyMapUnchanged(t *testing.T) {
	ctx := context.Background()
	var captured Cmder
	m := map[int]string{1: "a"}
	cmd := captureCmdable(&captured).HSet(ctx, "key", m)
	args := cmd.Args()
	// Non-string-key maps are not flattened; they stay a single trailing arg.
	if len(args) != 3 {
		t.Fatalf("expected 3 args for map[int]string, got %d: %#v", len(args), args)
	}
	if args[2] == nil {
		t.Fatal("expected map arg to be present")
	}
}

type structArgsModel struct {
	Name     string    `redis:"name"`
	Age      int       `redis:"age,omitempty"`
	Tags     []string  `redis:"tags,omitempty"`
	Seen     time.Time `redis:"seen,omitempty"`
	Untagged string
	Skipped  string `redis:"-"`
	NoName   string `redis:",omitempty"`
	hidden   string `redis:"hidden"`
}

func TestAppendArgStructFields(t *testing.T) {
	ctx := context.Background()
	seen := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name string
		arg  interface{}
		want []interface{}
	}{
		{
			name: "all_set",
			arg:  structArgsModel{Name: "a", Age: 3, Tags: []string{"x"}, Seen: seen, Untagged: "u", Skipped: "s", NoName: "n", hidden: "h"},
			want: []interface{}{"name", "a", "age", 3, "tags", []string{"x"}, "seen", seen},
		},
		{
			name: "omitempty_skips_zero_values",
			arg:  structArgsModel{Name: ""},
			want: []interface{}{"name", ""},
		},
		{
			name: "pointer",
			arg:  &structArgsModel{Name: "b", Age: 7},
			want: []interface{}{"name", "b", "age", 7},
		},
		{
			name: "nil_pointer",
			arg:  (*structArgsModel)(nil),
			want: []interface{}{},
		},
	}

	// Run the cases twice: the second pass reads the field list of the type
	// from the cache filled by the first.
	for pass := 0; pass < 2; pass++ {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var captured Cmder
				args := captureCmdable(&captured).HSet(ctx, "key", tt.arg).Args()
				want := append([]interface{}{"hset", "key"}, tt.want...)
				if !reflect.DeepEqual(args, want) {
					t.Fatalf("pass %d: got %#v, want %#v", pass, args, want)
				}
			})
		}
	}
}

func BenchmarkAppendArgStruct(b *testing.B) {
	ctx := context.Background()
	var captured Cmder
	c := captureCmdable(&captured)
	v := &structArgsModel{Name: "alice", Age: 30, Tags: []string{"admin"}, Seen: time.Unix(1700000000, 0)}
	b.ReportAllocs()
	for b.Loop() {
		c.HSet(ctx, "user:1", v)
	}
}
