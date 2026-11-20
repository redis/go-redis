package redis

import (
	"context"
	"reflect"
	"testing"
)

// FCall and FCallRO expand a single slice argument into its elements, like
// Eval. Every other argument, a nil one included, is sent as it is.
func TestFCallExpandsSliceArg(t *testing.T) {
	ctx := context.Background()
	var c cmdable = func(context.Context, Cmder) error { return nil }
	keys := []string{"k"}
	var nilString *string
	var nilInt *int

	tests := []struct {
		name string
		args []interface{}
		want []interface{}
	}{
		{"strings", []interface{}{[]string{"a", "b"}}, []interface{}{"a", "b"}},
		{"interfaces", []interface{}{[]interface{}{"a", 1}}, []interface{}{"a", 1}},
		{"variadic", []interface{}{"a", "b"}, []interface{}{"a", "b"}},
		{"single", []interface{}{"a"}, []interface{}{"a"}},
		{"none", nil, nil},
		{"nil", []interface{}{nil}, []interface{}{nil}},
		{"nil string pointer", []interface{}{nilString}, []interface{}{nilString}},
		{"nil int pointer", []interface{}{nilInt}, []interface{}{nilInt}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := append([]interface{}{"fn", 1, "k"}, tt.want...)
			for _, cmd := range []*Cmd{
				c.FCall(ctx, "fn", keys, tt.args...),
				c.FCallRO(ctx, "fn", keys, tt.args...),
			} {
				if got := cmd.Args()[1:]; !reflect.DeepEqual(got, want) {
					t.Errorf("%s args = %v, want %v", cmd.Name(), got, want)
				}
			}
		})
	}
}
