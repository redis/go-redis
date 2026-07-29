package redis

import (
	"context"
	"reflect"
	"testing"
)

// TestFTAliasListArgs pins the wire format of FT.ALIASLIST: exactly one
// argument, the index name, with no optional arguments.
func TestFTAliasListArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		call func(c cmdable) Cmder
		want []interface{}
	}{
		{
			name: "index_only",
			call: func(c cmdable) Cmder { return c.FTAliasList(ctx, "idx") },
			want: []interface{}{"FT.ALIASLIST", "idx"},
		},
		{
			name: "index_passed_verbatim",
			call: func(c cmdable) Cmder { return c.FTAliasList(ctx, "My-Index:v2") },
			want: []interface{}{"FT.ALIASLIST", "My-Index:v2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := tt.call(c)
			if cmd == nil {
				t.Fatalf("command builder returned nil")
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}
