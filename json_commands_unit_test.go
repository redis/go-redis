package redis

import (
	"context"
	"reflect"
	"testing"
)

// TestJSONGetWithArgsArgs checks that JSONGetWithArgs sends the paths with
// and without options.
func TestJSONGetWithArgsArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		options *JSONGetArgs
		paths   []string
		want    []interface{}
	}{
		{
			name:  "nil_options_with_paths",
			paths: []string{"$.a", "$.b"},
			want:  []interface{}{"JSON.GET", "key", "$.a", "$.b"},
		},
		{
			name: "nil_options_without_paths",
			want: []interface{}{"JSON.GET", "key"},
		},
		{
			name:    "empty_options_with_paths",
			options: &JSONGetArgs{},
			paths:   []string{"$.a"},
			want:    []interface{}{"JSON.GET", "key", "$.a"},
		},
		{
			name:    "options_with_paths",
			options: &JSONGetArgs{Indent: "  ", Newline: "\n", Space: " "},
			paths:   []string{"$.a"},
			want:    []interface{}{"JSON.GET", "key", "INDENT", "  ", "NEWLINE", "\n", "SPACE", " ", "$.a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			cmd := captureCmdable(&captured).JSONGetWithArgs(ctx, "key", tt.options, tt.paths...)
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}
