package redis

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// RESP3 sends a double as float64 and a big number as *big.Int. The Cmd
// helpers must give the same result for them as for the RESP2 bulk string.
func TestCmdHelpersRESP3NumberReply(t *testing.T) {
	helpers := map[string]func(cmd *Cmd) string{
		"Text":        func(cmd *Cmd) string { return fmt.Sprint(cmd.Text()) },
		"Int":         func(cmd *Cmd) string { return fmt.Sprint(cmd.Int()) },
		"Int64":       func(cmd *Cmd) string { return fmt.Sprint(cmd.Int64()) },
		"Uint64":      func(cmd *Cmd) string { return fmt.Sprint(cmd.Uint64()) },
		"Float32":     func(cmd *Cmd) string { return fmt.Sprint(cmd.Float32()) },
		"Float64":     func(cmd *Cmd) string { return fmt.Sprint(cmd.Float64()) },
		"StringSlice": func(cmd *Cmd) string { return fmt.Sprint(cmd.StringSlice()) },
		"Int64Slice":  func(cmd *Cmd) string { return fmt.Sprint(cmd.Int64Slice()) },
		"Uint64Slice": func(cmd *Cmd) string { return fmt.Sprint(cmd.Uint64Slice()) },
		"Bool":        func(cmd *Cmd) string { return fmt.Sprint(cmd.Bool()) },
		"BoolSlice":   func(cmd *Cmd) string { return fmt.Sprint(cmd.BoolSlice()) },
	}

	tests := []struct {
		resp3   string
		resp2   string
		helpers []string
	}{
		{",2\r\n", "$1\r\n2\r\n", []string{"Text", "Int", "Int64", "Uint64"}},
		{",1\r\n", "$1\r\n1\r\n", []string{"Bool"}},
		{",0\r\n", "$1\r\n0\r\n", []string{"Bool"}},
		{",1.5\r\n", "$3\r\n1.5\r\n", []string{"Text", "Int", "Int64", "Uint64", "Bool"}},
		{",-inf\r\n", "$4\r\n-inf\r\n", []string{"Text", "Int64"}},
		{",-0\r\n", "$2\r\n-0\r\n", []string{"Text", "Int64"}},
		{",1e+100\r\n", "$6\r\n1e+100\r\n", []string{"Text", "Int64", "Uint64"}},
		{",1e+19\r\n", "$5\r\n1e+19\r\n", []string{"Text", "Int64", "Uint64"}},
		{",4611686018427387904\r\n", "$19\r\n4611686018427387904\r\n", []string{"Text", "Int64"}},
		{",150000000000000000\r\n", "$18\r\n150000000000000000\r\n", []string{"Text", "Int64"}},
		{",0.00001\r\n", "$7\r\n0.00001\r\n", []string{"Text"}},
		{",2.5e-7\r\n", "$6\r\n2.5e-7\r\n", []string{"Text", "Int64"}},
		{"*2\r\n,2\r\n,1.5\r\n", "*2\r\n$1\r\n2\r\n$3\r\n1.5\r\n", []string{"StringSlice", "Int64Slice", "Uint64Slice"}},
		{"*2\r\n,1\r\n,0\r\n", "*2\r\n$1\r\n1\r\n$1\r\n0\r\n", []string{"BoolSlice"}},
		{"(1\r\n", "$1\r\n1\r\n", []string{"Bool"}},
		{"(42\r\n", "$2\r\n42\r\n", []string{"Text", "Int", "Int64", "Uint64", "Float32", "Float64", "Bool"}},
		{
			"(1234567999999999999999999999999999999\r\n",
			"$37\r\n1234567999999999999999999999999999999\r\n",
			[]string{"Text", "Int", "Int64", "Uint64", "Float32", "Float64"},
		},
		{"*2\r\n(42\r\n(-1\r\n", "*2\r\n$2\r\n42\r\n$2\r\n-1\r\n", []string{"StringSlice", "Int64Slice", "Uint64Slice"}},
		{"*2\r\n(1\r\n(0\r\n", "*2\r\n$1\r\n1\r\n$1\r\n0\r\n", []string{"BoolSlice"}},
	}

	for _, tt := range tests {
		resp3 := NewCmd(context.Background())
		if err := resp3.readReply(proto.NewReader(strings.NewReader(tt.resp3))); err != nil {
			t.Fatalf("readReply(%q): %v", tt.resp3, err)
		}
		resp2 := NewCmd(context.Background())
		if err := resp2.readReply(proto.NewReader(strings.NewReader(tt.resp2))); err != nil {
			t.Fatalf("readReply(%q): %v", tt.resp2, err)
		}
		for _, name := range tt.helpers {
			if got, want := helpers[name](resp3), helpers[name](resp2); got != want {
				t.Errorf("%s() for %q = %s; want %s", name, tt.resp3, got, want)
			}
		}
	}
}
