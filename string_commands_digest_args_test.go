package redis

import (
	"context"
	"reflect"
	"testing"
)

// A digest of zero is a valid xxh3 value: SetArgs and DelExArgs must send it
// like SetIFDEQ does, not drop the operand of IFDEQ/IFDNE.
func TestDigestConditionSendsZeroDigest(t *testing.T) {
	ctx := context.Background()
	var got Cmder
	c := cmdable(func(_ context.Context, cmd Cmder) error { got = cmd; return nil })
	const zero = "0000000000000000"

	cases := []struct {
		name string
		run  func()
		want []interface{}
	}{
		{
			"SetArgs IFDEQ GET",
			func() { c.SetArgs(ctx, "k", "v", SetArgs{Mode: "IFDEQ", Get: true}) },
			[]interface{}{"set", "k", "v", "IFDEQ", zero, "get"},
		},
		{
			"SetArgs IFDNE",
			func() { c.SetArgs(ctx, "k", "v", SetArgs{Mode: "IFDNE"}) },
			[]interface{}{"set", "k", "v", "IFDNE", zero},
		},
		{
			"DelExArgs IFDEQ",
			func() { c.DelExArgs(ctx, "k", DelExArgs{Mode: "IFDEQ"}) },
			[]interface{}{"delex", "k", "IFDEQ", zero},
		},
	}
	for _, tc := range cases {
		tc.run()
		if !reflect.DeepEqual(got.Args(), tc.want) {
			t.Errorf("%s: args = %v, want %v", tc.name, got.Args(), tc.want)
		}
	}
}
