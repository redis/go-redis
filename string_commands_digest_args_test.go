package redis

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// A zero MatchDigest cannot be told apart from an unset one, so SetArgs and
// DelExArgs must fail without sending the command.
func TestDigestConditionRejectsZeroDigest(t *testing.T) {
	ctx := context.Background()
	sent := 0
	c := cmdable(func(_ context.Context, _ Cmder) error { sent++; return nil })

	cases := []struct {
		name string
		run  func() error
	}{
		{"SetArgs IFDEQ GET", func() error { return c.SetArgs(ctx, "k", "v", SetArgs{Mode: "IFDEQ", Get: true}).Err() }},
		{"SetArgs IFDNE", func() error { return c.SetArgs(ctx, "k", "v", SetArgs{Mode: "IFDNE"}).Err() }},
		{"DelExArgs IFDEQ", func() error { return c.DelExArgs(ctx, "k", DelExArgs{Mode: "IFDEQ"}).Err() }},
		{"DelExArgs ifdne", func() error { return c.DelExArgs(ctx, "k", DelExArgs{Mode: "ifdne"}).Err() }},
	}
	for _, tc := range cases {
		if err := tc.run(); !errors.Is(err, errZeroMatchDigest) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, errZeroMatchDigest)
		}
	}
	if sent != 0 {
		t.Errorf("sent %d commands, want 0", sent)
	}
}

func TestDigestConditionSendsNonZeroDigest(t *testing.T) {
	ctx := context.Background()
	var got Cmder
	c := cmdable(func(_ context.Context, cmd Cmder) error { got = cmd; return nil })

	c.SetArgs(ctx, "k", "v", SetArgs{Mode: "IFDEQ", MatchDigest: 1, Get: true})
	if want := []interface{}{"set", "k", "v", "IFDEQ", "0000000000000001", "get"}; !reflect.DeepEqual(got.Args(), want) {
		t.Errorf("SetArgs: args = %v, want %v", got.Args(), want)
	}
	c.DelExArgs(ctx, "k", DelExArgs{Mode: "IFDNE", MatchDigest: 1})
	if want := []interface{}{"delex", "k", "IFDNE", "0000000000000001"}; !reflect.DeepEqual(got.Args(), want) {
		t.Errorf("DelExArgs: args = %v, want %v", got.Args(), want)
	}
}
