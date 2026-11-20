package redis

import (
	"context"
	"testing"
)

func TestVectorSliceCmdCloneCopiesVal(t *testing.T) {
	ctx := context.Background()
	attribs := func() *string { s := "{}"; return &s }

	t.Run("VectorScoreSliceCmd", func(t *testing.T) {
		cmd := NewVectorInfoSliceCmd(ctx, "vsim")
		cmd.SetVal([]VectorScore{{Name: "a", Score: 1}})
		clone := cmd.Clone().(*VectorScoreSliceCmd).Val()
		clone[0].Name = "changed"
		if got := cmd.Val()[0].Name; got != "a" {
			t.Errorf("changing the clone changed the original: %q", got)
		}
	})

	t.Run("VectorAttribSliceCmd", func(t *testing.T) {
		cmd := NewVectorAttribSliceCmd(ctx, "vsim")
		cmd.SetVal([]VectorAttrib{{Name: "a", Attribs: attribs()}})
		clone := cmd.Clone().(*VectorAttribSliceCmd).Val()
		clone[0].Name = "changed"
		*clone[0].Attribs = "changed"
		if got := cmd.Val()[0]; got.Name != "a" || *got.Attribs != "{}" {
			t.Errorf("changing the clone changed the original: %q %q", got.Name, *got.Attribs)
		}
	})

	t.Run("VectorScoreAttribSliceCmd", func(t *testing.T) {
		cmd := NewVectorScoreAttribSliceCmd(ctx, "vsim")
		cmd.SetVal([]VectorScoreAttrib{{Name: "a", Score: 1, Attribs: attribs()}})
		clone := cmd.Clone().(*VectorScoreAttribSliceCmd).Val()
		clone[0].Name = "changed"
		*clone[0].Attribs = "changed"
		if got := cmd.Val()[0]; got.Name != "a" || *got.Attribs != "{}" {
			t.Errorf("changing the clone changed the original: %q %q", got.Name, *got.Attribs)
		}
	})

	// An empty reply is a non-nil empty slice and must stay one.
	t.Run("empty", func(t *testing.T) {
		scores := NewVectorInfoSliceCmd(ctx, "vsim")
		scores.SetVal([]VectorScore{})
		attrs := NewVectorAttribSliceCmd(ctx, "vsim")
		attrs.SetVal([]VectorAttrib{})
		scoreAttrs := NewVectorScoreAttribSliceCmd(ctx, "vsim")
		scoreAttrs.SetVal([]VectorScoreAttrib{})

		if scores.Clone().(*VectorScoreSliceCmd).Val() == nil ||
			attrs.Clone().(*VectorAttribSliceCmd).Val() == nil ||
			scoreAttrs.Clone().(*VectorScoreAttribSliceCmd).Val() == nil {
			t.Error("clone of an empty result is nil")
		}
	})
}
