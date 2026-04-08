package redis

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestVectorFP32_Value(t *testing.T) {
	v := &VectorFP32{Val: []byte{1, 2, 3}}
	got := v.Value()
	want := []any{"FP32", []byte{1, 2, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("VectorFP32.Value() = %v, want %v", got, want)
	}
}

func TestVectorValues_Value(t *testing.T) {
	v := &VectorValues{Val: []float64{1.1, 2.2}}
	got := v.Value()
	want := []any{"Values", 2, 1.1, 2.2}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("VectorValues.Value() = %v, want %v", got, want)
	}
}

func TestVectorRef_Value(t *testing.T) {
	v := &VectorRef{Name: "foo"}
	got := v.Value()
	want := []any{"ele", "foo"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("VectorRef.Value() = %v, want %v", got, want)
	}
}

func TestVAdd(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	c.VAdd(context.Background(), "k", "e", vec)
	cmd, ok := m.lastCmd.(*BoolCmd)
	if !ok {
		t.Fatalf("expected BoolCmd, got %T", m.lastCmd)
	}
	if cmd.args[0] != "vadd" || cmd.args[1] != "k" || cmd.args[len(cmd.args)-1] != "e" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVAddWithArgs_AllOptions(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VAddArgs{Reduce: 3, Cas: true, NoQuant: true, EF: 5, SetAttr: "attr", M: 2}
	c.VAddWithArgs(context.Background(), "k", "e", vec, args)
	cmd := m.lastCmd.(*BoolCmd)
	found := map[string]bool{}
	for _, a := range cmd.args {
		if s, ok := a.(string); ok {
			found[s] = true
		}
	}
	for _, want := range []string{"reduce", "cas", "noquant", "ef", "setattr", "m"} {
		if !found[want] {
			t.Errorf("missing arg: %s", want)
		}
	}
}

func TestVCard(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VCard(context.Background(), "k")
	cmd := m.lastCmd.(*IntCmd)
	if cmd.args[0] != "vcard" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVDim(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VDim(context.Background(), "k")
	cmd := m.lastCmd.(*IntCmd)
	if cmd.args[0] != "vdim" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVEmb(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VEmb(context.Background(), "k", "e", true)
	cmd := m.lastCmd.(*SliceCmd)
	if cmd.args[0] != "vemb" || cmd.args[1] != "k" || cmd.args[2] != "e" || cmd.args[3] != "raw" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVGetAttr(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VGetAttr(context.Background(), "k", "e")
	cmd := m.lastCmd.(*StringCmd)
	if cmd.args[0] != "vgetattr" || cmd.args[1] != "k" || cmd.args[2] != "e" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVInfo(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VInfo(context.Background(), "k")
	cmd := m.lastCmd.(*MapStringInterfaceCmd)
	if cmd.args[0] != "vinfo" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVLinks(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VLinks(context.Background(), "k", "e")
	cmd := m.lastCmd.(*StringSliceCmd)
	if cmd.args[0] != "vlinks" || cmd.args[1] != "k" || cmd.args[2] != "e" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVLinksWithScores(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VLinksWithScores(context.Background(), "k", "e")
	cmd := m.lastCmd.(*VectorScoreSliceCmd)
	if cmd.args[0] != "vlinks" || cmd.args[1] != "k" || cmd.args[2] != "e" || cmd.args[3] != "withscores" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVRandMember(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VRandMember(context.Background(), "k")
	cmd := m.lastCmd.(*StringCmd)
	if cmd.args[0] != "vrandmember" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVRandMemberCount(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VRandMemberCount(context.Background(), "k", 5)
	cmd := m.lastCmd.(*StringSliceCmd)
	if cmd.args[0] != "vrandmember" || cmd.args[1] != "k" || cmd.args[2] != 5 {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVRem(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VRem(context.Background(), "k", "e")
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[0] != "vrem" || cmd.args[1] != "k" || cmd.args[2] != "e" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVSetAttr_String(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VSetAttr(context.Background(), "k", "e", "foo")
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[0] != "vsetattr" || cmd.args[1] != "k" || cmd.args[2] != "e" || cmd.args[3] != "foo" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVSetAttr_Bytes(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VSetAttr(context.Background(), "k", "e", []byte("bar"))
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[3] != "bar" {
		t.Errorf("expected 'bar', got %v", cmd.args[3])
	}
}

func TestVSetAttr_MarshalStruct(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	val := struct{ A int }{A: 1}
	c.VSetAttr(context.Background(), "k", "e", val)
	cmd := m.lastCmd.(*BoolCmd)
	want, _ := json.Marshal(val)
	if cmd.args[3] != string(want) {
		t.Errorf("expected marshalled struct, got %v", cmd.args[3])
	}
}

func TestVSetAttr_MarshalError(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	bad := func() {}
	cmd := c.VSetAttr(context.Background(), "k", "e", bad)
	if cmd.Err() == nil {
		t.Error("expected error for non-marshallable value")
	}
}

func TestVClearAttributes(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VClearAttributes(context.Background(), "k", "e")
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[0] != "vsetattr" || cmd.args[3] != "" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVSim(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	c.VSim(context.Background(), "k", vec)
	cmd := m.lastCmd.(*StringSliceCmd)
	if cmd.args[0] != "vsim" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVSimWithScores(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	c.VSimWithScores(context.Background(), "k", vec)
	cmd := m.lastCmd.(*VectorScoreSliceCmd)
	if cmd.args[0] != "vsim" || cmd.args[1] != "k" || cmd.args[len(cmd.args)-1] != "withscores" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVSimWithArgs_AllOptions(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VSimArgs{Count: 2, EF: 3, Filter: "f", FilterEF: 4, Truth: true, NoThread: true}
	c.VSimWithArgs(context.Background(), "k", vec, args)
	cmd := m.lastCmd.(*StringSliceCmd)
	found := map[string]bool{}
	for _, a := range cmd.args {
		if s, ok := a.(string); ok {
			found[s] = true
		}
	}
	for _, want := range []string{"count", "ef", "filter", "filter-ef", "truth", "nothread"} {
		if !found[want] {
			t.Errorf("missing arg: %s", want)
		}
	}
}

func TestVSimWithArgsWithScores_AllOptions(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VSimArgs{Count: 2, EF: 3, Filter: "f", FilterEF: 4, Truth: true, NoThread: true, Epsilon: 0.5}
	c.VSimWithArgsWithScores(context.Background(), "k", vec, args)
	cmd := m.lastCmd.(*VectorScoreSliceCmd)
	found := map[string]bool{}
	for _, a := range cmd.args {
		if s, ok := a.(string); ok {
			found[s] = true
		}
	}
	for _, want := range []string{"count", "ef", "filter", "filter-ef", "truth", "nothread", "epsilon", "withscores"} {
		if !found[want] {
			t.Errorf("missing arg: %s", want)
		}
	}
}

func TestVSimWithArgsWithAttribs_AllOptions(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VSimArgs{Count: 2, EF: 3, Filter: "f", FilterEF: 4, Truth: true, NoThread: true, Epsilon: 0.5}

	c.VSimWithArgsWithAttribs(context.Background(), "k", vec, args)
	cmd := m.lastCmd.(*VectorAttribSliceCmd)

	found := map[string]bool{}
	for _, a := range cmd.args {
		if s, ok := a.(string); ok {
			found[s] = true
		}
	}

	for _, want := range []string{"count", "ef", "filter", "filter-ef", "truth", "nothread", "epsilon", "withattribs"} {
		if !found[want] {
			t.Errorf("missing arg: %s", want)
		}
	}
	if found["withscores"] {
		t.Error("unexpected arg: withscores")
	}
}

func TestVSimWithArgsWithAttribs_NilArgs(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}

	c.VSimWithArgsWithAttribs(context.Background(), "k", vec, nil)
	cmd := m.lastCmd.(*VectorAttribSliceCmd)

	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "withattribs" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing withattribs arg")
	}
}

func TestVSimWithArgsWithScoresWithAttribs_AllOptions(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VSimArgs{Count: 2, EF: 3, Filter: "f", FilterEF: 4, Truth: true, NoThread: true, Epsilon: 0.5}

	c.VSimWithArgsWithScoresWithAttribs(context.Background(), "k", vec, args)
	cmd := m.lastCmd.(*VectorScoreAttribSliceCmd)

	found := map[string]bool{}
	for _, a := range cmd.args {
		if s, ok := a.(string); ok {
			found[s] = true
		}
	}

	for _, want := range []string{"count", "ef", "filter", "filter-ef", "truth", "nothread", "epsilon", "withscores", "withattribs"} {
		if !found[want] {
			t.Errorf("missing arg: %s", want)
		}
	}
}

func TestVSimWithArgsWithScoresWithAttribs_NilArgs(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}

	c.VSimWithArgsWithScoresWithAttribs(context.Background(), "k", vec, nil)
	cmd := m.lastCmd.(*VectorScoreAttribSliceCmd)

	foundScores := false
	foundAttribs := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "withscores" {
			foundScores = true
		}
		if s, ok := a.(string); ok && s == "withattribs" {
			foundAttribs = true
		}
	}
	if !foundScores {
		t.Error("missing withscores arg")
	}
	if !foundAttribs {
		t.Error("missing withattribs arg")
	}
}

func TestVectorScoreAttribSliceCmd_readReply(t *testing.T) {
	t.Run("resp3FlatArray", func(t *testing.T) {
		// *6 -> two (name, score, attrib) triplets; second attrib is nil
		reply := "*6\r\n" +
			"$2\r\na1\r\n,1.5\r\n$4\r\nattr\r\n" +
			"$2\r\na2\r\n,2.5\r\n_\r\n"
		cmd := NewVectorScoreAttribSliceCmd(context.Background())
		rd := proto.NewReader(strings.NewReader(reply))
		if err := cmd.readReply(rd); err != nil {
			t.Fatal(err)
		}

		if len(cmd.val) != 2 {
			t.Fatalf("len(val) = %d, want 2", len(cmd.val))
		}

		if cmd.val[0].Name != "a1" || cmd.val[0].Score != 1.5 || cmd.val[0].Attribs == nil || *cmd.val[0].Attribs != "attr" {
			t.Errorf("first row = %+v, want {a1 1.5 %q}", cmd.val[0], "attr")
		}

		if cmd.val[1].Name != "a2" || cmd.val[1].Score != 2.5 || cmd.val[1].Attribs != nil {
			t.Errorf("second row = %+v, want {a2 2.5 nil}", cmd.val[1])
		}
	})

	t.Run("resp3Map", func(t *testing.T) {
		// %2 -> two map entries; value each is *2 [score, attrib]
		reply := "%2\r\n" +
			"$2\r\nb1\r\n*2\r\n,0.5\r\n$1\r\na\r\n" +
			"$2\r\nb2\r\n*2\r\n,1\r\n_\r\n"
		cmd := NewVectorScoreAttribSliceCmd(context.Background())
		rd := proto.NewReader(strings.NewReader(reply))
		if err := cmd.readReply(rd); err != nil {
			t.Fatal(err)
		}

		if len(cmd.val) != 2 {
			t.Fatalf("len(val) = %d, want 2", len(cmd.val))
		}

		if cmd.val[0].Name != "b1" || cmd.val[0].Score != 0.5 || cmd.val[0].Attribs == nil || *cmd.val[0].Attribs != "a" {
			t.Errorf("first row = %+v, want {b1 0.5 %q}", cmd.val[0], "a")
		}

		if cmd.val[1].Name != "b2" || cmd.val[1].Score != 1 || cmd.val[1].Attribs != nil {
			t.Errorf("second row = %+v, want {b2 1 nil}", cmd.val[1])
		}
	})

	t.Run("flatArrayLenNotMultipleOf3", func(t *testing.T) {
		cmd := NewVectorScoreAttribSliceCmd(context.Background())
		rd := proto.NewReader(strings.NewReader("*1\r\n$1\r\nx\r\n"))
		if err := cmd.readReply(rd); err == nil {
			t.Fatal("expected error for array length not divisible by 3")
		}
	})
}

// Additional tests for missing coverage

func TestVectorValues_EmptySlice(t *testing.T) {
	v := &VectorValues{Val: []float64{}}
	got := v.Value()
	want := []any{"Values", 0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("VectorValues.Value() with empty slice = %v, want %v", got, want)
	}
}

func TestVEmb_WithoutRaw(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VEmb(context.Background(), "k", "e", false)
	cmd := m.lastCmd.(*SliceCmd)
	if cmd.args[0] != "vemb" || cmd.args[1] != "k" || cmd.args[2] != "e" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
	if len(cmd.args) != 3 {
		t.Errorf("expected 3 args when raw=false, got %d", len(cmd.args))
	}
}

func TestVAddWithArgs_Q8Option(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VAddArgs{Q8: true}
	c.VAddWithArgs(context.Background(), "k", "e", vec, args)
	cmd := m.lastCmd.(*BoolCmd)
	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "q8" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing q8 arg")
	}
}

func TestVAddWithArgs_BinOption(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VAddArgs{Bin: true}
	c.VAddWithArgs(context.Background(), "k", "e", vec, args)
	cmd := m.lastCmd.(*BoolCmd)
	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "bin" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing bin arg")
	}
}

func TestVAddWithArgs_NilArgs(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	c.VAddWithArgs(context.Background(), "k", "e", vec, nil)
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[0] != "vadd" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVSimWithArgs_NilArgs(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	c.VSimWithArgs(context.Background(), "k", vec, nil)
	cmd := m.lastCmd.(*StringSliceCmd)
	if cmd.args[0] != "vsim" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVSimWithArgsWithScores_NilArgs(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	c.VSimWithArgsWithScores(context.Background(), "k", vec, nil)
	cmd := m.lastCmd.(*VectorScoreSliceCmd)
	if cmd.args[0] != "vsim" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
	// Should still have withscores
	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "withscores" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing withscores arg")
	}
}

func TestVAdd_WithVectorFP32(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorFP32{Val: []byte{1, 2, 3, 4}}
	c.VAdd(context.Background(), "k", "e", vec)
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[0] != "vadd" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
	// Check that FP32 format is used
	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "FP32" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing FP32 format in args")
	}
}

func TestVAdd_WithVectorRef(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorRef{Name: "ref-vector"}
	c.VAdd(context.Background(), "k", "e", vec)
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[0] != "vadd" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
	// Check that ele format is used
	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "ele" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing ele format in args")
	}
}

func TestVSim_WithVectorFP32(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorFP32{Val: []byte{1, 2, 3, 4}}
	c.VSim(context.Background(), "k", vec)
	cmd := m.lastCmd.(*StringSliceCmd)
	if cmd.args[0] != "vsim" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
	// Check that FP32 format is used
	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "FP32" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing FP32 format in args")
	}
}

func TestVSim_WithVectorRef(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorRef{Name: "ref-vector"}
	c.VSim(context.Background(), "k", vec)
	cmd := m.lastCmd.(*StringSliceCmd)
	if cmd.args[0] != "vsim" || cmd.args[1] != "k" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
	// Check that ele format is used
	found := false
	for _, a := range cmd.args {
		if s, ok := a.(string); ok && s == "ele" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing ele format in args")
	}
}

func TestVAddWithArgs_ReduceOption(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VAddArgs{Reduce: 128}
	c.VAddWithArgs(context.Background(), "k", "e", vec, args)
	cmd := m.lastCmd.(*BoolCmd)
	// Check that reduce appears early in args (after key)
	if cmd.args[0] != "vadd" || cmd.args[1] != "k" || cmd.args[2] != "reduce" {
		t.Errorf("unexpected args order: %v", cmd.args)
	}
}

func TestVAddWithArgs_ZeroValues(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	vec := &VectorValues{Val: []float64{1, 2}}
	args := &VAddArgs{Reduce: 0, EF: 0, M: 0} // Zero values should not appear in args
	c.VAddWithArgs(context.Background(), "k", "e", vec, args)
	cmd := m.lastCmd.(*BoolCmd)
	// Check that zero values don't appear
	for _, a := range cmd.args {
		if s, ok := a.(string); ok {
			if s == "reduce" || s == "ef" || s == "m" {
				t.Errorf("zero value option should not appear in args: %s", s)
			}
		}
	}
}

func TestVSimArgs_IndividualOptions(t *testing.T) {
	tests := []struct {
		name string
		args *VSimArgs
		want string
	}{
		{"Count", &VSimArgs{Count: 5}, "count"},
		{"EF", &VSimArgs{EF: 10}, "ef"},
		{"Filter", &VSimArgs{Filter: "test"}, "filter"},
		{"FilterEF", &VSimArgs{FilterEF: 15}, "filter-ef"},
		{"Truth", &VSimArgs{Truth: true}, "truth"},
		{"NoThread", &VSimArgs{NoThread: true}, "nothread"},
		{"Epsilon", &VSimArgs{Epsilon: 0.5}, "epsilon"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mockCmdable{}
			c := m.asCmdable()
			vec := &VectorValues{Val: []float64{1, 2}}
			c.VSimWithArgs(context.Background(), "k", vec, tt.args)
			cmd := m.lastCmd.(*StringSliceCmd)
			found := false
			for _, a := range cmd.args {
				if s, ok := a.(string); ok && s == tt.want {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("missing arg: %s", tt.want)
			}
		})
	}
}

func TestVRange(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VRange(context.Background(), "k", "-", "+", int64(10))
	cmd := m.lastCmd.(*StringSliceCmd)
	if cmd.args[0] != "vrange" || cmd.args[1] != "k" || cmd.args[2] != "-" || cmd.args[3] != "+" || cmd.args[4] != int64(10) {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}

func TestVIsMember(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()
	c.VIsMember(context.Background(), "k", "e")
	cmd := m.lastCmd.(*BoolCmd)
	if cmd.args[0] != "vismember" || cmd.args[1] != "k" || cmd.args[2] != "e" {
		t.Errorf("unexpected args: %v", cmd.args)
	}
}
