package redis

import (
	"context"
	"reflect"
	"testing"
)

// Clone must carry every field of the value, not only the ones that existed
// when the method was written.
func TestCloneKeepsAllValueFields(t *testing.T) {
	ctx := context.Background()
	msg := XMessage{
		ID:                        "1-0",
		Values:                    map[string]interface{}{"k": "v"},
		MillisElapsedFromDelivery: 1500,
		DeliveredCount:            2,
	}

	t.Run("XMessageSliceCmd", func(t *testing.T) {
		cmd := NewXMessageSliceCmd(ctx)
		cmd.SetVal([]XMessage{msg})
		got := cmd.Clone().(*XMessageSliceCmd).Val()
		if !reflect.DeepEqual(got, cmd.Val()) {
			t.Errorf("got %+v, want %+v", got, cmd.Val())
		}
	})

	t.Run("XStreamSliceCmd", func(t *testing.T) {
		cmd := NewXStreamSliceCmd(ctx)
		cmd.SetVal([]XStream{{Stream: "s", Messages: []XMessage{msg}}})
		got := cmd.Clone().(*XStreamSliceCmd).Val()
		if !reflect.DeepEqual(got, cmd.Val()) {
			t.Errorf("got %+v, want %+v", got, cmd.Val())
		}
	})

	t.Run("XInfoStreamCmd", func(t *testing.T) {
		cmd := NewXInfoStreamCmd(ctx, "s")
		cmd.SetVal(&XInfoStream{
			Length: 1, FirstEntry: msg, LastEntry: msg,
			IDMPDuration: 100, IDMPMaxSize: 200, PIDsTracked: 3, IIDsTracked: 4, IIDsAdded: 5, IIDsDuplicates: 6,
		})
		got := cmd.Clone().(*XInfoStreamCmd).Val()
		if !reflect.DeepEqual(got, cmd.Val()) {
			t.Errorf("got %+v, want %+v", got, cmd.Val())
		}
		got.FirstEntry.Values["k"] = "changed"
		if cmd.Val().FirstEntry.Values["k"] != "v" {
			t.Error("clone shares the FirstEntry values map with the original")
		}
	})

	t.Run("ClientInfoCmd", func(t *testing.T) {
		cmd := NewClientInfoCmd(ctx)
		cmd.SetVal(&ClientInfo{ID: 7, TotalNetIn: 10, TotalNetOut: 20, TotalCmds: 30})
		got := cmd.Clone().(*ClientInfoCmd).Val()
		if !reflect.DeepEqual(got, cmd.Val()) {
			t.Errorf("got %+v, want %+v", got, cmd.Val())
		}
	})

	t.Run("ACLLogCmd", func(t *testing.T) {
		cmd := NewACLLogCmd(ctx)
		cmd.SetVal([]*ACLLogEntry{{
			Count:      1,
			ClientInfo: &ClientInfo{ID: 7, TotalNetIn: 10, TotalNetOut: 20, TotalCmds: 30},
		}})
		got := cmd.Clone().(*ACLLogCmd).Val()
		if !reflect.DeepEqual(got, cmd.Val()) {
			t.Errorf("got %+v, want %+v", got[0].ClientInfo, cmd.Val()[0].ClientInfo)
		}
		if got[0].ClientInfo == cmd.Val()[0].ClientInfo {
			t.Error("clone shares ClientInfo with the original")
		}
	})

	t.Run("FTSearchCmd", func(t *testing.T) {
		cmd := newFTSearchCmd(ctx, nil)
		cmd.SetVal(FTSearchResult{Total: 1, Docs: []Document{{ID: "doc:1", Error: Nil}}})
		got := cmd.Clone().(*FTSearchCmd).Val()
		if !reflect.DeepEqual(got, cmd.Val()) {
			t.Errorf("got %+v, want %+v", got, cmd.Val())
		}
	})
}
