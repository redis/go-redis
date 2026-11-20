package redis

import (
	"context"
	"reflect"
	"testing"
)

func TestXInfoStreamFullCmdClone(t *testing.T) {
	newVal := func() *XInfoStreamFull {
		return &XInfoStreamFull{
			Length:               1,
			RadixTreeKeys:        2,
			RadixTreeNodes:       3,
			LastGeneratedID:      "1-0",
			MaxDeletedEntryID:    "0-1",
			EntriesAdded:         4,
			Entries:              []XMessage{{ID: "1-0", Values: map[string]interface{}{"k": "v"}}},
			RecordedFirstEntryID: "1-0",
			IDMPDuration:         100,
			IDMPMaxSize:          200,
			PIDsTracked:          5,
			IIDsTracked:          6,
			IIDsAdded:            7,
			IIDsDuplicates:       8,
			Groups: []XInfoStreamGroup{{
				Name:    "g",
				Pending: []XInfoStreamGroupPending{{ID: "1-0", Consumer: "c"}},
				Consumers: []XInfoStreamConsumer{{
					Name:    "c",
					Pending: []XInfoStreamConsumerPending{{ID: "1-0"}},
				}},
			}},
		}
	}

	cmd := NewXInfoStreamFullCmd(context.Background(), "xinfo", "stream", "s", "full")
	cmd.SetVal(newVal())
	clone := cmd.Clone().(*XInfoStreamFullCmd).Val()

	if !reflect.DeepEqual(clone, newVal()) {
		t.Fatalf("Clone() = %+v; want %+v", *clone, *newVal())
	}

	clone.Entries[0].Values["k"] = "changed"
	clone.Groups[0].Pending[0].ID = "changed"
	clone.Groups[0].Consumers[0].Name = "changed"
	clone.Groups[0].Consumers[0].Pending[0].ID = "changed"
	if !reflect.DeepEqual(cmd.Val(), newVal()) {
		t.Fatalf("changing the clone changed the original: %+v", *cmd.Val())
	}
}
