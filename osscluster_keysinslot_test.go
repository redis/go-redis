package redis

import (
	"context"
	"testing"
)

// CLUSTER GETKEYSINSLOT / COUNTKEYSINSLOT carry the slot as an argument, so
// the slot must be read from it however the caller typed it (raw Do calls
// often pass strings or int64) and short arg lists must not panic.
func TestClusterCmdSlotKeysInSlotArgTypes(t *testing.T) {
	ctx := context.Background()
	client := &ClusterClient{}

	tests := []struct {
		name string
		args []interface{}
		want int
	}{
		{"getkeysinslot int", []interface{}{"cluster", "getkeysinslot", 100, 10}, 100},
		{"countkeysinslot int", []interface{}{"cluster", "countkeysinslot", 100}, 100},
		{"countkeysinslot string", []interface{}{"cluster", "countkeysinslot", "100"}, 100},
		{"getkeysinslot string", []interface{}{"cluster", "getkeysinslot", "100", "10"}, 100},
		{"countkeysinslot int64", []interface{}{"cluster", "countkeysinslot", int64(100)}, 100},
		{"countkeysinslot uint16", []interface{}{"cluster", "countkeysinslot", uint16(100)}, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("cmdSlot panicked: %v", r)
				}
			}()
			cmd := NewCmd(ctx, tt.args...)
			if got := client.cmdSlot(cmd, -1); got != tt.want {
				t.Fatalf("cmdSlot() = %d, want %d", got, tt.want)
			}
		})
	}

	for _, args := range [][]interface{}{
		{"cluster"},
		{"cluster", "countkeysinslot"},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("cmdSlot(%v) panicked: %v", args, r)
				}
			}()
			client.cmdSlot(NewCmd(ctx, args...), -1)
		}()
	}
}
