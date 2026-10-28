package redis

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// HOTKEYS GET answers with an array holding a single map. The CPU totals are
// named total-cpu-time-user-ms and total-cpu-time-sys-ms by the server, so the
// reply must be read under those names. RESP2 sends the same fields as a flat
// array of name/value pairs.
func TestHotKeysCmdReadReplyCPUFields(t *testing.T) {
	fields := [][2]string{
		{"tracking-active", ":1\r\n"},
		{"sample-ratio", ":1\r\n"},
		{"selected-slots", "*1\r\n*2\r\n:0\r\n:16383\r\n"},
		{"all-commands-all-slots-us", ":103\r\n"},
		{"net-bytes-all-commands-all-slots", ":2042\r\n"},
		{"collection-start-time-unix-ms", ":1770824933147\r\n"},
		{"collection-duration-ms", ":0\r\n"},
		{"total-cpu-time-user-ms", ":23\r\n"},
		{"total-cpu-time-sys-ms", ":7\r\n"},
		{"total-net-bytes", ":2038\r\n"},
		{"by-cpu-time-us", "*2\r\n" + hotKeysBulkTestString("hotkey_001_counter") + ":29\r\n"},
		{"by-net-bytes", "*2\r\n" + hotKeysBulkTestString("hotkey_001") + ":446\r\n"},
	}

	var body strings.Builder
	for _, field := range fields {
		body.WriteString(hotKeysBulkTestString(field[0]))
		body.WriteString(field[1])
	}
	resp3 := "*1\r\n%" + strconv.Itoa(len(fields)) + "\r\n" + body.String()
	resp2 := "*1\r\n*" + strconv.Itoa(2*len(fields)) + "\r\n" + body.String()

	for _, reply := range []string{resp3, resp2} {
		cmd := NewHotKeysCmd(context.Background(), "hotkeys", "get")
		if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
			t.Fatalf("readReply(%.80q): %v", reply, err)
		}

		val := cmd.Val()
		if val == nil {
			t.Fatalf("readReply(%.80q) left no result", reply)
		}
		if got := val.UsedCPUUser; got != 23*time.Millisecond {
			t.Errorf("UsedCPUUser for %.80q = %v; want 23ms", reply, got)
		}
		if got := val.UsedCPUSys; got != 7*time.Millisecond {
			t.Errorf("UsedCPUSys for %.80q = %v; want 7ms", reply, got)
		}
		if !val.TrackingActive {
			t.Errorf("TrackingActive for %.80q = false; want true", reply)
		}
		if got := val.TotalNetBytes; got != 2038 {
			t.Errorf("TotalNetBytes for %.80q = %d; want 2038", reply, got)
		}
		if got := val.AllCommandsAllSlots; got != 103*time.Microsecond {
			t.Errorf("AllCommandsAllSlots for %.80q = %v; want 103us", reply, got)
		}
		if got := val.ByCPUTime; len(got) != 1 || got[0].Key != "hotkey_001_counter" || got[0].Value != int64(29) {
			t.Errorf("ByCPUTime for %.80q = %v; want one hotkey_001_counter entry of 29", reply, got)
		}
	}
}

func hotKeysBulkTestString(s string) string {
	return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
}
