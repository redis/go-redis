package redis

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestLatencyHistory_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	c.LatencyHistory(context.Background(), "command")
	if want := []any{"latency", "history", "command"}; !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
}

func TestLatencyHistoryCmd_readReply(t *testing.T) {
	want := []LatencySample{
		{Time: time.Unix(1795191463, 0), Latency: 118 * time.Millisecond},
		{Time: time.Unix(1795191464, 0), Latency: 354 * time.Millisecond},
	}

	// The reply of Redis 8.10 to LATENCY HISTORY, the same in RESP2 and RESP3.
	const reply = "*2\r\n*2\r\n:1795191463\r\n:118\r\n*2\r\n:1795191464\r\n:354\r\n"

	cmd := NewLatencyHistoryCmd(context.Background(), "latency", "history", "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cmd.Val(), want) {
		t.Errorf("val = %+v, want %+v", cmd.Val(), want)
	}

	clone := cmd.Clone().(*LatencyHistoryCmd)
	clone.Val()[0].Latency = 0
	if !reflect.DeepEqual(cmd.Val(), want) {
		t.Errorf("val changed with its clone: %+v", cmd.Val())
	}

	t.Run("unknown event", func(t *testing.T) {
		cmd := NewLatencyHistoryCmd(context.Background(), "latency", "history", "nope")
		if err := cmd.readReply(proto.NewReader(strings.NewReader("*0\r\n"))); err != nil {
			t.Fatal(err)
		}
		if len(cmd.Val()) != 0 {
			t.Errorf("val = %+v, want no samples", cmd.Val())
		}
	})

	t.Run("short sample", func(t *testing.T) {
		cmd := NewLatencyHistoryCmd(context.Background(), "latency", "history", "command")
		if err := cmd.readReply(proto.NewReader(strings.NewReader("*1\r\n*1\r\n:1\r\n"))); err == nil {
			t.Error("short sample accepted")
		}
	})
}
