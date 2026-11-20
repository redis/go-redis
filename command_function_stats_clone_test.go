package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestFunctionStatsCmdCloneCopiesRunningScriptCommand(t *testing.T) {
	reply := "%2\r\n" +
		"$14\r\nrunning_script\r\n" +
		"%3\r\n" +
		"$4\r\nname\r\n$4\r\nloop\r\n" +
		"$7\r\ncommand\r\n*2\r\n$5\r\nfcall\r\n$4\r\nloop\r\n" +
		"$11\r\nduration_ms\r\n:5\r\n" +
		"$7\r\nengines\r\n%0\r\n"

	cmd := NewFunctionStatsCmd(context.Background(), "function", "stats")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
		t.Fatalf("readReply() returned unexpected error: %v", err)
	}

	clone := cmd.Clone().(*FunctionStatsCmd).Val()
	rs, _ := clone.RunningScript()
	rs.Command[0] = "changed"

	val := cmd.Val()
	if got, _ := val.RunningScript(); got.Command[0] != "fcall" {
		t.Errorf("changing the clone changed the original: %q", got.Command[0])
	}
}
