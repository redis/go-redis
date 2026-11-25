package push

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// TestProcessorConsumesTypedErrorFirstPush: a push whose first element is a
// typed Redis error (here MOVED) is consumed whole, so the caller's next read
// returns its own reply and not the rest of the push frame.
func TestProcessorConsumesTypedErrorFirstPush(t *testing.T) {
	const stream = ">2\r\n-MOVED 1 h:1\r\n:5\r\n" + "+OK\r\n"
	rd := proto.NewReader(strings.NewReader(stream))
	if err := NewProcessor().ProcessPendingNotifications(context.Background(), NotificationHandlerContext{}, rd); err != nil {
		t.Fatalf("ProcessPendingNotifications: %v", err)
	}
	reply, err := rd.ReadReply()
	if err != nil || reply != "OK" {
		t.Fatalf("next reply = %#v, %v; want \"OK\" (push frame only partly consumed)", reply, err)
	}
}
