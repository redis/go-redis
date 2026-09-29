package push

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// TestVoidProcessorConsumesUnpeekablePush is the regression for the VoidProcessor
// counterpart of the built-in Processor's confirmed-push consume discipline.
//
// VoidProcessor drains out-of-band push frames on RESP3 connections that opt out
// of push handling, running before every reply read:
//
//	voidProcessor.ProcessPendingNotifications(ctx, handlerCtx, rd) // drain pushes
//	rd.ReadReply()                                                 // read the reply
//
// A push frame whose name cannot be peeked for any reason other than "too long"
// (a non-string / malformed name) was left buffered by a break, so the reply read
// below consumed the push as the command value (reply shift). The frame must be
// consumed instead.
func TestVoidProcessorConsumesUnpeekablePush(t *testing.T) {
	// ">1\r\n:5\r\n": a confirmed push whose sole element is an integer, so its
	// name is not a bulk/simple string and PeekPushNotificationName fails with a
	// non-too-long error. "+OK\r\n" is the real reply that must survive intact.
	const stream = ">1\r\n:5\r\n" + "+OK\r\n"

	rd := proto.NewReader(strings.NewReader(stream))
	vp := NewVoidProcessor()
	ctx := context.Background()
	handlerCtx := NotificationHandlerContext{}

	if err := vp.ProcessPendingNotifications(ctx, handlerCtx, rd); err != nil {
		t.Fatalf("ProcessPendingNotifications: %v", err)
	}

	reply, err := rd.ReadReply()
	if err != nil {
		t.Fatalf("ReadReply: %v", err)
	}
	if reply != "OK" {
		t.Fatalf("reply shift: got %#v, want \"OK\" (push frame was not consumed)", reply)
	}
}
