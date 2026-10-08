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
// Only the "name too long" peek error consumed the frame. A buffered push with a
// malformed header (a non-string name) was left at the buffer head by a break, so
// the reply read below took the push as the command value (reply shift). The frame
// is buffered and unpeekable, so it must be consumed instead.
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

// readTimeoutError is a net.Error-shaped read timeout.
type readTimeoutError struct{}

func (readTimeoutError) Error() string { return "i/o timeout" }
func (readTimeoutError) Timeout() bool { return true }

// prefixThenErrReader hands out prefix once, then fails every read. It models a
// push frame whose header is still in flight when the read deadline expires.
type prefixThenErrReader struct {
	prefix []byte
	err    error
}

func (r *prefixThenErrReader) Read(p []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	return 0, r.err
}

// TestVoidProcessorKeepsPushOnPeekIOError is the other half of the consume
// discipline: an I/O error from PeekPushNotificationName peeked nothing, so the
// frame has not fully arrived and must be left alone. Consuming it would read a
// partial frame and leave the connection mid-stream, which is worse than the
// reply shift the consume path avoids.
func TestVoidProcessorKeepsPushOnPeekIOError(t *testing.T) {
	// A valid but incomplete push header: the 7-byte name is only 3 bytes in,
	// so PeekPushNotificationName blocks for more and gets the read timeout.
	rd := proto.NewReader(&prefixThenErrReader{
		prefix: []byte(">1\r\n$7\r\nMOV"),
		err:    readTimeoutError{},
	})

	vp := NewVoidProcessor()
	if err := vp.ProcessPendingNotifications(context.Background(), NotificationHandlerContext{}, rd); err != nil {
		t.Fatalf("ProcessPendingNotifications: %v", err)
	}

	// The frame must still be at the buffer head, untouched.
	replyType, err := rd.PeekReplyType()
	if err != nil {
		t.Fatalf("PeekReplyType: %v", err)
	}
	if replyType != proto.RespPush {
		t.Fatalf("push frame was partially consumed: next reply type %q, want %q", replyType, proto.RespPush)
	}
}

// TestVoidProcessorKeepsPushOnUnparseableHeader covers a push whose header does
// not parse. ReadReply cannot find the end of such a frame, so consuming it
// would stop partway and hand the rest of the push to the caller as the reply.
// It must be left at the buffer head for the caller's own read to fail on.
func TestVoidProcessorKeepsPushOnUnparseableHeader(t *testing.T) {
	const rest = "$3\r\nfoo\r\n" + "+OK\r\n"

	for name, header := range map[string]string{
		"array length":      ">x\r\n",
		"name length":       ">2\r\n$x\r\n",
		"unknown name type": ">2\r\nxyz\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			stream := header + rest
			rd := proto.NewReader(strings.NewReader(stream))

			vp := NewVoidProcessor()
			if err := vp.ProcessPendingNotifications(context.Background(), NotificationHandlerContext{}, rd); err != nil {
				t.Fatalf("ProcessPendingNotifications: %v", err)
			}

			if got := rd.Buffered(); got != len(stream) {
				t.Fatalf("push frame was partially consumed: %d of %d bytes left", got, len(stream))
			}
			if reply, err := rd.ReadReply(); err == nil {
				t.Fatalf("reply shift: got %#v, want a parse error", reply)
			}
		})
	}
}
