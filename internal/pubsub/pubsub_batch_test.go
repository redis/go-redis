package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestBatchedMessageBeforeConsumedPush pins that a message read in the
// same buffer as a following push that processPush consumes (a
// maintenance notification) is delivered at once: the batch must not
// wait for a later frame on an otherwise idle connection.
func TestBatchedMessageBeforeConsumedPush(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	hooks := testHooks(srv)
	// Consumes MOVING pushes, like the client's push processor.
	hooks.processPush = func(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error {
		for {
			if t, err := rd.PeekReplyType(); err != nil || t != proto.RespPush {
				return nil
			}
			if name, err := rd.PeekPushNotificationName(); err != nil || name != "MOVING" {
				return nil
			}
			if _, err := rd.ReadReply(); err != nil {
				return err
			}
			if rd.Buffered() == 0 {
				return nil
			}
		}
	}
	m := NewManager(testConfig("node:6379"), hooks)
	t.Cleanup(func() { _ = m.Close() })

	h, err := m.Subscribe(ctx, "p")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "p")
	fsc.sendConfirm(t, "subscribe", "p", 1)
	recvEvent(t, h.Events())

	fsc.write(t, messageFrame("p", "m1")+">3\r\n$6\r\nMOVING\r\n:1\r\n:15\r\n")
	select {
	case ev := <-h.Events():
		if !isMessage(ev, "m1") {
			t.Fatalf("event = %#v, want m1", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("message followed by a consumed push was not delivered without a further frame")
	}
}

// TestBatchedDeliveryKeepsFrameOrder pins that message batching (see
// Manager.flushBatch) keeps per-handle frame order across non-message
// frames read from the same buffer: messages, an error reply and a
// pong, written in one go.
func TestBatchedDeliveryKeepsFrameOrder(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "o")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "o")
	fsc.sendConfirm(t, "subscribe", "o", 1)
	recvEvent(t, h.Events())
	if err := h.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc.expectCmd(t, "ping")

	// The pong answers the ping; the error reply, with the ledger then
	// empty, reaches every handle.
	fsc.write(t, messageFrame("o", "m1")+messageFrame("o", "m2")+
		"*2\r\n$4\r\npong\r\n$0\r\n\r\n"+messageFrame("o", "m3")+
		"-ERR boom\r\n"+messageFrame("o", "m4"))

	var got []string
	for range 6 {
		switch ev := recvEvent(t, h.Events()).(type) {
		case *Message:
			got = append(got, ev.Payload)
		case *Pong:
			got = append(got, "pong")
		case error:
			got = append(got, "err")
		default:
			t.Fatalf("unexpected event %#v", ev)
		}
	}
	if want := "m1 m2 pong m3 err m4"; strings.Join(got, " ") != want {
		t.Fatalf("events = %v, want %s", got, want)
	}
}

func isMessage(ev any, payload string) bool {
	msg, ok := ev.(*Message)
	return ok && msg.Payload == payload
}
