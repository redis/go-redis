package pubsub

import (
	"errors"
	"reflect"
	"testing"
)

func TestParsePubSubMessage(t *testing.T) {
	tests := []struct {
		name  string
		reply any
		want  any
		// wantErr, when non-nil, is the sentinel the returned error must
		// match via errors.Is.
		wantErr error
	}{
		{
			name:  "subscribe confirmation",
			reply: []any{"subscribe", "ch1", int64(1)},
			want:  &Subscription{Kind: "subscribe", Channel: "ch1", Count: 1},
		},
		{
			name:  "unsubscribe with nil channel",
			reply: []any{"unsubscribe", nil, int64(0)},
			want:  &Subscription{Kind: "unsubscribe", Channel: "", Count: 0},
		},
		{
			name:  "message",
			reply: []any{"message", "ch1", "payload"},
			want:  &Message{Channel: "ch1", Payload: "payload"},
		},
		{
			name:  "message with payload slice",
			reply: []any{"message", "ch1", []any{"a", "b"}},
			want:  &Message{Channel: "ch1", PayloadSlice: []string{"a", "b"}},
		},
		{
			name:  "smessage wraps the message for shard routing",
			reply: []any{"smessage", "ch1", "payload"},
			want:  &shardMessage{&Message{Channel: "ch1", Payload: "payload"}},
		},
		{
			name:  "pmessage wraps the message for pattern routing",
			reply: []any{"pmessage", "p.*", "p.one", "payload"},
			want:  &patternMessage{&Message{Pattern: "p.*", Channel: "p.one", Payload: "payload"}},
		},
		{
			name:  "pong as bare string",
			reply: "PONG",
			want:  &Pong{Payload: "PONG"},
		},
		{
			name:  "pong with payload",
			reply: []any{"pong", "hello"},
			want:  &Pong{Payload: "hello"},
		},
		{
			name:    "unknown kind",
			reply:   []any{"MOVING", "1", "host:6379"},
			wantErr: errUnsupportedMessage,
		},
		{
			name:    "non-array non-string frame",
			reply:   int64(42),
			wantErr: errUnsupportedMessage,
		},
		{
			name:    "non-string kind",
			reply:   []any{int64(1), "ch1", "payload"},
			wantErr: errUnsupportedMessage,
		},
		{
			name:    "short subscribe frame",
			reply:   []any{"subscribe", "ch1"},
			wantErr: errUnsupportedMessage,
		},
		{
			name:    "non-integer confirmation count",
			reply:   []any{"subscribe", "ch1", "not-a-count"},
			wantErr: errUnsupportedMessage,
		},
		{
			name:    "short message frame",
			reply:   []any{"message", "ch1"},
			wantErr: errUnsupportedMessage,
		},
		{
			name:    "unsupported payload type",
			reply:   []any{"message", "ch1", int64(7)},
			wantErr: errUnsupportedPayload,
		},
		{
			name:    "non-string payload slice element",
			reply:   []any{"message", "ch1", []any{"ok", int64(7)}},
			wantErr: errUnsupportedPayload,
		},
		{
			name:    "malformed pmessage",
			reply:   []any{"pmessage", "p.*", "p.one"},
			wantErr: errUnsupportedMessage,
		},
		{
			name:    "malformed pong",
			reply:   []any{"pong"},
			wantErr: errUnsupportedMessage,
		},
	}

	t.Run("Stringers", func(t *testing.T) {
		if s := (&Subscription{Kind: "subscribe", Channel: "ch"}).String(); s != "subscribe: ch" {
			t.Errorf("Subscription.String() = %q, want %q", s, "subscribe: ch")
		}
		if s := (&Message{Channel: "ch", Payload: "pay"}).String(); s != "Message<ch: pay>" {
			t.Errorf("Message.String() = %q, want %q", s, "Message<ch: pay>")
		}
		if s := (&Pong{}).String(); s != "Pong" {
			t.Errorf("Pong.String() = %q, want %q", s, "Pong")
		}
		if s := (&Pong{Payload: "pay"}).String(); s != "Pong<pay>" {
			t.Errorf("Pong.String() = %q, want %q", s, "Pong<pay>")
		}
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePubSubMessage(tt.reply)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("parsePubSubMessage error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePubSubMessage: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parsePubSubMessage = %#v, want %#v", got, tt.want)
			}
		})
	}
}
