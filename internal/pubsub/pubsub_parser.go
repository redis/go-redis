package pubsub

import "fmt"

// Subscription received after a successful subscription to channel.
type Subscription struct {
	// Can be "subscribe", "unsubscribe", "psubscribe" or "punsubscribe".
	Kind string
	// Channel name we have subscribed to.
	Channel string
	// Number of channels we are currently subscribed to.
	Count int
}

func (m *Subscription) String() string {
	return fmt.Sprintf("%s: %s", m.Kind, m.Channel)
}

// Message received as result of a PUBLISH command issued by another client.
type Message struct {
	Channel      string
	Pattern      string
	Payload      string
	PayloadSlice []string
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<%s: %s>", m.Channel, m.Payload)
}

// Pong received as result of a PING command issued by another client.
type Pong struct {
	Payload string
}

func (p *Pong) String() string {
	if p.Payload != "" {
		return fmt.Sprintf("Pong<%s>", p.Payload)
	}
	return "Pong"
}

// pubSubString returns reply[i] as a string, reporting false when the
// element is missing or not a string.
func pubSubString(reply []any, i int) (string, bool) {
	if i >= len(reply) {
		return "", false
	}
	s, ok := reply[i].(string)
	return s, ok
}

// parsePubSubMessage converts a raw pub/sub reply (e.g. ["message",
// channel, payload]) into a *Subscription, *Message, *shardMessage,
// *patternMessage or *Pong. The wrapper types carry the frame kind:
// routing must not infer it from Message fields (an empty Pattern is a
// valid pattern, not a discriminator). Malformed frames are reported as errUnsupportedMessage /
// errUnsupportedPayload, never panics: the caller is the manager's
// listen goroutine, where a panic would crash the process.
func parsePubSubMessage(reply any) (any, error) {
	switch reply := reply.(type) {
	case string:
		return &Pong{
			Payload: reply,
		}, nil
	case []any:
		kind, ok := pubSubString(reply, 0)
		if !ok {
			return nil, fmt.Errorf("%w: malformed frame %#v", errUnsupportedMessage, reply)
		}
		switch kind {
		case "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ssubscribe", "sunsubscribe":
			if len(reply) < 3 {
				return nil, fmt.Errorf("%w: %q with %d element(s)", errUnsupportedMessage, kind, len(reply))
			}
			count, ok := reply[2].(int64)
			if !ok {
				return nil, fmt.Errorf("%w: %q with non-integer count %T", errUnsupportedMessage, kind, reply[2])
			}
			// The channel can be nil in case of "unsubscribe".
			channel, _ := pubSubString(reply, 1)
			return &Subscription{
				Kind:    kind,
				Channel: channel,
				Count:   int(count),
			}, nil
		case "message", "smessage":
			channel, ok := pubSubString(reply, 1)
			if !ok || len(reply) < 3 {
				return nil, fmt.Errorf("%w: %q with %d element(s)", errUnsupportedMessage, kind, len(reply))
			}
			var msg *Message
			switch payload := reply[2].(type) {
			case string:
				msg = &Message{
					Channel: channel,
					Payload: payload,
				}
			case []any:
				ss := make([]string, len(payload))
				for i := range payload {
					s, ok := pubSubString(payload, i)
					if !ok {
						return nil, fmt.Errorf("%w: non-string element %T", errUnsupportedPayload, payload[i])
					}
					ss[i] = s
				}
				msg = &Message{
					Channel:      channel,
					PayloadSlice: ss,
				}
			default:
				return nil, fmt.Errorf("%w: %T", errUnsupportedPayload, payload)
			}
			if kind == "smessage" {
				return &shardMessage{msg}, nil
			}
			return msg, nil
		case "pmessage":
			pattern, ok1 := pubSubString(reply, 1)
			channel, ok2 := pubSubString(reply, 2)
			payload, ok3 := pubSubString(reply, 3)
			if !ok1 || !ok2 || !ok3 {
				return nil, fmt.Errorf("%w: malformed %q frame", errUnsupportedMessage, kind)
			}
			return &patternMessage{&Message{
				Pattern: pattern,
				Channel: channel,
				Payload: payload,
			}}, nil
		case "pong":
			payload, ok := pubSubString(reply, 1)
			if !ok {
				return nil, fmt.Errorf("%w: malformed %q frame", errUnsupportedMessage, kind)
			}
			return &Pong{
				Payload: payload,
			}, nil
		default:
			return nil, fmt.Errorf("%w: %q", errUnsupportedMessage, kind)
		}
	default:
		return nil, fmt.Errorf("%w: %#v", errUnsupportedMessage, reply)
	}
}
