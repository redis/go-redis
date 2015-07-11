package redis

import (
	"fmt"
	"time"
)

// Posts a message to the given channel.
func (c *Client) Publish(channel, message string) *IntCmd {
	req := NewIntCmd("PUBLISH", channel, message)
	c.Process(req)
	return req
}

// PubSub implements Pub/Sub commands as described in
// http://redis.io/topics/pubsub.
type PubSub struct {
	*baseClient
}

// Deprecated. Use Subscribe/PSubscribe instead.
func (c *Client) PubSub() *PubSub {
	return &PubSub{
		baseClient: &baseClient{
			opt:      c.opt,
			connPool: newSingleConnPool(c.connPool, false),
		},
	}
}

// Subscribes the client to the specified channels.
func (c *Client) Subscribe(channels ...string) (*PubSub, error) {
	pubsub := c.PubSub()
	return pubsub, pubsub.Subscribe(channels...)
}

// Subscribes the client to the given patterns.
func (c *Client) PSubscribe(channels ...string) (*PubSub, error) {
	pubsub := c.PubSub()
	return pubsub, pubsub.PSubscribe(channels...)
}

func (c *PubSub) Ping(payload string) error {
	cn, err := c.conn()
	if err != nil {
		return err
	}

	args := []interface{}{"PING"}
	if payload != "" {
		args = append(args, payload)
	}
	cmd := NewCmd(args...)
	return cn.writeCmds(cmd)
}

// Message received after a successful subscription to channel.
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
	Channel string
	Payload string
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<%s: %s>", m.Channel, m.Payload)
}

// Message matching a pattern-matching subscription received as result
// of a PUBLISH command issued by another client.
type PMessage struct {
	Channel string
	Pattern string
	Payload string
}

func (m *PMessage) String() string {
	return fmt.Sprintf("PMessage<%s: %s>", m.Channel, m.Payload)
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

// Returns a message as a Subscription, Message, PMessage, Pong or
// error. See PubSub example for details.
func (c *PubSub) Receive() (interface{}, error) {
	return c.ReceiveTimeout(0)
}

func newMessage(reply []interface{}) (interface{}, error) {
	switch kind := reply[0].(string); kind {
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		return &Subscription{
			Kind:    kind,
			Channel: reply[1].(string),
			Count:   int(reply[2].(int64)),
		}, nil
	case "message":
		return &Message{
			Channel: reply[1].(string),
			Payload: reply[2].(string),
		}, nil
	case "pmessage":
		return &PMessage{
			Pattern: reply[1].(string),
			Channel: reply[2].(string),
			Payload: reply[3].(string),
		}, nil
	case "pong":
		return &Pong{
			Payload: reply[1].(string),
		}, nil
	default:
		return nil, fmt.Errorf("redis: unsupported pubsub notification: %q", kind)
	}
}

// ReceiveTimeout acts like Receive but returns an error if message
// is not received in time.
func (c *PubSub) ReceiveTimeout(timeout time.Duration) (interface{}, error) {
	cn, err := c.conn()
	if err != nil {
		return nil, err
	}
	cn.ReadTimeout = timeout

	cmd := NewSliceCmd()
	if err := cmd.parseReply(cn.rd); err != nil {
		return nil, err
	}
	return newMessage(cmd.Val())
}

func (c *PubSub) subscribe(cmd string, channels ...string) error {
	cn, err := c.conn()
	if err != nil {
		return err
	}

	args := make([]interface{}, 1+len(channels))
	args[0] = cmd
	for i, channel := range channels {
		args[1+i] = channel
	}
	req := NewSliceCmd(args...)
	return cn.writeCmds(req)
}

// Subscribes the client to the specified channels.
func (c *PubSub) Subscribe(channels ...string) error {
	return c.subscribe("SUBSCRIBE", channels...)
}

// Subscribes the client to the given patterns.
func (c *PubSub) PSubscribe(patterns ...string) error {
	return c.subscribe("PSUBSCRIBE", patterns...)
}

// Unsubscribes the client from the given channels, or from all of
// them if none is given.
func (c *PubSub) Unsubscribe(channels ...string) error {
	return c.subscribe("UNSUBSCRIBE", channels...)
}

// Unsubscribes the client from the given patterns, or from all of
// them if none is given.
func (c *PubSub) PUnsubscribe(patterns ...string) error {
	return c.subscribe("PUNSUBSCRIBE", patterns...)
}
