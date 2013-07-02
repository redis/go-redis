package redis

import (
	"fmt"
	"sync"
)

type PubSubClient struct {
	*baseClient

	ch   chan *Message
	once sync.Once
}

func (c *Client) PubSubClient() (*PubSubClient, error) {
	return &PubSubClient{
		baseClient: &baseClient{
			connPool: newSingleConnPool(c.connPool, nil, false),

			password: c.password,
			db:       c.db,
		},

		ch: make(chan *Message),
	}, nil
}

func (c *Client) Publish(channel, message string) *IntReq {
	req := NewIntReq("PUBLISH", channel, message)
	c.Process(req)
	return req
}

type Message struct {
	Name, Channel, ChannelPattern, Message string
	Number                                 int64

	Err error
}

func (c *PubSubClient) consumeMessages(cn *conn) {
	req := NewIfaceSliceReq()

	for {
		msg := &Message{}

		replyIface, err := req.ParseReply(cn.Rd)
		if err != nil {
			msg.Err = err
			c.ch <- msg
			return
		}
		reply, ok := replyIface.([]interface{})
		if !ok {
			msg.Err = fmt.Errorf("redis: unexpected reply type %T", replyIface)
			c.ch <- msg
			return
		}

		msgName := reply[0].(string)
		switch msgName {
		case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
			msg.Name = msgName
			msg.Channel = reply[1].(string)
			msg.Number = reply[2].(int64)
		case "message":
			msg.Name = msgName
			msg.Channel = reply[1].(string)
			msg.Message = reply[2].(string)
		case "pmessage":
			msg.Name = msgName
			msg.ChannelPattern = reply[1].(string)
			msg.Channel = reply[2].(string)
			msg.Message = reply[3].(string)
		default:
			msg.Err = fmt.Errorf("redis: unsupported message name: %q", msgName)
		}
		c.ch <- msg
	}
}

func (c *PubSubClient) subscribe(cmd string, channels ...string) (chan *Message, error) {
	args := append([]string{cmd}, channels...)
	req := NewIfaceSliceReq(args...)

	cn, err := c.conn()
	if err != nil {
		return nil, err
	}

	if err := c.writeReq(cn, req); err != nil {
		return nil, err
	}

	c.once.Do(func() {
		go c.consumeMessages(cn)
	})

	return c.ch, nil
}

func (c *PubSubClient) Subscribe(channels ...string) (chan *Message, error) {
	return c.subscribe("SUBSCRIBE", channels...)
}

func (c *PubSubClient) PSubscribe(patterns ...string) (chan *Message, error) {
	return c.subscribe("PSUBSCRIBE", patterns...)
}

func (c *PubSubClient) unsubscribe(cmd string, channels ...string) error {
	args := append([]string{cmd}, channels...)
	req := NewIfaceSliceReq(args...)

	cn, err := c.conn()
	if err != nil {
		return err
	}

	return c.writeReq(cn, req)
}

func (c *PubSubClient) Unsubscribe(channels ...string) error {
	return c.unsubscribe("UNSUBSCRIBE", channels...)
}

func (c *PubSubClient) PUnsubscribe(patterns ...string) error {
	return c.unsubscribe("PUNSUBSCRIBE", patterns...)
}
