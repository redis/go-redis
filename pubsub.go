package redis

import (
	"fmt"
)

type PubSubClient struct {
	*Client
	isSubscribed bool
	ch           chan *Message
}

func NewPubSubClient(connect connectFunc, disconnect disconnectFunc) *PubSubClient {
	c := &PubSubClient{
		Client: NewClient(connect, disconnect),
		ch:     make(chan *Message),
	}
	return c
}

type Message struct {
	Name, Channel, Message string
	Number                 int64

	Err error
}

func (c *PubSubClient) consumeMessages() {
	req := NewMultiBulkReq()

	for {
		// Replies can arrive in batches.
		// Read whole reply and parse messages one by one.

		rd, err := c.readerPool.Get()
		if err != nil {
			msg := &Message{}
			msg.Err = err
			c.ch <- msg
			return
		}
		defer c.readerPool.Add(rd)

		err = c.ReadReply(rd)
		if err != nil {
			msg := &Message{}
			msg.Err = err
			c.ch <- msg
			return
		}

		for {
			msg := &Message{}

			replyI, err := req.ParseReply(rd)
			if err != nil {
				msg.Err = err
				c.ch <- msg
				break
			}
			reply := replyI.([]interface{})

			msgName := reply[0].(string)
			switch msgName {
			case "subscribe", "unsubscribe":
				msg.Name = msgName
				msg.Channel = reply[1].(string)
				msg.Number = reply[2].(int64)
			case "message":
				msg.Name = msgName
				msg.Channel = reply[1].(string)
				msg.Message = reply[2].(string)
			default:
				msg.Err = fmt.Errorf("Unsupported message name: %q.", msgName)
			}
			c.ch <- msg

			if !rd.HasUnread() {
				break
			}
		}
	}
}

func (c *PubSubClient) Subscribe(channels ...string) (chan *Message, error) {
	args := append([]string{"SUBSCRIBE"}, channels...)
	req := NewMultiBulkReq(args...)

	if err := c.WriteReq(req.Req()); err != nil {
		return nil, err
	}

	c.mtx.Lock()
	if !c.isSubscribed {
		c.isSubscribed = true
		go c.consumeMessages()
	}
	c.mtx.Unlock()

	return c.ch, nil
}

func (c *PubSubClient) Unsubscribe(channels ...string) error {
	args := append([]string{"UNSUBSCRIBE"}, channels...)
	req := NewMultiBulkReq(args...)
	return c.WriteReq(req.Req())
}
