package redis

import (
	"fmt"
	"sync"
)

type PubSubClient struct {
	*Client
	conn *Conn
	ch   chan *Message
	once sync.Once
}

func newPubSubClient(client *Client) (*PubSubClient, error) {
	conn, _, err := client.ConnPool.Get()
	if err != nil {
		return nil, err
	}

	c := &PubSubClient{
		Client: client,
		conn:   conn,
		ch:     make(chan *Message),
	}
	return c, nil
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

		err := c.ReadReply(c.conn)
		if err != nil {
			msg := &Message{}
			msg.Err = err
			c.ch <- msg
			return
		}

		for {
			msg := &Message{}

			replyI, err := req.ParseReply(c.conn.Rd)
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

			if !c.conn.Rd.HasUnread() {
				break
			}
		}
	}
}

func (c *PubSubClient) Subscribe(channels ...string) (chan *Message, error) {
	args := append([]string{"SUBSCRIBE"}, channels...)
	req := NewMultiBulkReq(args...)

	if err := c.WriteReq(req.Req(), c.conn); err != nil {
		return nil, err
	}

	c.once.Do(func() {
		go c.consumeMessages()
	})

	return c.ch, nil
}

func (c *PubSubClient) Unsubscribe(channels ...string) error {
	args := append([]string{"UNSUBSCRIBE"}, channels...)
	req := NewMultiBulkReq(args...)
	return c.WriteReq(req.Req(), c.conn)
}
