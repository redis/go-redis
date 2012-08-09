package redis

import (
	"fmt"
	"sync"
)

type PubSubClient struct {
	*Client
	ch   chan *Message
	once sync.Once
}

func newPubSubClient(client *Client) (*PubSubClient, error) {
	pubSubConn, _, err := client.ConnPool.Get()
	if err != nil {
		return nil, err
	}
	client.ConnPool.Remove(pubSubConn)

	c := &PubSubClient{
		Client: &Client{
			ConnPool: NewSingleConnPool(pubSubConn),
		},
		ch: make(chan *Message),
	}
	return c, nil
}

type Message struct {
	Name, Channel, ChannelPattern, Message string
	Number                                 int64

	Err error
}

func (c *PubSubClient) consumeMessages() {
	conn, err := c.conn()
	// SignleConnPool never returns error.
	if err != nil {
		panic(err)
	}
	req := NewMultiBulkReq()

	for {
		for {
			msg := &Message{}

			replyI, err := req.ParseReply(conn.Rd)
			if err != nil {
				msg.Err = err
				c.ch <- msg
				break
			}
			reply := replyI.([]interface{})

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
				msg.Err = fmt.Errorf("Unsupported message name: %q.", msgName)
			}
			c.ch <- msg

			if conn.Rd.Buffered() <= 0 {
				break
			}
		}
	}
}

func (c *PubSubClient) subscribe(cmd string, channels ...string) (chan *Message, error) {
	args := append([]string{cmd}, channels...)
	req := NewMultiBulkReq(args...)

	conn, err := c.conn()
	if err != nil {
		return nil, err
	}

	if err := c.WriteReq(req.Req(), conn); err != nil {
		return nil, err
	}

	c.once.Do(func() {
		go c.consumeMessages()
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
	req := NewMultiBulkReq(args...)

	conn, err := c.conn()
	if err != nil {
		return err
	}

	return c.WriteReq(req.Req(), conn)
}

func (c *PubSubClient) Unsubscribe(channels ...string) error {
	return c.unsubscribe("UNSUBSCRIBE", channels...)
}

func (c *PubSubClient) PUnsubscribe(patterns ...string) error {
	return c.unsubscribe("PUNSUBSCRIBE", patterns...)
}
