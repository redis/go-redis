package redis

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9/internal/pool"
)

// PubSubAck is the confirmation of one SubscribeWithAck call. Its result is
// shared by all waiters. Canceling a wait does not unsubscribe the channels.
type PubSubAck struct {
	done      chan struct{}
	err       error
	remaining int
	settled   bool
}

// Done is closed when the operation has a result. Call Wait to read that result.
func (a *PubSubAck) Done() <-chan struct{} { return a.done }

// Wait waits for all requested channels to be acknowledged on the connection
// that sent the command, or for that connection to retire. A successful result
// is a readiness observation, not a guarantee against subsequent disconnection.
// A PubSub receiver (Receive, Channel, or ChannelWithSubscriptions) must run.
func (a *PubSubAck) Wait(ctx context.Context) error {
	select {
	case <-a.done:
		return a.err
	default:
	}
	select {
	case <-a.done:
		return a.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubscribeWithAck writes SUBSCRIBE and returns its confirmation. The PubSub
// owns the pending result until confirmation or connection retirement. Ordinary
// automatic resubscriptions cannot complete this result.
func (c *PubSub) SubscribeWithAck(ctx context.Context, channels ...string) (*PubSubAck, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ackInvalid != nil {
		_ = c.closeTheCn(errors.New("redis: pubsub acknowledgment stream is ambiguous"))
	}
	ack := &PubSubAck{done: make(chan struct{}), remaining: len(channels)}
	if len(channels) == 0 {
		ack.finish(nil)
		return ack, nil
	}
	cn, err := c.conn(ctx, channels)
	if err != nil {
		return nil, err
	}
	err = c.subscribeWithAck(ctx, cn, "subscribe", channels, ack)
	if err != nil {
		_ = c.closeTheCn(err)
	} else {
		c.releaseConn(ctx, cn, nil, false)
	}
	if c.channels == nil {
		c.channels = make(map[string]struct{})
	}
	for _, name := range channels {
		c.channels[name] = struct{}{}
	}
	if err != nil {
		return nil, err
	}
	return ack, nil
}

// finish is serialized by the owning PubSub mutex.
func (a *PubSubAck) finish(err error) {
	if a.settled {
		return
	}
	a.settled = true
	a.err = err
	close(a.done)
}

type pubSubAckEntry struct {
	ack   *PubSubAck
	count uint64 // Compress consecutive untracked acknowledgments.
}

func (c *PubSub) registerAck(cn *pool.Conn, channels []string, ack *PubSubAck) {
	if c.ackInvalid == cn {
		return
	}
	if c.acks == nil {
		c.acks = make(map[*pool.Conn]map[string][]pubSubAckEntry)
	}
	byChannel := c.acks[cn]
	if byChannel == nil {
		byChannel = make(map[string][]pubSubAckEntry)
		c.acks[cn] = byChannel
	}
	for _, name := range channels {
		queue := byChannel[name]
		if ack == nil && len(queue) > 0 && queue[len(queue)-1].ack == nil {
			queue[len(queue)-1].count++
		} else {
			queue = append(queue, pubSubAckEntry{ack: ack, count: 1})
		}
		byChannel[name] = queue
	}
}

func (c *PubSub) hasPendingAck(cn *pool.Conn) bool {
	for _, queue := range c.acks[cn] {
		for _, entry := range queue {
			if entry.ack != nil {
				return true
			}
		}
	}
	return false
}

func (c *PubSub) failAcks(cn *pool.Conn, err error) {
	for _, queue := range c.acks[cn] {
		for _, entry := range queue {
			if entry.ack != nil {
				entry.ack.finish(err)
			}
		}
	}
	delete(c.acks, cn)
}

func (c *PubSub) receiveAck(cn *pool.Conn, msg *Subscription) {
	if msg.Kind != "subscribe" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	byChannel := c.acks[cn]
	queue := byChannel[msg.Channel]
	if len(queue) == 0 {
		return
	}
	ack := queue[0].ack
	queue[0].count--
	if queue[0].count > 0 {
		return
	}
	queue[0] = pubSubAckEntry{}
	if len(queue) == 1 {
		delete(byChannel, msg.Channel)
	} else {
		byChannel[msg.Channel] = queue[1:]
	}
	if len(byChannel) == 0 {
		delete(c.acks, cn)
	}
	if ack != nil {
		msg.Ack = ack
		ack.remaining--
		if ack.remaining == 0 {
			ack.finish(nil)
		}
	}
}
