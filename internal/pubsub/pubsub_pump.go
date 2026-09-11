package pubsub

import (
	"context"
	"time"
)

// pumpMode records which consumer view was started over Events; the two
// are mutually exclusive.
type pumpMode uint8

const (
	pumpNone pumpMode = iota
	pumpMessages
	pumpAll
)

// pump moves the events filter selects into out, closing out when the
// stream ends. With sendTimeout <= 0 sends block (backpressure lands in
// the events buffer); a positive sendTimeout drops events the consumer
// doesn't take in time, with throttled logging. done unblocks a parked
// send when the handle closes.
func pump[T any](
	events <-chan any, done <-chan struct{}, out chan T,
	sendTimeout, logInterval time.Duration, filter func(any) (T, bool),
) {
	var timer *time.Timer
	if sendTimeout > 0 {
		timer = time.NewTimer(sendTimeout)
		timer.Stop()
	}
	var dropped int
	var dropLogTime time.Time
loop:
	for ev := range events {
		v, ok := filter(ev)
		if !ok {
			continue
		}
		if timer == nil {
			select {
			case out <- v:
			case <-done:
				break loop
			}
			continue
		}
		timer.Reset(sendTimeout)
		select {
		case out <- v:
			timer.Stop()
		case <-done:
			break loop
		case <-timer.C:
			dropped++
			if logThrottled(context.TODO(), &dropLogTime, logInterval,
				"redis: pubsub: dropped %d message(s) to a slow consumer (send timed out after %s, see WithChannelSendTimeout)",
				dropped, sendTimeout) {
				dropped = 0
			}
		}
	}
	close(out)
}

// messageFilter selects the Channel view: messages only.
func messageFilter(ev any) (*Message, bool) {
	msg, ok := ev.(*Message)
	return msg, ok
}

// allEventsFilter selects the ChannelWithSubscriptions view: messages
// and subscription confirmations; pongs and error replies are filtered
// out (the channel views auto-recover; errors surface through Receive).
func allEventsFilter(ev any) (any, bool) {
	switch ev.(type) {
	case *Pong, error:
		return nil, false
	}
	return ev, true
}
