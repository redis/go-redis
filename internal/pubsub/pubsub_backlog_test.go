package pubsub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConfirmationsSurviveFullBuffer pins that subscribe confirmations
// are never dropped: a handle that subscribes more names than its
// events buffer holds, and reads only afterwards, gets one confirmation
// per name, in order, as on a dedicated connection.
func TestConfirmationsSurviveFullBuffer(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node:6379")
	cfg.ChanSize = 4
	m := newTestManager(t, srv, cfg)

	names := make([]string, 20)
	for i := range names {
		names[i] = fmt.Sprintf("c%d", i)
	}
	h, err := m.Subscribe(ctx, names...)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, append([]string{"subscribe"}, names...)...)

	// The fan-out is FIFO: the fence message proves every confirmation
	// was fanned out before anything is read.
	fence, err := m.Subscribe(ctx, "fence")
	if err != nil {
		t.Fatalf("Subscribe fence: %v", err)
	}
	fenceCh := fence.Channel()
	fsc.expectCmd(t, "subscribe", "fence")
	fsc.sendMessage(t, "message", "fence", "done")
	recvMsg(t, fenceCh)

	for i, name := range names {
		sub, ok := recvEvent(t, h.Events()).(*Subscription)
		if !ok || sub.Channel != name || sub.Count != i+1 {
			t.Fatalf("event %d = %#v, want the confirmation of %s", i, sub, name)
		}
	}
}

// TestBacklogCloseRace closes handles while their forwarders are busy:
// events must be closed exactly once, with no send on a closed channel.
func TestBacklogCloseRace(t *testing.T) {
	ctx := context.Background()
	for range 20 {
		srv := newFakeServer()
		srv.autoConfirm = true
		cfg := testConfig("node:6379")
		cfg.ChanSize = 1
		m := newTestManager(t, srv, cfg)

		var hs []PubSuber
		for range 4 {
			h, err := m.Subscribe(ctx, "flood")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			hs = append(hs, h)
		}
		fsc := srv.waitDial(t)
		frame := messageFrame("flood", strings.Repeat("p", 64))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 300 {
				fsc.writeRaw(frame)
			}
		}()
		// Slow readers keep the forwarders parked on full buffers.
		for _, h := range hs[:2] {
			go func(ch <-chan any) {
				for range ch {
					time.Sleep(10 * time.Microsecond)
				}
			}(h.Events())
		}
		time.Sleep(5 * time.Millisecond)
		_ = hs[0].Close()
		_ = hs[2].Close()
		_ = m.Close()
		wg.Wait()
		for _, h := range hs {
			deadline := time.After(5 * time.Second)
		drain:
			for {
				select {
				case _, ok := <-h.Events():
					if !ok {
						break drain
					}
				case <-deadline:
					t.Fatal("events not closed after Close")
				}
			}
		}
	}
}
