package redis

import (
	"testing"
	"time"
)

// TestBackgroundDrainerLifecycle verifies start/stop bookkeeping: starting
// registers a stop channel, double-start is a no-op, and stop removes the
// registration and closes the channel (so the goroutine exits).
func TestBackgroundDrainerLifecycle(t *testing.T) {
	c := &baseClient{opt: &Options{Protocol: 3}}

	c.startBackgroundDrainer()
	v, ok := cscDrainStopField.Load(c)
	if !ok {
		t.Fatal("startBackgroundDrainer did not register a stop channel")
	}
	stop := v.(chan struct{})

	// Double-start must not replace the channel.
	c.startBackgroundDrainer()
	if v2, _ := cscDrainStopField.Load(c); v2.(chan struct{}) != stop {
		t.Fatal("double start replaced the stop channel")
	}

	c.stopBackgroundDrainer()
	if _, still := cscDrainStopField.Load(c); still {
		t.Fatal("stopBackgroundDrainer did not remove the registration")
	}
	select {
	case <-stop:
		// closed — drainer goroutine will exit
	case <-time.After(time.Second):
		t.Fatal("stop channel was not closed")
	}

	// Stop again — idempotent, no panic.
	c.stopBackgroundDrainer()
}
