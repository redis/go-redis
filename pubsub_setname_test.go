package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestClientSetNameRESP2SubscribedErrorIs pins that on RESP2, ClientSetName
// on a PubSub whose shared connection another PubSub subscribed fails up
// front with the exported ErrClientSetNameSubscribed.
func TestClientSetNameRESP2SubscribedErrorIs(t *testing.T) {
	var reject atomic.Bool
	client := NewClient(&Options{
		Addr:            listenSubscribeServer(t, &reject),
		Protocol:        2,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx := context.Background()
	ps1 := client.Subscribe(ctx, "ch")
	defer ps1.Close()
	if _, err := ps1.ReceiveTimeout(ctx, 5*time.Second); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	ps2 := client.Subscribe(ctx)
	defer ps2.Close()
	if err := ps2.ClientSetName(ctx, "n"); !errors.Is(err, ErrClientSetNameSubscribed) {
		t.Fatalf("ClientSetName = %v, want ErrClientSetNameSubscribed", err)
	}
}
