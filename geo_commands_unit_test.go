package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A pipelined or autopipelined command is built when it is queued and its
// reply is read later. Changing the caller's query in between must not change
// how the reply is parsed.
func TestGeoCmdsParseReplyWithQueuedFlags(t *testing.T) {
	// Two entries, each [name, dist]: the reply to a WITHDIST query.
	const withDistReply = "*2\r\n" +
		"*2\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n" +
		"*2\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n"

	t.Run("GeoLocationCmd", func(t *testing.T) {
		q := &GeoRadiusQuery{Radius: 200, Unit: "km", WithDist: true}
		cmd := NewGeoLocationCmd(context.Background(), q, "georadius", "Sicily", 15, 37)
		q.WithDist, q.WithCoord = false, true

		if err := cmd.readReply(proto.NewReader(strings.NewReader(withDistReply))); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		if got := cmd.Val(); len(got) != 2 || got[0].Dist != 56.4413 || got[1].Dist != 190.4424 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("GeoSearchLocationCmd", func(t *testing.T) {
		q := &GeoSearchLocationQuery{
			GeoSearchQuery: GeoSearchQuery{Member: "Catania", Radius: 200},
			WithDist:       true,
		}
		cmd := NewGeoSearchLocationCmd(context.Background(), q, "geosearch", "Sicily")
		q.WithDist, q.WithCoord = false, true

		if err := cmd.readReply(proto.NewReader(strings.NewReader(withDistReply))); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		if got := cmd.Val(); len(got) != 2 || got[0].Dist != 56.4413 || got[1].Dist != 190.4424 {
			t.Fatalf("got %+v", got)
		}
	})
}
