package multidb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// delayedHTTPClient answers every REST request after a fixed delay. The bdbs
// list matches the requested host; the availability call returns failStatus
// for hosts in fail, 200 otherwise.
type delayedHTTPClient struct {
	delay time.Duration
	fail  map[string]bool
}

func (c *delayedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	select {
	case <-time.After(c.delay):
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	host := req.URL.Hostname()
	if strings.Contains(req.URL.Path, "/v1/bdbs") {
		body := fmt.Sprintf(`[{"uid": 1, "endpoints": [{"dns_name": %q, "addr": [], "port": 6379}]}]`, host)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
	status := 200
	if c.fail[host] {
		status = 503
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
}

// TestLagAwareClusterChecksMastersConcurrently pins that one cluster probe
// checks its routed masters concurrently: with N masters and two REST calls
// of delay D each, the probe takes about 2D, not 2N*D, so a healthy cluster
// fits the probe budget. Every master must still pass.
func TestLagAwareClusterChecksMastersConcurrently(t *testing.T) {
	const (
		masters = 4
		delay   = 100 * time.Millisecond
	)
	newCluster := func() *redis.ClusterClient {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{"m1.example.com:6379"},
			ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
				slots := make([]redis.ClusterSlot, masters)
				per := 16384 / masters
				for i := range slots {
					slots[i] = redis.ClusterSlot{
						Start: i * per,
						End:   (i+1)*per - 1,
						Nodes: []redis.ClusterNode{{Addr: fmt.Sprintf("m%d.example.com:6379", i+1)}},
					}
				}
				return slots, nil
			},
		})
	}

	noDNS := func(context.Context, string) ([]string, error) { return nil, fmt.Errorf("no DNS in test") }

	t.Run("healthy masters finish in about one master's latency", func(t *testing.T) {
		hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(&delayedHTTPClient{delay: delay}))
		hc.lookupHost = noDNS
		cc := newCluster()
		defer cc.Close()
		start := time.Now()
		ok, err := hc.CheckClusterHealth(context.Background(), cc)
		elapsed := time.Since(start)
		if !ok || err != nil {
			t.Fatalf("CheckClusterHealth = (%v, %v), want healthy", ok, err)
		}
		// One master costs 2*delay; serial checks would cost masters*2*delay.
		// The limit sits one delay below serial, leaving headroom for slow CI.
		if limit := time.Duration(masters*2-1) * delay; elapsed >= limit {
			t.Fatalf("CheckClusterHealth took %v for %d masters, want < %v (masters checked serially)", elapsed, masters, limit)
		}
	})

	t.Run("one failing master still fails the member", func(t *testing.T) {
		client := &delayedHTTPClient{delay: delay, fail: map[string]bool{"m3.example.com": true}}
		hc := NewLagAwareHealthCheck(WithLagAwareHTTPClient(client))
		hc.lookupHost = noDNS
		cc := newCluster()
		defer cc.Close()
		ok, err := hc.CheckClusterHealth(context.Background(), cc)
		if ok || err == nil {
			t.Fatalf("CheckClusterHealth = (%v, %v), want unhealthy: one routed master did not pass", ok, err)
		}
		if !strings.Contains(err.Error(), "status 503") {
			t.Fatalf("CheckClusterHealth error = %v, want the failing master's error", err)
		}
	})
}
