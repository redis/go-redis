package redis_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// queryLabelsTestAddr is the standalone Redis used by the TS.QUERYLABELS
// integration test. Defaults to :6379 (the repo's standard test server) and
// can be overridden when a Redis 8.10 instance runs on a different port:
//
//	REDIS_QUERYLABELS_TEST_ADDR=localhost:6399 go test -run TestTSQueryLabels_Integration
func queryLabelsTestAddr() string {
	if a := os.Getenv("REDIS_QUERYLABELS_TEST_ADDR"); a != "" {
		return a
	}
	return ":6379"
}

// TestTSQueryLabels_Integration exercises both TS.QUERYLABELS forms
// end-to-end against a live server under RESP2 (array reply) and RESP3
// (set reply).
//
// TS.QUERYLABELS ships in RedisTimeSeries 8.10. The test gates on the
// command itself rather than on the REDIS_VERSION env variable, whose float
// form cannot tell 8.10 from 8.1; older servers reply with an "unknown
// command" error, which this test treats as a skip.
func TestTSQueryLabels_Integration(t *testing.T) {
	probeCtx := context.Background()
	probe := redis.NewClient(&redis.Options{Addr: queryLabelsTestAddr()})
	t.Cleanup(func() { _ = probe.Close() })
	if err := probe.Ping(probeCtx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", queryLabelsTestAddr(), err)
	}
	if err := probe.TSQueryLabels(probeCtx, nil).Err(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unknown command") {
			t.Skipf("TS.QUERYLABELS not supported by server (requires Redis 8.10+): %v", err)
		}
		t.Skipf("timeseries module not available: %v", err)
	}

	for _, proto := range []int{2, 3} {
		proto := proto
		t.Run(fmt.Sprintf("RESP%d", proto), func(t *testing.T) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{Addr: queryLabelsTestAddr(), Protocol: proto})
			t.Cleanup(func() { _ = client.Close() })

			// The command aggregates over every indexed series in the shared
			// test database, so scope all assertions with a filter on a tag
			// value unique to this test run.
			group := fmt.Sprintf("querylabels-test-resp%d", proto)
			groupFilter := []string{"test_group=" + group}
			keys := []string{
				fmt.Sprintf("querylabels-test:%d:1", proto),
				fmt.Sprintf("querylabels-test:%d:2", proto),
				fmt.Sprintf("querylabels-test:%d:3", proto),
			}
			// Pre-delete in case a previous run crashed between creation and
			// cleanup; TS.CREATE fails on an existing key.
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("Del %v: %v", keys, err)
			}
			t.Cleanup(func() { _ = client.Del(ctx, keys...).Err() })

			mustCreate := func(key string, labels map[string]string) {
				t.Helper()
				if err := client.TSCreateWithArgs(ctx, key, &redis.TSOptions{Labels: labels}).Err(); err != nil {
					t.Fatalf("TS.CREATE %s: %v", key, err)
				}
			}
			mustCreate(keys[0], map[string]string{"test_group": group, "location": "kitchen", "unit": "celsius"})
			mustCreate(keys[1], map[string]string{"test_group": group, "location": "bedroom", "unit": "celsius"})
			mustCreate(keys[2], map[string]string{"test_group": group, "location": "kitchen"})

			assertUnorderedEqual := func(what string, got, want []string) {
				t.Helper()
				g := append([]string(nil), got...)
				w := append([]string(nil), want...)
				sort.Strings(g)
				sort.Strings(w)
				if len(g) != len(w) {
					t.Fatalf("%s mismatch\n got: %v\nwant: %v", what, got, want)
				}
				for i := range w {
					if g[i] != w[i] {
						t.Fatalf("%s mismatch\n got: %v\nwant: %v", what, got, want)
					}
				}
			}

			// LABELS: every distinct label name across the matching series,
			// including the label name used in the filter itself.
			labels, err := client.TSQueryLabels(ctx, groupFilter).Result()
			if err != nil {
				t.Fatalf("TS.QUERYLABELS LABELS: %v", err)
			}
			assertUnorderedEqual("label names", labels, []string{"test_group", "location", "unit"})

			// VALUES: deduplicated union of the label's values; series
			// without the label contribute nothing.
			values, err := client.TSQueryLabelValues(ctx, "location", groupFilter).Result()
			if err != nil {
				t.Fatalf("TS.QUERYLABELS VALUES location: %v", err)
			}
			assertUnorderedEqual("location values", values, []string{"kitchen", "bedroom"})

			values, err = client.TSQueryLabelValues(ctx, "unit", groupFilter).Result()
			if err != nil {
				t.Fatalf("TS.QUERYLABELS VALUES unit: %v", err)
			}
			assertUnorderedEqual("unit values", values, []string{"celsius"})

			// A label that exists on no matching series yields an empty
			// reply, not an error.
			values, err = client.TSQueryLabelValues(ctx, "no-such-label", groupFilter).Result()
			if err != nil {
				t.Fatalf("TS.QUERYLABELS VALUES no-such-label: %v", err)
			}
			if len(values) != 0 {
				t.Fatalf("expected an empty reply for an unknown label, got %v", values)
			}

			// Label names are matched byte-exactly: a case variant is a
			// different label.
			values, err = client.TSQueryLabelValues(ctx, "Location", groupFilter).Result()
			if err != nil {
				t.Fatalf("TS.QUERYLABELS VALUES Location: %v", err)
			}
			if len(values) != 0 {
				t.Fatalf("expected an empty reply for a case-variant label, got %v", values)
			}

			// Omitting the filter queries all indexed series: the reply must
			// at least contain this test's label names (the shared server may
			// hold others).
			labels, err = client.TSQueryLabels(ctx, nil).Result()
			if err != nil {
				t.Fatalf("TS.QUERYLABELS LABELS without FILTER: %v", err)
			}
			seen := make(map[string]bool, len(labels))
			for _, l := range labels {
				seen[l] = true
			}
			for _, want := range []string{"test_group", "location", "unit"} {
				if !seen[want] {
					t.Fatalf("unfiltered label names %v do not contain %q", labels, want)
				}
			}

			// Server-side filter errors are propagated as-is (TSDB: prefix):
			// a filter list without an inclusive matcher is rejected by the
			// server, not by the client.
			err = client.TSQueryLabels(ctx, []string{"test_group!=" + group}).Err()
			if err == nil {
				t.Fatalf("expected a server error for a filter without an inclusive matcher")
			}
			if !strings.Contains(err.Error(), "TSDB:") {
				t.Fatalf("expected the TSDB-prefixed server error verbatim, got: %v", err)
			}
		})
	}
}
