package redis_test

import (
	"context"
	"fmt"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// TS.QUERYLABELS ships in RedisTimeSeries 8.10. The specs gate on
// SkipBeforeRedisVersion, which compares major.minor as integers, and
// exercise both command forms under RESP2 (array reply) and RESP3 (set
// reply) against the suite's standard server.
var _ = Describe("TS.QUERYLABELS", Label("timeseries", "tsquerylabels"), func() {
	ctx := context.TODO()

	for _, protocol := range []int{2, 3} {
		protocol := protocol

		Describe(fmt.Sprintf("RESP%d", protocol), func() {
			var client *redis.Client

			// The command aggregates over every indexed series in the
			// database, so scope assertions with a filter on a tag value
			// unique to this spec group.
			group := fmt.Sprintf("querylabels-test-resp%d", protocol)
			groupFilter := []string{"test_group=" + group}
			keys := []string{
				fmt.Sprintf("querylabels-test:%d:1", protocol),
				fmt.Sprintf("querylabels-test:%d:2", protocol),
				fmt.Sprintf("querylabels-test:%d:3", protocol),
			}

			BeforeEach(func() {
				SkipBeforeRedisVersion("8.10", "TS.QUERYLABELS requires Redis 8.10+")
				client = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: protocol})
				Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

				mustCreate := func(key string, labels map[string]string) {
					Expect(client.TSCreateWithArgs(ctx, key, &redis.TSOptions{Labels: labels}).Err()).NotTo(HaveOccurred())
				}
				mustCreate(keys[0], map[string]string{"test_group": group, "location": "kitchen", "unit": "celsius"})
				mustCreate(keys[1], map[string]string{"test_group": group, "location": "bedroom", "unit": "celsius"})
				mustCreate(keys[2], map[string]string{"test_group": group, "location": "kitchen"})
			})

			AfterEach(func() {
				if client == nil {
					return
				}
				_ = client.Del(ctx, keys...).Err()
				Expect(client.Close()).NotTo(HaveOccurred())
				client = nil
			})

			It("lists every distinct label name across the matching series", func() {
				// The reply includes the label name used in the filter
				// itself and is unordered and deduplicated.
				labels, err := client.TSQueryLabels(ctx, groupFilter).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(labels).To(ConsistOf("test_group", "location", "unit"))
			})

			It("lists the deduplicated union of a label's values", func() {
				values, err := client.TSQueryLabelValues(ctx, "location", groupFilter).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(values).To(ConsistOf("kitchen", "bedroom"))

				// Series without the label contribute nothing.
				values, err = client.TSQueryLabelValues(ctx, "unit", groupFilter).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(values).To(ConsistOf("celsius"))
			})

			It("returns an empty reply for a label on no matching series", func() {
				values, err := client.TSQueryLabelValues(ctx, "no-such-label", groupFilter).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(values).To(BeEmpty())
			})

			It("matches label names byte-exactly", func() {
				// A case variant is a different label.
				values, err := client.TSQueryLabelValues(ctx, "Location", groupFilter).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(values).To(BeEmpty())
			})

			It("queries all indexed series when the filter is omitted", func() {
				labels, err := client.TSQueryLabels(ctx, nil).Result()
				Expect(err).NotTo(HaveOccurred())
				// FlushDB in BeforeEach makes this spec's series the only
				// indexed ones in the database.
				Expect(labels).To(ConsistOf("test_group", "location", "unit"))
			})

			It("propagates server-side filter errors as-is", func() {
				// A filter list without an inclusive matcher is rejected by
				// the server (TSDB: prefix), not by the client.
				err := client.TSQueryLabels(ctx, []string{"test_group!=" + group}).Err()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("TSDB:"))
			})
		})
	}
})
