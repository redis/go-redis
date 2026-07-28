package redis_test

import (
	"context"
	"fmt"
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// FT.ALIASLIST ships in Redis 8.10 (Query Engine 8.10.0). The specs gate on
// SkipBeforeRedisVersion, which compares major.minor as integers, and run
// under both RESP2 and RESP3 against the suite's standard server.
var _ = Describe("FT.ALIASLIST", Label("search", "ftaliaslist"), func() {
	ctx := context.TODO()

	for _, protocol := range []int{2, 3} {
		protocol := protocol

		Describe(fmt.Sprintf("RESP%d", protocol), func() {
			var client *redis.Client

			index := fmt.Sprintf("aliaslist-test-idx-resp%d", protocol)
			aliases := []string{
				fmt.Sprintf("aliaslist-test-a1-resp%d", protocol),
				fmt.Sprintf("aliaslist-test-a2-resp%d", protocol),
			}

			createIndex := func() {
				Expect(client.FTCreate(ctx, index, &redis.FTCreateOptions{
					OnHash: true,
					Prefix: []interface{}{fmt.Sprintf("aliaslist-test-doc-resp%d:", protocol)},
				}, &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}).Err()).NotTo(HaveOccurred())
			}

			BeforeEach(func() {
				SkipBeforeRedisVersion("8.10", "FT.ALIASLIST requires Redis 8.10+")
				client = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: protocol})
				Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				if client == nil {
					return
				}
				// Aliases are global, not per-database; FLUSHDB alone does not
				// remove them unless their index is dropped with them.
				for _, a := range aliases {
					_ = client.FTAliasDel(ctx, a).Err()
				}
				_ = client.FTDropIndex(ctx, index).Err()
				Expect(client.Close()).NotTo(HaveOccurred())
				client = nil
			})

			It("returns an empty collection for an index with no aliases", func() {
				createIndex()

				// An existing index with no aliases yields an empty
				// collection, not an error.
				got, err := client.FTAliasList(ctx, index).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeEmpty())
			})

			It("lists all aliases associated with the index", func() {
				createIndex()

				for _, a := range aliases {
					Expect(client.FTAliasAdd(ctx, index, a).Err()).NotTo(HaveOccurred())
				}

				// The reply is an unordered collection; do not assert order.
				got, err := client.FTAliasList(ctx, index).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(ConsistOf(aliases[0], aliases[1]))
			})

			It("propagates the server index-not-found error for a missing index", func() {
				err := client.FTAliasList(ctx, fmt.Sprintf("aliaslist-test-missing-resp%d", protocol)).Err()
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(ContainSubstring("index not found"))
			})

			It("does not resolve an alias passed in place of the index", func() {
				createIndex()

				Expect(client.FTAliasAdd(ctx, index, aliases[0]).Err()).NotTo(HaveOccurred())

				// The argument must be the index name; the server does not
				// resolve aliases here and replies with the same
				// index-not-found error a missing index produces.
				err := client.FTAliasList(ctx, aliases[0]).Err()
				Expect(err).To(HaveOccurred())
				Expect(strings.ToLower(err.Error())).To(ContainSubstring("index not found"))
			})
		})
	}
})
