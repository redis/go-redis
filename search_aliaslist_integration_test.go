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

// aliasListTestAddr is the standalone Redis used by the FT.ALIASLIST
// integration test. Defaults to :6379 (the repo's standard test server) and
// can be overridden when a Redis 8.10 instance runs on a different port:
//
//	REDIS_ALIASLIST_TEST_ADDR=localhost:6399 go test -run TestFTAliasList_Integration
func aliasListTestAddr() string {
	if a := os.Getenv("REDIS_ALIASLIST_TEST_ADDR"); a != "" {
		return a
	}
	return ":6379"
}

// TestFTAliasList_Integration exercises FT.ALIASLIST end-to-end against a
// live server under both RESP2 and RESP3.
//
// FT.ALIASLIST ships in Redis 8.10 (Query Engine 8.10.0). The test gates on
// the command itself rather than on the REDIS_VERSION env variable, whose
// float form cannot tell 8.10 from 8.1; older servers reply with an
// "unknown command" error, which the HLD documents as the expected failure
// mode and this test treats as a skip.
func TestFTAliasList_Integration(t *testing.T) {
	probeCtx := context.Background()
	probe := redis.NewClient(&redis.Options{Addr: aliasListTestAddr()})
	t.Cleanup(func() { _ = probe.Close() })
	if err := probe.Ping(probeCtx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", aliasListTestAddr(), err)
	}
	if err := probe.FTAliasList(probeCtx, "__aliaslist_probe__").Err(); err != nil {
		low := strings.ToLower(err.Error())
		if strings.Contains(low, "unknown command") {
			t.Skipf("FT.ALIASLIST not supported by server (requires Redis 8.10+): %v", err)
		}
		if !strings.Contains(low, "not found") {
			t.Skipf("search module not available: %v", err)
		}
		// "Index not found" means the command exists; proceed.
	}

	for _, proto := range []int{2, 3} {
		proto := proto
		t.Run(fmt.Sprintf("RESP%d", proto), func(t *testing.T) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{Addr: aliasListTestAddr(), Protocol: proto})
			t.Cleanup(func() { _ = client.Close() })

			index := fmt.Sprintf("aliaslist-test-idx-resp%d", proto)
			aliases := []string{
				fmt.Sprintf("aliaslist-test-a1-resp%d", proto),
				fmt.Sprintf("aliaslist-test-a2-resp%d", proto),
			}

			// Pre-clean in case a previous run crashed between creation and
			// cleanup; FT.CREATE fails on an existing index.
			for _, a := range aliases {
				_ = client.FTAliasDel(ctx, a).Err()
			}
			_ = client.FTDropIndex(ctx, index).Err()

			if err := client.FTCreate(ctx, index, &redis.FTCreateOptions{
				OnHash: true,
				Prefix: []interface{}{fmt.Sprintf("aliaslist-test-doc-resp%d:", proto)},
			}, &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}).Err(); err != nil {
				t.Fatalf("FT.CREATE %s: %v", index, err)
			}
			t.Cleanup(func() {
				for _, a := range aliases {
					_ = client.FTAliasDel(ctx, a).Err()
				}
				_ = client.FTDropIndex(ctx, index).Err()
			})

			// An existing index with no aliases yields an empty collection,
			// not an error.
			got, err := client.FTAliasList(ctx, index).Result()
			if err != nil {
				t.Fatalf("FT.ALIASLIST on alias-free index: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("expected no aliases, got %v", got)
			}

			for _, a := range aliases {
				if err := client.FTAliasAdd(ctx, index, a).Err(); err != nil {
					t.Fatalf("FT.ALIASADD %s -> %s: %v", a, index, err)
				}
			}

			got, err = client.FTAliasList(ctx, index).Result()
			if err != nil {
				t.Fatalf("FT.ALIASLIST: %v", err)
			}
			// The reply is an unordered collection; sort before comparing.
			sort.Strings(got)
			want := append([]string(nil), aliases...)
			sort.Strings(want)
			if len(got) != len(want) {
				t.Fatalf("aliases mismatch\n got: %v\nwant: %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("aliases mismatch\n got: %v\nwant: %v", got, want)
				}
			}

			// A missing index propagates the server's index-not-found error
			// unchanged (SEARCH_INDEX_NOT_FOUND prefix, not ERR).
			err = client.FTAliasList(ctx, fmt.Sprintf("aliaslist-test-missing-resp%d", proto)).Err()
			if err == nil {
				t.Fatalf("expected an error for a missing index")
			}
			if !strings.Contains(strings.ToLower(err.Error()), "index not found") {
				t.Fatalf("expected the server index-not-found error verbatim, got: %v", err)
			}

			// An alias passed in place of the index name fails with the same
			// index-not-found error; the server does not resolve aliases here
			// and the client must not either.
			err = client.FTAliasList(ctx, aliases[0]).Err()
			if err == nil {
				t.Fatalf("expected an error when passing an alias instead of an index")
			}
			if !strings.Contains(strings.ToLower(err.Error()), "index not found") {
				t.Fatalf("expected the server index-not-found error verbatim, got: %v", err)
			}
		})
	}
}
