package redis_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// collectTestAddr is the standalone Redis used by the COLLECT integration
// test. Defaults to :6379 (the repo's standard test server) and can be
// overridden, e.g. when a Redis 8.8 + search-enable-unstable-features instance
// runs on a different port:
//
//	REDIS_COLLECT_TEST_ADDR=localhost:6399 go test -run TestFTAggregateCollect_Integration
func collectTestAddr() string {
	if a := os.Getenv("REDIS_COLLECT_TEST_ADDR"); a != "" {
		return a
	}
	return ":6379"
}

// TestFTAggregateCollect_Integration exercises the COLLECT reducer end-to-end
// against a live server, through both the options-struct (NewCollectReducer)
// and fluent-builder (AggregateBuilder.Collect) surfaces, and decodes the
// reply with AggregateRow.Collect under both RESP2 and RESP3.
//
// COLLECT requires Redis 8.8+ with search-enable-unstable-features enabled.
// The test skips if the server is unreachable, lacks the search module, or
// does not allow enabling unstable features.
func TestFTAggregateCollect_Integration(t *testing.T) {
	// COLLECT reducer ships in Redis 8.8. Older search builds accept the
	// search-enable-unstable-features config but still reject the reducer
	// ("No such reducer: COLLECT"), so gate on the live server's version
	// (INFO server redis_version) rather than on the config toggle or the
	// REDIS_VERSION env variable (whose float form cannot tell 8.10 from 8.1
	// and defaults when unset).
	probeCtx := context.Background()
	probe := redis.NewClient(&redis.Options{Addr: collectTestAddr()})
	t.Cleanup(func() { _ = probe.Close() })
	if err := probe.Ping(probeCtx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", collectTestAddr(), err)
	}
	major, minor, err := collectServerVersion(probeCtx, probe)
	if err != nil {
		t.Fatalf("cannot determine server version: %v", err)
	}
	if major < 8 || (major == 8 && minor < 8) {
		t.Skipf("COLLECT requires Redis 8.8+ (server is %d.%d)", major, minor)
	}
	for _, proto := range []int{2, 3} {
		proto := proto
		t.Run(fmt.Sprintf("RESP%d", proto), func(t *testing.T) {
			ctx := context.Background()
			opt := &redis.Options{Addr: collectTestAddr(), Protocol: proto}
			if proto == 3 {
				opt.UnstableResp3 = true
			}
			client := redis.NewClient(opt)
			t.Cleanup(func() { _ = client.Close() })

			if err := client.Ping(ctx).Err(); err != nil {
				t.Skipf("redis not reachable at %s: %v", collectTestAddr(), err)
			}
			// COLLECT is gated behind unstable features; skip if we cannot
			// enable it. Remember the prior value and restore it on cleanup so
			// the test does not leak config changes into the shared server.
			const unstableCfg = "search-enable-unstable-features"
			prevCfg, err := client.ConfigGet(ctx, unstableCfg).Result()
			if err != nil {
				t.Skipf("cannot read %s (need Redis 8.8+ with search): %v", unstableCfg, err)
			}
			if err := client.ConfigSet(ctx, unstableCfg, "yes").Err(); err != nil {
				t.Skipf("cannot enable search unstable features (need Redis 8.8+ with search): %v", err)
			}
			t.Cleanup(func() {
				if prev, ok := prevCfg[unstableCfg]; ok {
					_ = client.ConfigSet(ctx, unstableCfg, prev).Err()
				}
			})

			// Clear only this test's keys (never FlushDB: the server is shared).
			docKeys := []string{"fruit:1", "fruit:2", "fruit:3", "fruit:4"}
			if err := client.Del(ctx, docKeys...).Err(); err != nil {
				t.Fatalf("Del %v: %v", docKeys, err)
			}

			const idx = "idx_collect_it"
			if err := client.Do(ctx,
				"FT.CREATE", idx, "ON", "HASH", "PREFIX", 1, "fruit:",
				"SCHEMA", "color", "TAG", "name", "TEXT", "sweetness", "NUMERIC", "SORTABLE",
			).Err(); err != nil {
				if strings.Contains(strings.ToUpper(err.Error()), "UNKNOWN COMMAND") {
					t.Skipf("search module not loaded: %v", err)
				}
				t.Fatalf("FT.CREATE: %v", err)
			}
			// Drop the index and its documents (DD) on cleanup.
			t.Cleanup(func() {
				_ = client.FTDropIndexWithArgs(ctx, idx,
					&redis.FTDropIndexOptions{DeleteDocs: true}).Err()
			})

			docs := []struct {
				key, color, name string
				sweetness        interface{}
			}{
				{"fruit:1", "red", "apple", 4},
				{"fruit:2", "red", "strawberry", 3},
				{"fruit:3", "yellow", "banana", 4},
				{"fruit:4", "yellow", "lemon", nil}, // sparse: no sweetness
			}
			for _, d := range docs {
				vals := []interface{}{"color", d.color, "name", d.name}
				if d.sweetness != nil {
					vals = append(vals, "sweetness", d.sweetness)
				}
				if err := client.HSet(ctx, d.key, vals...).Err(); err != nil {
					t.Fatalf("HSet %s: %v", d.key, err)
				}
			}

			// --- Surface 1: options-struct via NewCollectReducer ---
			reducer, err := redis.NewCollectReducer(redis.FTAggregateCollect{
				Fields: []string{"name", "sweetness"}, // no @ prefix: must be normalized
				SortBy: []redis.FTAggregateSortBy{{FieldName: "sweetness", Desc: true}},
				As:     "fruits",
			})
			if err != nil {
				t.Fatalf("NewCollectReducer: %v", err)
			}
			res, err := client.FTAggregateWithArgs(ctx, idx, "*", &redis.FTAggregateOptions{
				GroupBy: []redis.FTAggregateGroupBy{{
					Fields: []interface{}{"@color"},
					Reduce: []redis.FTAggregateReducer{reducer},
				}},
			}).Result()
			if err != nil {
				t.Fatalf("FTAggregateWithArgs COLLECT: %v", err)
			}
			assertCollectGroups(t, res)

			// --- Surface 2: fluent builder ---
			res2, err := client.NewAggregateBuilder(ctx, idx, "*").
				GroupBy("@color").
				Collect(redis.FTAggregateCollect{
					FieldsAll: true,
					SortBy:    []redis.FTAggregateSortBy{{FieldName: "sweetness", Desc: true}},
					As:        "fruits",
				}).
				Run()
			if err != nil {
				t.Fatalf("builder COLLECT: %v", err)
			}
			// FIELDS * projects pipeline fields (color group key + name + sweetness).
			if len(res2.Rows) != 2 {
				t.Fatalf("builder: got %d groups, want 2", len(res2.Rows))
			}
			for _, row := range res2.Rows {
				col, err := row.Collect("fruits")
				if err != nil {
					t.Fatalf("builder row.Collect: %v", err)
				}
				if len(col) == 0 {
					t.Fatalf("builder: empty collect column for row %v", row.Fields)
				}
			}
		})
	}
}

// assertCollectGroups verifies the two color groups, ordering within a group,
// and the sparse entry (lemon has no sweetness).
func assertCollectGroups(t *testing.T, res *redis.FTAggregateResult) {
	t.Helper()
	if len(res.Rows) != 2 {
		t.Fatalf("got %d groups, want 2 (red, yellow)", len(res.Rows))
	}

	byColor := map[string]redis.CollectColumn{}
	for _, row := range res.Rows {
		color, _ := row.Fields["color"].(string)
		col, err := row.Collect("fruits")
		if err != nil {
			t.Fatalf("row.Collect(fruits) for color=%q: %v", color, err)
		}
		byColor[color] = col
	}

	red := byColor["red"]
	if len(red) != 2 {
		t.Fatalf("red group: got %d entries, want 2", len(red))
	}
	// SORTBY sweetness DESC: apple(4) before strawberry(3).
	if got := red[0]["name"]; got != "apple" {
		t.Fatalf("red[0].name = %v, want apple (DESC sweetness order)", got)
	}
	if got := red[1]["name"]; got != "strawberry" {
		t.Fatalf("red[1].name = %v, want strawberry", got)
	}

	yellow := byColor["yellow"]
	if len(yellow) != 2 {
		t.Fatalf("yellow group: got %d entries, want 2", len(yellow))
	}
	// Sparse: the lemon entry must omit sweetness (no NULL placeholder).
	names := []string{yellow[0]["name"].(string), yellow[1]["name"].(string)}
	sort.Strings(names)
	if names[0] != "banana" || names[1] != "lemon" {
		t.Fatalf("yellow names = %v, want [banana lemon]", names)
	}
	for _, e := range yellow {
		if e["name"] == "lemon" {
			if _, ok := e["sweetness"]; ok {
				t.Fatalf("lemon entry must be sparse (no sweetness), got %v", e)
			}
		}
	}
}

// collectServerVersion returns the major and minor components of the live
// server's redis_version, taken from INFO server. Unlike the float-typed
// RedisVersion (parsed from the REDIS_VERSION env variable), integer
// major/minor comparison keeps 8.10 > 8.8 and reflects the server actually
// answering, not the environment.
func collectServerVersion(ctx context.Context, client *redis.Client) (major, minor int, err error) {
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(info, "\n") {
		v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:")
		if !ok {
			continue
		}
		parts := strings.SplitN(v, ".", 3)
		if len(parts) < 2 {
			return 0, 0, fmt.Errorf("unexpected redis_version %q in INFO server", v)
		}
		major, err = strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, fmt.Errorf("unexpected redis_version %q in INFO server: %w", v, err)
		}
		minor, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("unexpected redis_version %q in INFO server: %w", v, err)
		}
		return major, minor, nil
	}
	return 0, 0, fmt.Errorf("redis_version not found in INFO server reply")
}
