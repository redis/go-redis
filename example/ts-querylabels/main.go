// Example demonstrating TS.QUERYLABELS: discovering label names and label
// values across time series.
//
// TS.QUERYLABELS (Redis 8.10+) completes the drill-down flow dashboarding
// tools such as Grafana use to populate variable dropdowns:
//
//  1. TSQueryLabels      - all label names for a group of series
//  2. TSQueryLabelValues - all values of a chosen label for that group
//  3. TSQueryIndex       - all keys matching the fully-selected filter
//
// Requires Redis 8.10+ with the TimeSeries module.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	keys := seed(ctx, rdb)
	defer rdb.Del(ctx, keys...)

	// Step 1: first dropdown - which labels does the sensor group carry?
	// The reply is unordered, deduplicated, and includes the label used in
	// the filter itself ("type").
	labels, err := rdb.TSQueryLabels(ctx, []string{"type=sensor"}).Result()
	if err != nil {
		log.Fatalf("TS.QUERYLABELS LABELS: %v", err)
	}
	fmt.Printf("label names for type=sensor: %v\n", labels)

	// Step 2: second dropdown - which values does "location" take in that
	// group? Series without the label contribute nothing.
	locations, err := rdb.TSQueryLabelValues(ctx, "location", []string{"type=sensor"}).Result()
	if err != nil {
		log.Fatalf("TS.QUERYLABELS VALUES: %v", err)
	}
	fmt.Printf("locations for type=sensor:   %v\n", locations)

	// Step 3: third dropdown - the keys matching the fully-selected filter
	// (existing TS.QUERYINDEX).
	series, err := rdb.TSQueryIndex(ctx, []string{"type=sensor", "location=kitchen"}).Result()
	if err != nil {
		log.Fatalf("TS.QUERYINDEX: %v", err)
	}
	fmt.Printf("kitchen sensor series:       %v\n", series)

	// A label that no matching series carries yields an empty reply, not an
	// error - dashboards keep working.
	missing, err := rdb.TSQueryLabelValues(ctx, "rack", []string{"type=sensor"}).Result()
	if err != nil {
		log.Fatalf("TS.QUERYLABELS VALUES rack: %v", err)
	}
	fmt.Printf("values of an absent label:   %d\n", len(missing))

	// Passing no filters queries every indexed series in the database.
	all, err := rdb.TSQueryLabels(ctx, nil).Result()
	if err != nil {
		log.Fatalf("TS.QUERYLABELS LABELS (no filter): %v", err)
	}
	fmt.Printf("label names, all series:     %v\n", all)
}

func seed(ctx context.Context, rdb *redis.Client) []string {
	series := map[string]map[string]string{
		"ts:temp:kitchen": {"type": "sensor", "location": "kitchen", "unit": "celsius"},
		"ts:temp:bedroom": {"type": "sensor", "location": "bedroom", "unit": "celsius"},
		"ts:hum:kitchen":  {"type": "sensor", "location": "kitchen"},
		"ts:build:count":  {"type": "counter"},
	}
	keys := make([]string, 0, len(series))
	for key, labels := range series {
		if err := rdb.TSCreateWithArgs(ctx, key, &redis.TSOptions{Labels: labels}).Err(); err != nil {
			log.Fatalf("TS.CREATE %s (requires Redis 8.10+ with timeseries): %v", key, err)
		}
		keys = append(keys, key)
	}
	return keys
}
