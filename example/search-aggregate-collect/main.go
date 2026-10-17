// Example demonstrating the FT.AGGREGATE COLLECT reducer.
//
// COLLECT is a GROUPBY reducer that, within each group, projects a chosen
// set of fields from every row — optionally deduplicated, sorted and
// limited — and emits them as an array of per-entry maps under the reducer
// alias. A single aggregation can therefore return each group AND its
// top-N members.
//
// COLLECT requires Redis 8.8+ with unstable search features enabled
// (CONFIG SET search-enable-unstable-features yes); the example enables
// the setting itself.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

const indexName = "idx:products"

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Protocol: 2,
	})
	defer rdb.Close()

	// COLLECT is gated behind unstable search features (Redis 8.8+).
	if err := rdb.ConfigSet(ctx, "search-enable-unstable-features", "yes").Err(); err != nil {
		log.Fatalf("cannot enable search unstable features (COLLECT needs Redis 8.8+ with search): %v", err)
	}

	if err := rdb.FlushDB(ctx).Err(); err != nil {
		log.Fatalf("flushdb: %v", err)
	}
	createIndex(ctx, rdb)
	seedProducts(ctx, rdb)

	fmt.Println("# 1. Top-2 products per category (options struct)")
	topPerCategory(ctx, rdb)

	fmt.Println("\n# 2. Whole documents per brand (builder, LOAD * + FIELDS *)")
	documentsPerBrand(ctx, rdb)
}

func createIndex(ctx context.Context, rdb *redis.Client) {
	_, err := rdb.FTCreate(ctx, indexName,
		&redis.FTCreateOptions{OnHash: true, Prefix: []interface{}{"product:"}},
		&redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText},
		&redis.FieldSchema{FieldName: "category", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "brand", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "price", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "rating", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
	).Result()
	if err != nil {
		log.Fatalf("ftcreate: %v", err)
	}
}

func seedProducts(ctx context.Context, rdb *redis.Client) {
	products := []map[string]interface{}{
		{"name": "Roadster", "category": "bike", "brand": "Velorim", "price": 270, "rating": 4.5},
		{"name": "Commuter", "category": "bike", "brand": "Velorim", "price": 810, "rating": 4.1},
		{"name": "Summit Pro", "category": "bike", "brand": "Bicyk", "price": 2300, "rating": 4.9},
		{"name": "Gravel GT", "category": "bike", "brand": "Bicyk", "price": 430, "rating": 3.8},
		{"name": "AirFlow", "category": "helmet", "brand": "Nord", "price": 120, "rating": 4.7},
		{"name": "SafeRide", "category": "helmet", "brand": "Nord", "price": 80, "rating": 4.0},
		{"name": "ProShell", "category": "helmet", "brand": "Peaknetic", "price": 200, "rating": 4.8},
		{"name": "BeamMax", "category": "light", "brand": "Peaknetic", "price": 45, "rating": 4.3},
		{"name": "NightGlow", "category": "light", "brand": "Nord", "price": 30, "rating": 3.9},
		{"name": "LumenX", "category": "light", "brand": "Velorim", "price": 65, "rating": 4.6},
	}
	for i, p := range products {
		key := fmt.Sprintf("product:%d", i)
		if err := rdb.HSet(ctx, key, p).Err(); err != nil {
			log.Fatalf("hset %s: %v", key, err)
		}
	}
}

// topPerCategory groups products by category and, inside each group,
// collects the name and price of the two highest-rated products:
//
//	GROUPBY 1 @category
//	  REDUCE COUNT 0 AS n
//	  REDUCE COLLECT 12 FIELDS 2 @name @price SORTBY 2 @rating DESC LIMIT 0 2
//	  AS top_products
//
// NewCollectReducer assembles the token list and computes the argument
// count; field and sort names may be given with or without the "@" prefix.
func topPerCategory(ctx context.Context, rdb *redis.Client) {
	reducer, err := redis.NewCollectReducer(redis.FTAggregateCollect{
		Fields: []string{"name", "price"},
		SortBy: []redis.FTAggregateSortBy{{FieldName: "rating", Desc: true}},
		Limit:  &redis.FTAggregateCollectLimit{Offset: 0, Count: 2},
		As:     "top_products",
	})
	if err != nil {
		log.Fatalf("collect reducer: %v", err)
	}

	res, err := rdb.FTAggregateWithArgs(ctx, indexName, "*", &redis.FTAggregateOptions{
		GroupBy: []redis.FTAggregateGroupBy{{
			Fields: []interface{}{"@category"},
			Reduce: []redis.FTAggregateReducer{
				{Reducer: redis.SearchCount, As: "n"},
				reducer,
			},
		}},
	}).Result()
	if err != nil {
		log.Fatalf("aggregate: %v", err)
	}

	for _, row := range res.Rows {
		fmt.Printf("  category=%v n=%v\n", row.Fields["category"], row.Fields["n"])
		printEntries(row, "top_products")
	}
}

// documentsPerBrand expresses the same reducer through the fluent builder.
// FIELDS * projects every field present in the pipeline at the COLLECT
// stage, so it is paired with an upstream LOAD * to collect complete
// documents. Entry order is unspecified without a SORTBY; on current
// unstable server builds, SORTBY combined with FIELDS * drops the sort
// field from the entries, so sort with an explicit Fields list instead
// (as in topPerCategory).
func documentsPerBrand(ctx context.Context, rdb *redis.Client) {
	res, err := rdb.NewAggregateBuilder(ctx, indexName, "*").
		LoadAll().
		GroupBy("@brand").
		Collect(redis.FTAggregateCollect{
			FieldsAll: true,
			As:        "products",
		}).
		Run()
	if err != nil {
		log.Fatalf("aggregate: %v", err)
	}

	for _, row := range res.Rows {
		fmt.Printf("  brand=%v\n", row.Fields["brand"])
		printEntries(row, "products")
	}
}

// printEntries decodes the COLLECT column stored under alias with
// AggregateRow.Collect, which hides the RESP2/RESP3 representation
// difference, and prints one line per collected entry.
func printEntries(row redis.AggregateRow, alias string) {
	entries, err := row.Collect(alias)
	if err != nil {
		log.Fatalf("decode %s: %v", alias, err)
	}
	for _, e := range entries {
		fmt.Printf("    %v\n", e)
	}
}
