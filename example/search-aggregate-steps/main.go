// Example demonstrating the step-based FT.AGGREGATE API.
//
// FTAggregateOptions.Steps lets LOAD, APPLY, GROUPBY and SORTBY appear
// multiple times in any order. This is useful for shard-level trimming
// (SORTBY ... MAX N) before GROUPBY and multi-stage pipelines.
//
// Requires a Redis instance with the Search module (e.g. Redis 8+).
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

	if err := rdb.FlushDB(ctx).Err(); err != nil {
		log.Fatalf("flushdb: %v", err)
	}
	createIndex(ctx, rdb)
	seedProducts(ctx, rdb)

	fmt.Println("# 1. Trim before GROUPBY (real-world use case)")
	trimBeforeGroup(ctx, rdb)

	fmt.Println("\n# 2. Multi-stage pipeline (LOAD -> APPLY -> SORTBY -> GROUPBY -> APPLY -> SORTBY)")
	multiStagePipeline(ctx, rdb)

	fmt.Println("\n# 3. Same as #1 but using AggregateBuilder")
	withBuilder(ctx, rdb)
}

func createIndex(ctx context.Context, rdb *redis.Client) {
	_, err := rdb.FTCreate(ctx, indexName,
		&redis.FTCreateOptions{OnHash: true, Prefix: []interface{}{"product:"}},
		&redis.FieldSchema{FieldName: "category", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "brand", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "price", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "quantity", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "rating", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
	).Result()
	if err != nil {
		log.Fatalf("ftcreate: %v", err)
	}
}

func seedProducts(ctx context.Context, rdb *redis.Client) {
	products := []map[string]interface{}{
		{"category": "bike", "brand": "Velorim", "price": 270, "quantity": 12, "rating": 4.5},
		{"category": "bike", "brand": "Velorim", "price": 810, "quantity": 3, "rating": 4.1},
		{"category": "bike", "brand": "Bicyk", "price": 2300, "quantity": 1, "rating": 4.9},
		{"category": "bike", "brand": "Bicyk", "price": 430, "quantity": 7, "rating": 3.8},
		{"category": "helmet", "brand": "Nord", "price": 120, "quantity": 25, "rating": 4.7},
		{"category": "helmet", "brand": "Nord", "price": 80, "quantity": 40, "rating": 4.0},
		{"category": "helmet", "brand": "Peaknetic", "price": 200, "quantity": 10, "rating": 4.8},
		{"category": "light", "brand": "Peaknetic", "price": 45, "quantity": 60, "rating": 4.3},
		{"category": "light", "brand": "Nord", "price": 30, "quantity": 100, "rating": 3.9},
		{"category": "light", "brand": "Velorim", "price": 65, "quantity": 35, "rating": 4.6},
	}
	for i, p := range products {
		key := fmt.Sprintf("product:%d", i)
		if err := rdb.HSet(ctx, key, p).Err(); err != nil {
			log.Fatalf("hset %s: %v", key, err)
		}
	}
}

// trimBeforeGroup shows SORTBY ... MAX N applied BEFORE a GROUPBY
// (the pattern that the old pipeline-ordered API could not express).
func trimBeforeGroup(ctx context.Context, rdb *redis.Client) {
	opts := &redis.FTAggregateOptions{
		Steps: []redis.FTAggregateStep{
			// Keep only the top 5 highest-rated products per shard.
			{SortBy: &redis.FTAggregateSortByStep{
				Fields: []redis.FTAggregateSortBy{{FieldName: "@rating", Desc: true}},
				Max:    5,
			}},
			// Sum their price per category.
			{GroupBy: &redis.FTAggregateGroupBy{
				Fields: []interface{}{"@category"},
				Reduce: []redis.FTAggregateReducer{
					{Reducer: redis.SearchSum, Args: []interface{}{"@price"}, As: "price_total"},
					{Reducer: redis.SearchCount, As: "n"},
				},
			}},
			// Sort categories by the aggregated total.
			{SortBy: &redis.FTAggregateSortByStep{
				Fields: []redis.FTAggregateSortBy{{FieldName: "@price_total", Desc: true}},
			}},
		},
	}
	printRows(rdb.FTAggregateWithArgs(ctx, indexName, "*", opts).Result())
}

func multiStagePipeline(ctx context.Context, rdb *redis.Client) {
	opts := &redis.FTAggregateOptions{
		Steps: []redis.FTAggregateStep{
			{Load: &redis.FTAggregateLoad{Field: "@price"}},
			{Load: &redis.FTAggregateLoad{Field: "@quantity"}},
			{Apply: &redis.FTAggregateApply{Field: "@price * @quantity", As: "stock_value"}},
			{SortBy: &redis.FTAggregateSortByStep{
				Fields: []redis.FTAggregateSortBy{{FieldName: "@stock_value", Desc: true}},
				Max:    8,
			}},
			{GroupBy: &redis.FTAggregateGroupBy{
				Fields: []interface{}{"@brand"},
				Reduce: []redis.FTAggregateReducer{
					{Reducer: redis.SearchSum, Args: []interface{}{"@stock_value"}, As: "brand_value"},
				},
			}},
			{Apply: &redis.FTAggregateApply{Field: "floor(@brand_value)", As: "brand_value"}},
			{SortBy: &redis.FTAggregateSortByStep{
				Fields: []redis.FTAggregateSortBy{{FieldName: "@brand_value", Desc: true}},
			}},
		},
	}
	printRows(rdb.FTAggregateWithArgs(ctx, indexName, "*", opts).Result())
}

func withBuilder(ctx context.Context, rdb *redis.Client) {
	res, err := rdb.NewAggregateBuilder(ctx, indexName, "*").
		SortBy("@rating", false).SortByMax(5).
		GroupBy("@category").
		ReduceAs(redis.SearchSum, "price_total", "@price").
		ReduceAs(redis.SearchCount, "n").
		SortBy("@price_total", false).
		Run()
	printRows(res, err)
}

func printRows(res *redis.FTAggregateResult, err error) {
	if err != nil {
		log.Fatalf("aggregate: %v", err)
	}
	for _, row := range res.Rows {
		fmt.Printf("  %v\n", row.Fields)
	}
}
