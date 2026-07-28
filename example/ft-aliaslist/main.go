// Example demonstrating FT.ALIASLIST: listing the aliases of a search index.
//
// FT.ALIASADD, FT.ALIASUPDATE and FT.ALIASDEL manage index aliases, but until
// Redis 8.10 there was no direct way to list them. FT.ALIASLIST closes that
// gap: given an index name, it returns all aliases currently associated with
// that index as an unordered, deduplicated collection.
//
// Requires Redis 8.10+ with the Query Engine (search) module.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

const indexName = "idx:cities"

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	setup(ctx, rdb)
	defer cleanup(ctx, rdb)

	// A freshly created index has no aliases: the reply is empty, not an error.
	aliases, err := rdb.FTAliasList(ctx, indexName).Result()
	if err != nil {
		log.Fatalf("FT.ALIASLIST: %v", err)
	}
	fmt.Printf("aliases right after FT.CREATE: %d\n", len(aliases))

	// Point two aliases at the index.
	for _, alias := range []string{"cities", "cities-latest"} {
		if err := rdb.FTAliasAdd(ctx, indexName, alias).Err(); err != nil {
			log.Fatalf("FT.ALIASADD %s: %v", alias, err)
		}
	}

	// List them. The server returns the set of alias names; order is not
	// part of the contract, so treat the slice as an unordered collection.
	aliases, err = rdb.FTAliasList(ctx, indexName).Result()
	if err != nil {
		log.Fatalf("FT.ALIASLIST: %v", err)
	}
	fmt.Printf("aliases after FT.ALIASADD: %v\n", aliases)

	// The argument must be the index name, not an alias: the server does not
	// resolve aliases here and replies with its index-not-found error.
	if err := rdb.FTAliasList(ctx, "cities").Err(); err != nil {
		fmt.Printf("FT.ALIASLIST with an alias as argument: %v\n", err)
	}
}

func setup(ctx context.Context, rdb *redis.Client) {
	err := rdb.FTCreate(ctx, indexName,
		&redis.FTCreateOptions{OnHash: true, Prefix: []interface{}{"city:"}},
		&redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText},
	).Err()
	if err != nil {
		log.Fatalf("FT.CREATE (requires Redis 8.10+ with search): %v", err)
	}
}

func cleanup(ctx context.Context, rdb *redis.Client) {
	for _, alias := range []string{"cities", "cities-latest"} {
		_ = rdb.FTAliasDel(ctx, alias).Err()
	}
	_ = rdb.FTDropIndex(ctx, indexName).Err()
}
