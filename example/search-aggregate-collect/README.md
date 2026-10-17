# FT.AGGREGATE COLLECT reducer example

This example demonstrates the `COLLECT` reducer for `FT.AGGREGATE`: a
`GROUPBY` reducer that, within each group, projects a chosen set of fields
from every row — optionally deduplicated, sorted and limited — and returns
them as an array of per-entry maps under the reducer alias. A single
aggregation can therefore return each group **and** its top-N members.

## Prerequisites

Redis 8.8+ with the Search module, listening on `localhost:6379`. COLLECT is
gated behind unstable search features; the example enables the gate itself
with `CONFIG SET search-enable-unstable-features yes`.

The client is configured with `Protocol: 2` because the RESP3 response shape
of `FT.AGGREGATE` is still marked unstable; set `UnstableResp3: true` instead
if you want to use RESP3. `AggregateRow.Collect` decodes both the RESP2 and
RESP3 entry shapes, so no other code changes are needed.

## Run

```shell
go run .
```

The example:

1. Creates an index `idx:products` over hashes prefixed with `product:`.
2. Seeds 10 products with `name`, `category`, `brand`, `price`, `rating`.
3. Runs two aggregations:
   - **Top-2 per category (options struct)** — `GROUPBY @category` with
     `REDUCE COLLECT ... FIELDS 2 @name @price SORTBY 2 @rating DESC LIMIT 0 2`,
     built with `redis.NewCollectReducer` next to a plain `COUNT` reducer.
   - **Whole documents per brand (builder)** — `LOAD *` → `GROUPBY @brand` →
     `COLLECT FIELDS *`, expressed with the fluent `AggregateBuilder.Collect`.
     (On current unstable server builds, `SORTBY` combined with `FIELDS *`
     drops the sort field from the entries — sort with an explicit field list
     instead, as in the first query.)

Replies are decoded with `AggregateRow.Collect`, which returns the collected
entries as `[]map[string]interface{}` regardless of protocol.
