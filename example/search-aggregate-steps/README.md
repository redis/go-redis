# FT.AGGREGATE step-based pipeline example

This example demonstrates `FTAggregateOptions.Steps`, which lets `LOAD`,
`APPLY`, `GROUPBY` and `SORTBY` appear multiple times in any order in an
`FT.AGGREGATE` pipeline.

The main motivation is shard-level trimming: running a `SORTBY ... MAX N`
**before** a `GROUPBY` so each shard only ships its top-N rows onward.

## Prerequisites

A Redis instance with the Search module loaded (Redis 8+, or Redis Stack)
listening on `localhost:6379`.

The client is configured with `Protocol: 2` because the RESP3 response shape
of `FT.AGGREGATE` is still marked unstable; set `UnstableResp3: true` instead
if you want to use RESP3.

## Run

```shell
go run .
```

The example:

1. Creates an index `idx:products` over hashes prefixed with `product:`.
2. Seeds 10 products with `category`, `brand`, `price`, `quantity`, `rating`.
3. Runs three aggregations:
   - **Trim before GROUPBY** — `SORTBY @rating DESC MAX 5` → `GROUPBY @category` → `SORTBY @price_total DESC`.
   - **Multi-stage pipeline** — `LOAD` → `LOAD` → `APPLY` → `SORTBY MAX` → `GROUPBY` → `APPLY` → `SORTBY`.
   - **AggregateBuilder** — the same first query expressed via the fluent `NewAggregateBuilder` API.
