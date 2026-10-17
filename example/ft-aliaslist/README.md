# FT.ALIASLIST example

This example demonstrates `FTAliasList`, the client binding for
`FT.ALIASLIST` (Redis 8.10+): given an index name, it returns all aliases
currently associated with that index.

Key points shown by the example:

- An index with no aliases yields an empty result, not an error.
- The reply is an unordered, deduplicated collection of alias names; do not
  rely on its order.
- The argument must be an index created with `FT.CREATE`. Passing an alias is
  not resolved and fails with the server's `SEARCH_INDEX_NOT_FOUND` error —
  the same error a missing index produces.

## Prerequisites

Redis 8.10+ with the Query Engine (search) module, listening on
`localhost:6379`.

## Run

```shell
go run .
```

Expected output (alias order may differ):

```
aliases right after FT.CREATE: 0
aliases after FT.ALIASADD: [cities cities-latest]
FT.ALIASLIST with an alias as argument: SEARCH_INDEX_NOT_FOUND Index not found: cities
```
