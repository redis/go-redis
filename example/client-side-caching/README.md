# Client-side caching example

This example enables the built-in client-side cache on a standalone go-redis
client. It demonstrates:

- a first read fetched from Redis
- a repeated read served from the local cache
- automatic eviction after another client changes the key
- cache hit and miss statistics through `CSCStats`

go-redis uses Redis `CLIENT TRACKING` and RESP3 invalidation notifications to
keep the local cache synchronized. Invalidations are asynchronous, so the
example waits until the cached client observes the updated value.

## Requirements

- Redis with `CLIENT TRACKING` support
- RESP3
- database 0
- no dynamic credential provider

Fixed `Username` and `Password` values are supported.

## Run

The Redis address defaults to `localhost:6379`. Override it with `REDIS_ADDR`:

```sh
go run .
# or
REDIS_ADDR=localhost:6390 go run .
```

Expected output:

```text
first read: hello
second read: hello
cache stats: 1 hit, 1 miss
after invalidation: hello again
```
