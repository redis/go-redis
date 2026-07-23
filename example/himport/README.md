# HIMPORT example

Fast hash ingestion with the HIMPORT command family (hinted hash templates,
Redis 8.10+).

`HIMPORT PREPARE` registers an ordered list of field names (a *fieldset*)
inside a connection's session; `HIMPORT SET` then creates hashes by sending
only the values, paired positionally with the prepared fields. The server
stores such hashes in a memory-efficient template encoding where the shared
field names are kept only once.

Fieldsets are scoped to a single physical connection on the server. go-redis
hides that from pooled callers: fieldsets registered with `HImportPrepare`
are remembered client-side and the `PREPARE` is replayed lazily — at most
once per connection session — on whichever pooled connection executes an
`HImportSet`, including inside pipelines and transactions. A connection that
lost its session state (`RESET`, reconnect) is re-prepared transparently.

The example walks through:

- basic prepare/set and reading the result back with regular hash commands
- bulk ingestion through pipelines, timed against plain `HSET` pipelines
- 100 concurrent goroutines writing through the pool with no explicit
  connection management
- the observable server side: `OBJECT ENCODING` template encodings and the
  `INFO memory` template counters
- discarding a fieldset and the resulting server error for further sets

## Run

Requires a Redis 8.10+ server built with the hinted hash templates feature
(the example exits with a message otherwise). The address defaults to
`localhost:6379` and can be overridden with `REDIS_ADDR`:

```sh
go run .
# or
REDIS_ADDR=localhost:6390 go run .
```
