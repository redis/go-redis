## [9.0.2](https://github.com/redis/go-redis/compare/v9.0.1...v9.0.2) (2023-02-01)


### Features

* upgrade OpenTelemetry, use the new metrics API. ([#2410](https://github.com/redis/go-redis/issues/2410)) ([e29e42c](https://github.com/redis/go-redis/commit/e29e42cde2755ab910d04185025dc43ce6f59c65))



## v9 2023-01-30

### Added

- Added support for [RESP3](https://github.com/antirez/RESP3/blob/master/spec.md) protocol. It was
  contributed by @monkey92t who has done the majority of work in this release.
- Added `ContextTimeoutEnabled` option that controls whether the client respects context timeouts
  and deadlines. See
  [Redis Timeouts](https://redis.uptrace.dev/guide/go-redis-debugging.html#timeouts) for details.
- Added `ParseClusterURL` to parse URLs into `ClusterOptions`, for example,
  `redis://user:password@localhost:6789?dial_timeout=3&read_timeout=6s&addr=localhost:6790&addr=localhost:6791`.
- Added metrics instrumentation using `redisotel.IstrumentMetrics`. See
  [documentation](https://redis.uptrace.dev/guide/go-redis-monitoring.html)
- Added `redis.HasErrorPrefix` to help working with errors.

### Changed

- Removed asynchronous cancellation based on the context timeout. It was racy in v8 and is
  completely gone in v9.
- Reworked hook interface and added `DialHook`.
- Replaced `redisotel.NewTracingHook` with `redisotel.InstrumentTracing`. See
  [example](example/otel) and
  [documentation](https://redis.uptrace.dev/guide/go-redis-monitoring.html).
- Replaced `*redis.Z` with `redis.Z` since it is small enough to be passed as value without making
  an allocation.
- Renamed the option `MaxConnAge` to `ConnMaxLifetime`.
- Renamed the option `IdleTimeout` to `ConnMaxIdleTime`.
- Removed connection reaper in favor of `MaxIdleConns`.
- Removed `WithContext` since `context.Context` can be passed directly as an arg.
- Removed `Pipeline.Close` since there is no real need to explicitly manage pipeline resources and
  it can be safely reused via `sync.Pool` etc. `Pipeline.Discard` is still available if you want to
  reset commands for some reason.
- Changed Pipelines to not be thread-safe any more.

### Fixed

- Improved and fixed pipeline retries.
- As usually, added support for more commands and fixed some bugs.
