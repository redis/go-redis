# [9.0.0-rc.1](https://github.com/go-redis/redis/compare/v9.0.0-beta.3...v9.0.0-rc.1) (2022-10-14)


### Bug Fixes

* late binding for dial hook ([2ec03d9](https://github.com/go-redis/redis/commit/2ec03d9b370ea6f42b4ce4054121fccf31649b00))
* reset pubsub state when unsubscribing from all channels ([331e40d](https://github.com/go-redis/redis/commit/331e40dc6ccde2297df3e1b3c6b747dc4c6cc83a))
* retry dial errors from pipelines ([4bb485d](https://github.com/go-redis/redis/commit/4bb485d04438669b23f93c64c69e5391f961b915))
* use all provided sections ([28028b3](https://github.com/go-redis/redis/commit/28028b330fc11ea3f23fecd39a85828c4aa91a3e))


### Features

* add ContextTimeoutEnabled to respect context timeouts and deadlines ([58f7149](https://github.com/go-redis/redis/commit/58f7149e3802de7b92e8b14516493ab05fc0bf2c))
* add OpenTelemetry metrics instrumentation ([0dff3d1](https://github.com/go-redis/redis/commit/0dff3d1461793059f6b0349d9138b7a0955c1248))



## v9 UNRELEASED

### Added

- Added support for [RESP3](https://github.com/antirez/RESP3/blob/master/spec.md) protocol.
  Contributed by @monkey92t who has done a lot of work recently.
- Added `ContextTimeoutEnabled` option that controls whether the client respects context timeouts
  and deadlines. See
  [Redis Timeouts](https://redis.uptrace.dev/guide/go-redis-debugging.html#timeouts) for details.
- Added `ParseClusterURL` to parse URLs into `ClusterOptions`, for example,
  `redis://user:password@localhost:6789?dial_timeout=3&read_timeout=6s&addr=localhost:6790&addr=localhost:6791`.
- Added metrics instrumentation using `redisotel.IstrumentMetrics`. See
  [documentation](https://redis.uptrace.dev/guide/go-redis-monitoring.html)

### Changed

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

### Fixed

- Improved and fixed pipeline retries.
- As usual, added more commands and fixed some bugs.
