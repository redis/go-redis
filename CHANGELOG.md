# [9.0.0-rc.2](https://github.com/go-redis/redis/compare/v9.0.0-rc.1...v9.0.0-rc.2) (2022-11-26)


### Bug Fixes

* capture error correctly in withConn ([d1bfaba](https://github.com/go-redis/redis/commit/d1bfaba549fe380d269c26cea0a0183ed1520a85))
* fixes ring.SetAddrs and rebalance race  ([#2283](https://github.com/go-redis/redis/issues/2283)) ([d83436b](https://github.com/go-redis/redis/commit/d83436b321cd9ed52ba33c3edbe8f63bb0444c59))
* read in route_randomly query param correctly ([f236053](https://github.com/go-redis/redis/commit/f236053735d10aec5e6e31fc3ced1b2e53292554))
* reduce `SetAddrs` shards lock contention ([6c05a9f](https://github.com/go-redis/redis/commit/6c05a9f6b17f8e32593d3f7d594f82ba3dbcafb1)), closes [/github.com/go-redis/redis/pull/2190#discussion_r953040289](https://github.com//github.com/go-redis/redis/pull/2190/issues/discussion_r953040289) [#2077](https://github.com/go-redis/redis/issues/2077)
* wrap cmds in Conn.TxPipeline ([5053db2](https://github.com/go-redis/redis/commit/5053db2f9c8b3ca25f497a75f70012c7ad6cd775))


### Features

* add HasErrorPrefix ([d3d8002](https://github.com/go-redis/redis/commit/d3d8002e894a1eab5bab2c9fff13439527e330d8))
* add support for SINTERCARD command ([bc51c61](https://github.com/go-redis/redis/commit/bc51c61a458d1bc4fb4424c7c3e912325ef980cc))



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

### Fixed

- Improved and fixed pipeline retries.
- As usual, added more commands and fixed some bugs.
