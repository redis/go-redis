# [9.0.0-rc.4](https://github.com/redis/go-redis/compare/v9.0.0-rc.3...v9.0.0-rc.4) (2023-01-24)



# [9.0.0-rc.3](https://github.com/redis/go-redis/compare/v9.0.0-rc.2...v9.0.0-rc.3) (2023-01-24)


### Bug Fixes

* 386 platform test ([701b1d0](https://github.com/redis/go-redis/commit/701b1d0a8bc497c8dc55fb61bda05afde2dd073b))
* change serialize key "key" to "redis" ([913936b](https://github.com/redis/go-redis/commit/913936b4cd9ae131e4671d0960bf1f9e46e6b171))
* fix the withHook func ([0ed4a44](https://github.com/redis/go-redis/commit/0ed4a4420fddcbe897b3884ef637ece53ccc55b8))
* read cursor as uint64 ([b88bd93](https://github.com/redis/go-redis/commit/b88bd93662f55ff2d0b2353f5f79e7065464f982))
* **redisotel:** correct metrics.DialHook attrs ([#2331](https://github.com/redis/go-redis/issues/2331)) ([7c4b924](https://github.com/redis/go-redis/commit/7c4b92435024eef4429a30146fad28ec98085c5b))
* remove comment ([4ce9046](https://github.com/redis/go-redis/commit/4ce90461a5572395f0bffcf1e0eb5f17ae31ce11))
* remove mutex from pipeline ([6525bbb](https://github.com/redis/go-redis/commit/6525bbbaa157eaea40e363c462057a3ad29536a9))
* tags for structToMap "json" -> "key" ([07e15d2](https://github.com/redis/go-redis/commit/07e15d2876ccc88afcd0f344a3eed6a050ff1921))
* test code ([1fdcbf8](https://github.com/redis/go-redis/commit/1fdcbf86bbb390e4e689a35a391a4a4b3917216d))


### Features

* add ClientName option ([a872c35](https://github.com/redis/go-redis/commit/a872c35b1a9cbd19904010c105281ad15ab687ab))
* add SORT_RO command ([ca063fd](https://github.com/redis/go-redis/commit/ca063fd0adf0974504f4e9d7352e1b4d7b14cb61))
* add zintercard cmd ([bb65dcd](https://github.com/redis/go-redis/commit/bb65dcdf0903459ed341c87de34ad689632dceff))
* appendArgs adds to read the structure field and supplements the test ([0064199](https://github.com/redis/go-redis/commit/0064199323e408f0dafcd033460acb94a9ad9f4f))
* enable struct on HSet ([bf334e7](https://github.com/redis/go-redis/commit/bf334e773819574a898717f5a709e15cecaa43ff))
* hook mode is changed to FIFO ([97697f4](https://github.com/redis/go-redis/commit/97697f488fe5179542d07af72e031939fd854a99))
* **redisotel:** add code attributes ([3892986](https://github.com/redis/go-redis/commit/3892986f01959e1e71aee8710d9719400e0b1205))
* **scan:** add Scanner interface ([#2317](https://github.com/redis/go-redis/issues/2317)) ([a4336cb](https://github.com/redis/go-redis/commit/a4336cbd43a1e620cb8967bca27a678b9445bef8))



## v9 UNRELEASED

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
