# [9.0.0-beta.2](https://github.com/go-redis/redis/compare/v9.0.0-beta.1...v9.0.0-beta.2) (2022-07-28)


### Bug Fixes

* [#2114](https://github.com/go-redis/redis/issues/2114) for redis-server not support Hello ([b6d2a92](https://github.com/go-redis/redis/commit/b6d2a925297e3e516eb5c76c114c1c9fcd5b68c5))
* additional node failures in clustered pipelined reads ([03376a5](https://github.com/go-redis/redis/commit/03376a5d9c7dfd7197b14ce13b24a0431a07a663))
* disregard failed pings in updateLatency() for cluster nodes ([64f972f](https://github.com/go-redis/redis/commit/64f972fbeae401e52a2c066a0e1c922af617e15c))
* don't panic when test cannot start ([9e16c79](https://github.com/go-redis/redis/commit/9e16c79951e7769621b7320f1ecdf04baf539b82))
* handle panic in ringShards Hash function when Ring got closed ([a80b84f](https://github.com/go-redis/redis/commit/a80b84f01f9fc0d3e6f08445ba21f7e07880775e)), closes [#2126](https://github.com/go-redis/redis/issues/2126)
* ignore Nil error when reading EntriesRead ([89d6dfe](https://github.com/go-redis/redis/commit/89d6dfe09a88321d445858c1c5b24d2757b95a3e))
* log errors from cmdsInfoCache ([fa4d1ea](https://github.com/go-redis/redis/commit/fa4d1ea8398cd729ad5cbaaff88e4b8805393945))
* provide a signal channel to end heartbeat goroutine ([f032c12](https://github.com/go-redis/redis/commit/f032c126db3e2c1a239ce1790b0ab81994df75cf))
* remove conn reaper from the pool and uptrace option names ([f6a8adc](https://github.com/go-redis/redis/commit/f6a8adc50cdaec30527f50d06468f9176ee674fe))
* replace heartbeat signal channel with context.WithCancel ([20d0ca2](https://github.com/go-redis/redis/commit/20d0ca235efff48ad48cc05b98790b825d4ba979))



# [9.0.0-beta.1](https://github.com/go-redis/redis/compare/v8.11.5...v9.0.0-beta.1) (2022-06-04)

### Bug Fixes

- **#1943:** xInfoConsumer.Idle should be time.Duration instead of int64
  ([#2052](https://github.com/go-redis/redis/issues/2052))
  ([997ab5e](https://github.com/go-redis/redis/commit/997ab5e7e3ddf53837917013a4babbded73e944f)),
  closes [#1943](https://github.com/go-redis/redis/issues/1943)
- add XInfoConsumers test
  ([6f1a1ac](https://github.com/go-redis/redis/commit/6f1a1ac284ea3f683eeb3b06a59969e8424b6376))
- fix tests
  ([3a722be](https://github.com/go-redis/redis/commit/3a722be81180e4d2a9cf0a29dc9a1ee1421f5859))
- remove test(XInfoConsumer.idle), not a stable return value when tested.
  ([f5fbb36](https://github.com/go-redis/redis/commit/f5fbb367e7d9dfd7f391fc535a7387002232fa8a))
- update ChannelWithSubscriptions to accept options
  ([c98c5f0](https://github.com/go-redis/redis/commit/c98c5f0eebf8d254307183c2ce702a48256b718d))
- update COMMAND parser for Redis 7
  ([b0bb514](https://github.com/go-redis/redis/commit/b0bb514059249e01ed7328c9094e5b8a439dfb12))
- use redis over ssh channel([#2057](https://github.com/go-redis/redis/issues/2057))
  ([#2060](https://github.com/go-redis/redis/issues/2060))
  ([3961b95](https://github.com/go-redis/redis/commit/3961b9577f622a3079fe74f8fc8da12ba67a77ff))

### Features

- add ClientUnpause
  ([91171f5](https://github.com/go-redis/redis/commit/91171f5e19a261dc4cfbf8706626d461b6ba03e4))
- add NewXPendingResult for unit testing XPending
  ([#2066](https://github.com/go-redis/redis/issues/2066))
  ([b7fd09e](https://github.com/go-redis/redis/commit/b7fd09e59479bc6ed5b3b13c4645a3620fd448a3))
- add WriteArg and Scan net.IP([#2062](https://github.com/go-redis/redis/issues/2062))
  ([7d5167e](https://github.com/go-redis/redis/commit/7d5167e8624ac1515e146ed183becb97dadb3d1a))
- **pool:** add check for badConnection
  ([a8a7665](https://github.com/go-redis/redis/commit/a8a7665ddf8cc657c5226b1826a8ee83dab4b8c1)),
  closes [#2053](https://github.com/go-redis/redis/issues/2053)
- provide a username and password callback method, so that the plaintext username and password will
  not be stored in the memory, and the username and password will only be generated once when the
  CredentialsProvider is called. After the method is executed, the username and password strings on
  the stack will be released. ([#2097](https://github.com/go-redis/redis/issues/2097))
  ([56a3dbc](https://github.com/go-redis/redis/commit/56a3dbc7b656525eb88e0735e239d56e04a23bee))
- upgrade to Redis 7
  ([d09c27e](https://github.com/go-redis/redis/commit/d09c27e6046129fd27b1d275e5a13a477bd7f778))

## v9 UNRELEASED

- Added support for [RESP3](https://github.com/antirez/RESP3/blob/master/spec.md) protocol.
- Removed `Pipeline.Close` since there is no real need to explicitly manage pipeline resources.
  `Pipeline.Discard` is still available if you want to reset commands for some reason.
- Replaced `*redis.Z` with `redis.Z` since it is small enough to be passed as value.
- Renamed `MaxConnAge` to `ConnMaxLifetime`.
- Renamed `IdleTimeout` to `ConnMaxIdleTime`.
- Removed connection reaper in favor of `MaxIdleConns`.
- Removed `WithContext`.
