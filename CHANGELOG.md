# [9.0.0-beta.1](https://github.com/go-redis/redis/compare/v8.11.5...v9.0.0-beta.1) (2022-06-04)


### Bug Fixes

* **#1943:** xInfoConsumer.Idle should be time.Duration instead of int64 ([#2052](https://github.com/go-redis/redis/issues/2052)) ([997ab5e](https://github.com/go-redis/redis/commit/997ab5e7e3ddf53837917013a4babbded73e944f)), closes [#1943](https://github.com/go-redis/redis/issues/1943)
* add XInfoConsumers test ([6f1a1ac](https://github.com/go-redis/redis/commit/6f1a1ac284ea3f683eeb3b06a59969e8424b6376))
* fix tests ([3a722be](https://github.com/go-redis/redis/commit/3a722be81180e4d2a9cf0a29dc9a1ee1421f5859))
* remove test(XInfoConsumer.idle), not a stable return value when tested. ([f5fbb36](https://github.com/go-redis/redis/commit/f5fbb367e7d9dfd7f391fc535a7387002232fa8a))
* update ChannelWithSubscriptions to accept options ([c98c5f0](https://github.com/go-redis/redis/commit/c98c5f0eebf8d254307183c2ce702a48256b718d))
* update COMMAND parser for Redis 7 ([b0bb514](https://github.com/go-redis/redis/commit/b0bb514059249e01ed7328c9094e5b8a439dfb12))
* use redis over ssh channel([#2057](https://github.com/go-redis/redis/issues/2057)) ([#2060](https://github.com/go-redis/redis/issues/2060)) ([3961b95](https://github.com/go-redis/redis/commit/3961b9577f622a3079fe74f8fc8da12ba67a77ff))


### Features

* add ClientUnpause ([91171f5](https://github.com/go-redis/redis/commit/91171f5e19a261dc4cfbf8706626d461b6ba03e4))
* add NewXPendingResult for unit testing XPending ([#2066](https://github.com/go-redis/redis/issues/2066)) ([b7fd09e](https://github.com/go-redis/redis/commit/b7fd09e59479bc6ed5b3b13c4645a3620fd448a3))
* add WriteArg and Scan net.IP([#2062](https://github.com/go-redis/redis/issues/2062)) ([7d5167e](https://github.com/go-redis/redis/commit/7d5167e8624ac1515e146ed183becb97dadb3d1a))
* **pool:** add check for badConnection ([a8a7665](https://github.com/go-redis/redis/commit/a8a7665ddf8cc657c5226b1826a8ee83dab4b8c1)), closes [#2053](https://github.com/go-redis/redis/issues/2053)
* provide a username and password callback method, so that the plaintext username and password will not be stored in the memory, and the username and password will only be generated once when the CredentialsProvider is called. After the method is executed, the username and password strings on the stack will be released. ([#2097](https://github.com/go-redis/redis/issues/2097)) ([56a3dbc](https://github.com/go-redis/redis/commit/56a3dbc7b656525eb88e0735e239d56e04a23bee))
* upgrade to Redis 7 ([d09c27e](https://github.com/go-redis/redis/commit/d09c27e6046129fd27b1d275e5a13a477bd7f778))



## v9 UNRELEASED

- Added support for [RESP3](https://github.com/antirez/RESP3/blob/master/spec.md) protocol.
- Removed `Pipeline.Close` since there is no real need to explicitly manage pipeline resources.
  `Pipeline.Discard` is still available if you want to reset commands for some reason.
- Replaced `*redis.Z` with `redis.Z` since it is small enough to be passed as value.
