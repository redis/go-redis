## v9 UNRELEASED

- Added support for [RESP3](https://github.com/antirez/RESP3/blob/master/spec.md) protocol.
- Removed `Pipeline.Close` since there is no real need to explicitly manage pipeline resources.
  `Pipeline.Discard` is still available if you want to reset commands for some reason.
- Replaced `*redis.Z` with `redis.Z` since it is small enough to be passed as value.
