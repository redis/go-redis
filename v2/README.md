Redis client for Golang [![Build Status](https://travis-ci.org/vmihailenco/redis.png?branch=master)](https://travis-ci.org/vmihailenco/redis)
=======================

Supports:

- Redis 2.8 commands except QUIT, MONITOR, SLOWLOG and SYNC.
- Pub/sub.
- Transactions.
- Pipelining.
- Connection pool.
- TLS connections.
- Thread safety.
- Timeouts.

API docs: http://godoc.org/github.com/vmihailenco/redis/v2.
Examples: http://godoc.org/github.com/vmihailenco/redis/v2#pkg-examples.

Installation
------------

Install:

    go get github.com/vmihailenco/redis/v2

Upgrading from previous version
-------------------------------

Type system should catch most changes. But you have to manually change `SetEx`, `PSetEx`, `Expire` and `PExpire` to use `time.Duration` instead of `int64`.

Look and feel
-------------

Some corner cases:

    SORT list LIMIT 0 2 ASC
    client.Sort("list", redis.Sort{Offset: 0, Count: 2, Order: "ASC"})

    ZRANGEBYSCORE zset -inf +inf WITHSCORES LIMIT 0 2
    client.ZRangeByScoreWithScores("zset", redis.ZRangeByScore{
        Min: "-inf",
        Max: "+inf",
        Offset: 0,
        Count: 2,
    })

    ZINTERSTORE out 2 zset1 zset2 WEIGHTS 2 3 AGGREGATE SUM
    client.ZInterStore("out", redis.ZStore{Weights: []int64{2, 3}}, "zset1", "zset2")

    EVAL "return {KEYS[1],ARGV[1]}" 1 "key" "hello"
    client.Eval("return {KEYS[1],ARGV[1]}", []string{"key"}, []string{"hello"})
