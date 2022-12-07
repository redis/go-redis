module github.com/go-redis/redis/example/del-keys-without-ttl

go 1.14

replace github.com/go-redis/redis/v9 => ../..

require (
	github.com/go-redis/redis/v9 v9.0.0-rc.2
	go.uber.org/zap v1.24.0
)
