module github.com/redis/go-redis/example/del-keys-without-ttl

go 1.14

replace github.com/redis/go-redis/v9 => ../..

require (
	github.com/redis/go-redis/v9 v9.0.0-rc.4
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	go.uber.org/zap v1.24.0
)
