module github.com/redis/go-redis/example/del-keys-without-ttl

go 1.18

replace github.com/redis/go-redis/v9 => ../..

require (
	github.com/redis/go-redis/v9 v9.4.0
	go.uber.org/zap v1.24.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
)
