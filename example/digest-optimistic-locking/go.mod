module github.com/redis/go-redis/example/digest-optimistic-locking

go 1.24

replace github.com/redis/go-redis/v9 => ../..

require github.com/redis/go-redis/v9 v9.18.0

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)
