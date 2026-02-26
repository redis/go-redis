module cluster-state-machine

go 1.25.3

replace github.com/redis/go-redis/v9 => ../..

require github.com/redis/go-redis/v9 v9.0.0-00010101000000-000000000000

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
