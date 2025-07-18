module github.com/redis/go-redis/example/scan-struct

go 1.18

replace github.com/redis/go-redis/v9 => ../..

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/redis/go-redis/v9 v9.11.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
