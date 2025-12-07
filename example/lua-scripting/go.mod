module github.com/redis/go-redis/example/lua-scripting

go 1.21

replace github.com/redis/go-redis/v9 => ../..

require github.com/redis/go-redis/v9 v9.18.0-beta.1

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	go.uber.org/atomic v1.11.0 // indirect
)
