module github.com/redis/go-redis/extra/rediscmd/v9

go 1.19

replace github.com/redis/go-redis/v9 => ../..

require (
	github.com/bsm/ginkgo/v2 v2.12.0
	github.com/bsm/gomega v1.27.10
	github.com/redis/go-redis/v9 v9.9.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)

retract (
	v9.7.2 // This version was accidentally released.
	v9.5.3 // This version was accidentally released.
)
