module github.com/redis/go-redis/v9

go 1.18

require (
	github.com/bsm/ginkgo/v2 v2.12.0
	github.com/bsm/gomega v1.27.10
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f
	github.com/google/uuid v1.6.0
)

retract (
	v9.5.4 // This version was accidentally released. Please use version 9.6.0 instead.
	v9.5.3 // This version was accidentally released. Please use version 9.6.0 instead.
)
