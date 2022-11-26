module github.com/go-redis/redis/extra/redisprometheus/v9

go 1.17

replace github.com/go-redis/redis/v9 => ../..

require (
	github.com/go-redis/redis/v9 v9.0.0-rc.2
	github.com/prometheus/client_golang v1.12.2
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	golang.org/x/sys v0.2.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
)
