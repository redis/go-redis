module github.com/redis/go-redis/extra/redisprometheus/v9

go 1.26.0

replace github.com/redis/go-redis/v9 => ../..

require (
	github.com/prometheus/client_golang v1.24.1
	github.com/redis/go-redis/v9 v9.23.0-beta.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

retract (
	v9.7.2 // This version was accidentally released. Please use version 9.7.3 instead.
	v9.5.3 // This version was accidentally released. Please use version 9.6.0 instead.
)
