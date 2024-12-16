module github.com/redis/go-redis/extra/redisotel/v9

go 1.19

replace github.com/redis/go-redis/v9 => ../..

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/redis/go-redis/extra/rediscmd/v9 v9.6.2
	github.com/redis/go-redis/v9 v9.6.2
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/otel v1.23.0
	go.opentelemetry.io/otel/metric v1.23.0
	go.opentelemetry.io/otel/sdk v1.23.0
	go.opentelemetry.io/otel/sdk/metric v1.23.0
	go.opentelemetry.io/otel/trace v1.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v9.5.3 // This version was accidentally released.
