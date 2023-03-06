module github.com/redis/go-redis/example/otel

go 1.18

replace github.com/redis/go-redis/v9 => ../..

replace github.com/redis/go-redis/extra/redisotel/v9 => ../../extra/redisotel

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../../extra/rediscmd

require (
	github.com/redis/go-redis/extra/redisotel/v9 v9.0.2
	github.com/redis/go-redis/v9 v9.0.2
	github.com/uptrace/uptrace-go v1.12.0
	go.opentelemetry.io/otel v1.12.0
)

require (
	github.com/cenkalti/backoff/v4 v4.2.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.0 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.0.2 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.38.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.12.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.35.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.35.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.12.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.12.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.12.0 // indirect
	go.opentelemetry.io/otel/metric v0.35.0 // indirect
	go.opentelemetry.io/otel/sdk v1.12.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.12.0 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230131230820-1c016267d619 // indirect
	google.golang.org/grpc v1.52.3 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)
