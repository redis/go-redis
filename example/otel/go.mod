module github.com/go-redis/redis/example/otel

go 1.14

require (
	github.com/go-redis/redis/v8 v8.11.2
	github.com/go-redis/redis/v8/extra/redisotel v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.0.0-RC2
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.0.0-RC2
	go.opentelemetry.io/otel/sdk v1.0.0-RC2
	golang.org/x/sys v0.0.0-20210809222454-d867a43fc93e // indirect
)

replace github.com/go-redis/redis/v8 => ../../

replace github.com/go-redis/redis/v8/extra/redisotel => ../../extra/redisotel
