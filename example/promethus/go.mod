module github.com/go-redis/redis/example/promethus

go 1.15

replace github.com/go-redis/redis/v8 => ../..

replace github.com/go-redis/redis/extra/redisprom/v8 => ../../extra/redisprom

require (
	github.com/go-redis/redis/extra/redisprom/v8 v8.0.0-00010101000000-000000000000
	github.com/go-redis/redis/v8 v8.11.3
	github.com/prometheus/client_golang v1.11.0
	golang.org/x/sys v0.0.0-20210809222454-d867a43fc93e // indirect
)
