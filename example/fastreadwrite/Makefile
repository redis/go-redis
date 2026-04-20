# Makefile for the go-redis fastreadwrite benchmark.
#
# Typical flow:
#     make redis-up        # start a Redis container on 127.0.0.1:6379
#     make bench           # run the benchmark against it (host OS)
#     make bench-linux     # run the benchmark inside a Linux container
#     make redis-down      # tear down the Redis container

REDIS_IMAGE ?= redis:7-alpine
REDIS_NAME  ?= fastreadwrite-redis
REDIS_PORT  ?= 6379
REDIS_HOST  ?= 127.0.0.1

# Redis server tuning
REDIS_IO_THREADS ?= 8

# Benchmark parameters
CHUNK_MB     ?= 4.0
NUM_KEYS     ?= 500
NUM_WORKERS  ?= 8
MODE         ?= both
READ_BUF_KB  ?= 1024
WRITE_BUF_KB ?= 1024
RCVBUF_KB    ?= 4096
SNDBUF_KB    ?= 4096
POOL_SIZE    ?= 0
RUNS         ?= 5
MIN_IDLE     ?= 0
NO_GC        ?= 0
HUGE_PAGES   ?= 1
NO_DEADLINES ?= 0
EXTRA_ARGS   ?=

# Optional CPU pinning for the bench process, e.g. TASKSET="taskset -c 0-7".
# Only honored by bench-linux (applied inside the container).
TASKSET      ?=

# Compose per-flag args so empty vars don't inject bogus flags.
OPT_ARGS =
ifneq ($(strip $(MIN_IDLE)),0)
OPT_ARGS += --min-idle $(MIN_IDLE)
endif
ifneq ($(strip $(NO_GC)),0)
OPT_ARGS += --no-gc
endif
ifeq ($(strip $(HUGE_PAGES)),0)
OPT_ARGS += --huge-pages=false
endif
ifneq ($(strip $(NO_DEADLINES)),0)
OPT_ARGS += --no-deadlines
endif

BENCH_ARGS = \
	--host $(REDIS_HOST) --port $(REDIS_PORT) \
	--chunk-mb $(CHUNK_MB) --num-keys $(NUM_KEYS) \
	--num-workers $(NUM_WORKERS) --mode $(MODE) \
	--read-buf-kb $(READ_BUF_KB) --write-buf-kb $(WRITE_BUF_KB) \
	--sock-rcvbuf-kb $(RCVBUF_KB) --sock-sndbuf-kb $(SNDBUF_KB) \
	--pool-size $(POOL_SIZE) --runs $(RUNS) \
	$(OPT_ARGS) $(EXTRA_ARGS)

.PHONY: build vet run bench bench-regular bench-linux \
        redis-up redis-down redis-logs clean help tune-sysctl-info

help:
	@echo "Targets:"
	@echo "  redis-up          Start a Redis container (port $(REDIS_PORT), io-threads $(REDIS_IO_THREADS))"
	@echo "  redis-down        Stop and remove the Redis container"
	@echo "  redis-logs        Tail container logs"
	@echo "  build             go build"
	@echo "  vet               go vet"
	@echo "  bench             Run benchmark on the host against $(REDIS_HOST):$(REDIS_PORT)"
	@echo "  bench-regular     Also include regular (non-zero-copy) Set/Get"
	@echo "  bench-linux       Run benchmark from a Linux Go container (shares Redis netns)"
	@echo "  tune-sysctl-info  Print recommended host sysctl values for max throughput"
	@echo ""
	@echo "Variables (override on the command line):"
	@echo "  CHUNK_MB=$(CHUNK_MB) NUM_KEYS=$(NUM_KEYS) NUM_WORKERS=$(NUM_WORKERS) MODE=$(MODE)"
	@echo "  READ_BUF_KB=$(READ_BUF_KB) WRITE_BUF_KB=$(WRITE_BUF_KB)"
	@echo "  RCVBUF_KB=$(RCVBUF_KB) SNDBUF_KB=$(SNDBUF_KB) POOL_SIZE=$(POOL_SIZE)"
	@echo "  RUNS=$(RUNS) MIN_IDLE=$(MIN_IDLE) NO_GC=$(NO_GC) HUGE_PAGES=$(HUGE_PAGES) NO_DEADLINES=$(NO_DEADLINES)"
	@echo "  REDIS_IO_THREADS=$(REDIS_IO_THREADS) TASKSET=\"$(TASKSET)\" EXTRA_ARGS=\"$(EXTRA_ARGS)\""
	@echo ""
	@echo "Examples:"
	@echo "  make bench NUM_KEYS=1000 CHUNK_MB=1.0 NUM_WORKERS=16"
	@echo "  make bench-linux TASKSET=\"taskset -c 0-7\" RUNS=10 NO_GC=1"

build:
	go build ./...

vet:
	go vet ./...

run: build
	go run . $(BENCH_ARGS)

bench: run

bench-regular:
	go run . $(BENCH_ARGS) --regular

redis-up:
	@if [ -z "$$(docker ps -aq -f name=^/$(REDIS_NAME)$$)" ]; then \
		echo "Starting $(REDIS_NAME) ($(REDIS_IMAGE), io-threads=$(REDIS_IO_THREADS))..."; \
		docker run -d --name $(REDIS_NAME) \
			-p $(REDIS_PORT):6379 \
			$(REDIS_IMAGE) \
			redis-server --save "" --appendonly no \
				--io-threads $(REDIS_IO_THREADS) --io-threads-do-reads yes \
				--maxmemory-policy noeviction \
				> /dev/null; \
	else \
		echo "Container $(REDIS_NAME) exists; (re)starting..."; \
		docker start $(REDIS_NAME) > /dev/null; \
	fi
	@echo -n "Waiting for Redis... "
	@for i in $$(seq 1 30); do \
		if docker exec $(REDIS_NAME) redis-cli ping 2>/dev/null | grep -q PONG; then \
			echo "ready."; exit 0; \
		fi; sleep 0.5; \
	done; echo "timeout"; exit 1

redis-down:
	-@docker rm -f $(REDIS_NAME) > /dev/null 2>&1 || true
	@echo "Stopped $(REDIS_NAME)"

redis-logs:
	docker logs -f $(REDIS_NAME)

# Run the benchmark from inside a Linux golang container that shares the Redis
# container's network namespace. This exercises the TCP_QUICKACK / SO_*BUF
# tuning code paths on a real Linux kernel and works cross-platform (including
# Docker Desktop on macOS, where --network host would not reach the host).
bench-linux: redis-up
	docker run --rm \
		--network container:$(REDIS_NAME) \
		-v $(CURDIR)/../..:/src \
		-w /src/example/fastreadwrite \
		-e GOFLAGS=-mod=mod \
		-e GOCACHE=/tmp/gocache \
		-e GOMODCACHE=/tmp/gomodcache \
		golang:1.24 \
		sh -c '$(TASKSET) go run . --host 127.0.0.1 --port 6379 \
			--chunk-mb $(CHUNK_MB) --num-keys $(NUM_KEYS) \
			--num-workers $(NUM_WORKERS) --mode $(MODE) \
			--read-buf-kb $(READ_BUF_KB) --write-buf-kb $(WRITE_BUF_KB) \
			--sock-rcvbuf-kb $(RCVBUF_KB) --sock-sndbuf-kb $(SNDBUF_KB) \
			--pool-size $(POOL_SIZE) --runs $(RUNS) \
			$(OPT_ARGS) $(EXTRA_ARGS)'

# Print the host sysctl values that affect TCP bulk throughput. These are
# informational only; the container/VM kernel may have different values and
# some are only changeable by the host administrator.
tune-sysctl-info:
	@echo "Recommended host sysctl values for large-buffer TCP on loopback:"
	@echo "  net.core.rmem_max               = 134217728   # 128 MiB (caps SO_RCVBUF)"
	@echo "  net.core.wmem_max               = 134217728   # 128 MiB (caps SO_SNDBUF)"
	@echo "  net.ipv4.tcp_rmem               = 4096 262144 134217728"
	@echo "  net.ipv4.tcp_wmem               = 4096 262144 134217728"
	@echo "  net.core.netdev_max_backlog     = 250000"
	@echo "  net.ipv4.tcp_congestion_control = bbr         # or cubic"
	@echo "  vm.nr_hugepages                 = 512         # enables THP-backed buffers"
	@echo ""
	@echo "Apply on Linux host:  sudo sysctl -w <key>=<value>"
	@echo "Current host values (if Linux):"
	@for k in net.core.rmem_max net.core.wmem_max net.ipv4.tcp_rmem net.ipv4.tcp_wmem \
	          net.core.netdev_max_backlog net.ipv4.tcp_congestion_control; do \
		v=$$(sysctl -n $$k 2>/dev/null || echo "n/a"); \
		printf "  %-35s = %s\n" $$k "$$v"; \
	done

clean:
	go clean
