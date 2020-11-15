all: testdeps
	go test ./... -coverprofile=coverage-1.txt -covermode=atomic
	go test ./... -short -race -coverprofile=coverage-2.txt -covermode=atomic
	go test ./... -run=NONE -bench=. -benchmem -coverprofile=coverage-3.txt -covermode=atomic
	env GOOS=linux GOARCH=386 go test ./... -coverprofile=coverage-4.txt -covermode=atomic
	go vet
	golangci-lint run

testdeps: testdata/redis/src/redis-server

bench: testdeps
	go test ./... -test.run=NONE -test.bench=. -test.benchmem -coverprofile=coverage.txt -covermode=atomic

.PHONY: all test testdeps bench

testdata/redis:
	mkdir -p $@
	wget -qO- http://download.redis.io/redis-stable.tar.gz | tar xvz --strip-components=1 -C $@

testdata/redis/src/redis-server: testdata/redis
	cd $< && make all
