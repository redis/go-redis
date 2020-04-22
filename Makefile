all: testdeps
	go test ./...
	go test ./... -short -race
	go test ./... -run=NONE -bench=. -benchmem
	env GOOS=linux GOARCH=386 go test ./...
	golangci-lint run

testdeps: testdata/redis/src/redis-server

bench: testdeps
	go test ./... -test.run=NONE -test.bench=. -test.benchmem

.PHONY: all test testdeps bench

testdata/redis: testdata/redisbloom
	mkdir -p $@
	wget -qO- http://download.redis.io/releases/redis-5.0.7.tar.gz | tar xvz --strip-components=1 -C $@

testdata/redisbloom:
	mkdir -p $@
	wget -O $@/redisbloom.zip https://codeload.github.com/RedisBloom/RedisBloom/zip/master
	unzip $@/redisbloom.zip -d $@
	cd $@/RedisBloom-master && make
	cp -rf $@/RedisBloom-master/redisbloom.so $@

testdata/redis/src/redis-server: testdata/redis
	cd $< && make all
