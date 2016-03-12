all: testdeps
	go test ./... -test.v -test.cpu=1,2,4
	go test ./... -test.v -test.short -test.race

testdeps: testdata/redis/src/redis-server

.PHONY: all test testdeps

testdata/redis:
	mkdir -p $@
	wget -qO- https://github.com/antirez/redis/archive/unstable.tar.gz | tar xvz --strip-components=1 -C $@

testdata/redis/src/redis-server: testdata/redis
	cd $< && make all
