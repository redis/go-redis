package redis_test

import (
	"context"
	"fmt"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

func libCode(libName string) string {
	return fmt.Sprintf("#!js api_version=1.0 name=%s\n redis.registerFunction('foo', ()=>{{return 'bar'}})", libName)
}

func libCodeWithConfig(libName string) string {
	lib := `#!js api_version=1.0 name=%s

	var last_update_field_name = "__last_update__"
	
	if (redis.config.last_update_field_name !== undefined) {
		if (typeof redis.config.last_update_field_name != 'string') {
			throw "last_update_field_name must be a string";
		}
		last_update_field_name = redis.config.last_update_field_name
	}
	
	redis.registerFunction("hset", function(client, key, field, val){
		// get the current time in ms
		var curr_time = client.call("time")[0];
		return client.call('hset', key, field, val, last_update_field_name, curr_time);
	});`
	return fmt.Sprintf(lib, libName)
}

// TODO: Drop Gears
var _ = Describe("RedisGears commands", Label("gears"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379"})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		client.TFunctionDelete(ctx, "lib1")
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should TFunctionLoad, TFunctionLoadArgs and TFunctionDelete ", Label("gears", "tfunctionload"), func() {
		SkipAfterRedisVersion(7.4, "gears are not working in later versions")
		resultAdd, err := client.TFunctionLoad(ctx, libCode("lib1")).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("OK"))
		opt := &redis.TFunctionLoadOptions{Replace: true, Config: `{"last_update_field_name":"last_update"}`}
		resultAdd, err = client.TFunctionLoadArgs(ctx, libCodeWithConfig("lib1"), opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("OK"))
	})
	It("should TFunctionList", Label("gears", "tfunctionlist"), func() {
		SkipAfterRedisVersion(7.4, "gears are not working in later versions")
		resultAdd, err := client.TFunctionLoad(ctx, libCode("lib1")).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("OK"))
		resultList, err := client.TFunctionList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultList[0]["engine"]).To(BeEquivalentTo("js"))
		opt := &redis.TFunctionListOptions{Withcode: true, Verbose: 2}
		resultListArgs, err := client.TFunctionListArgs(ctx, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultListArgs[0]["code"]).NotTo(BeEquivalentTo(""))
	})

	It("should TFCall", Label("gears", "tfcall"), func() {
		SkipAfterRedisVersion(7.4, "gears are not working in later versions")
		var resultAdd interface{}
		resultAdd, err := client.TFunctionLoad(ctx, libCode("lib1")).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("OK"))
		resultAdd, err = client.TFCall(ctx, "lib1", "foo", 0).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("bar"))
	})

	It("should TFCallArgs", Label("gears", "tfcallargs"), func() {
		SkipAfterRedisVersion(7.4, "gears are not working in later versions")
		var resultAdd interface{}
		resultAdd, err := client.TFunctionLoad(ctx, libCode("lib1")).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("OK"))
		opt := &redis.TFCallOptions{Arguments: []string{"foo", "bar"}}
		resultAdd, err = client.TFCallArgs(ctx, "lib1", "foo", 0, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("bar"))
	})

	It("should TFCallASYNC", Label("gears", "TFCallASYNC"), func() {
		SkipAfterRedisVersion(7.4, "gears are not working in later versions")
		var resultAdd interface{}
		resultAdd, err := client.TFunctionLoad(ctx, libCode("lib1")).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("OK"))
		resultAdd, err = client.TFCallASYNC(ctx, "lib1", "foo", 0).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("bar"))
	})

	It("should TFCallASYNCArgs", Label("gears", "TFCallASYNCargs"), func() {
		SkipAfterRedisVersion(7.4, "gears are not working in later versions")
		var resultAdd interface{}
		resultAdd, err := client.TFunctionLoad(ctx, libCode("lib1")).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("OK"))
		opt := &redis.TFCallOptions{Arguments: []string{"foo", "bar"}}
		resultAdd, err = client.TFCallASYNCArgs(ctx, "lib1", "foo", 0, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo("bar"))
	})
})
