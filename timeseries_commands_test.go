package redis_test

import (
	"context"
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("RedisTimeseries commands", Label("timeseries"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: rediStackAddr})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should TSCreate and TSCreateWithArgs", Label("timeseries", "tscreate", "tscreateWithArgs"), func() {
		result, err := client.TSCreate(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		// Test TSCreateWithArgs
		opt := &redis.TSOptions{Retention: 5}
		result, err = client.TSCreateWithArgs(ctx, "2", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		opt = &redis.TSOptions{Labels: map[string]string{"Redis": "Labs"}}
		result, err = client.TSCreateWithArgs(ctx, "3", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		opt = &redis.TSOptions{Labels: map[string]string{"Time": "Series"}, Retention: 20}
		result, err = client.TSCreateWithArgs(ctx, "4", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		resultInfo, err := client.TSInfo(ctx, "4").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["labels"].(map[interface{}]interface{})["Time"]).To(BeEquivalentTo("Series"))
		// Test chunk size
		opt = &redis.TSOptions{ChunkSize: 128}
		result, err = client.TSCreateWithArgs(ctx, "ts-cs-1", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		resultInfo, err = client.TSInfo(ctx, "ts-cs-1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["chunkSize"]).To(BeEquivalentTo(128))
		// Test duplicate policy
		duplicate_policies := []string{"BLOCK", "LAST", "FIRST", "MIN", "MAX"}
		for _, dup := range duplicate_policies {
			keyName := "ts-dup-" + dup
			opt = &redis.TSOptions{DuplicatePolicy: dup}
			result, err = client.TSCreateWithArgs(ctx, keyName, opt).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEquivalentTo("OK"))
			resultInfo, err = client.TSInfo(ctx, keyName).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.ToUpper(resultInfo["duplicatePolicy"].(string))).To(BeEquivalentTo(dup))

		}
	})
	It("should TSAdd and TSAddWithArgs", Label("timeseries", "tsadd", "tsaddWithArgs"), func() {
		result, err := client.TSAdd(ctx, "1", 1, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		// Test TSAddWithArgs
		opt := &redis.TSOptions{Retention: 10}
		result, err = client.TSAddWithArgs(ctx, "2", 2, 3, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(2))
		opt = &redis.TSOptions{Labels: map[string]string{"Redis": "Labs"}}
		result, err = client.TSAddWithArgs(ctx, "3", 3, 2, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(3))
		opt = &redis.TSOptions{Labels: map[string]string{"Redis": "Labs", "Time": "Series"}, Retention: 10}
		result, err = client.TSAddWithArgs(ctx, "4", 4, 2, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(4))
		resultInfo, err := client.TSInfo(ctx, "4").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["labels"].(map[interface{}]interface{})["Time"]).To(BeEquivalentTo("Series"))
		// Test chunk size
		opt = &redis.TSOptions{ChunkSize: 128}
		result, err = client.TSAddWithArgs(ctx, "ts-cs-1", 1, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		resultInfo, err = client.TSInfo(ctx, "ts-cs-1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["chunkSize"]).To(BeEquivalentTo(128))
		// Test duplicate policy
		// LAST
		opt = &redis.TSOptions{DuplicatePolicy: "LAST"}
		result, err = client.TSAddWithArgs(ctx, "tsal-1", 1, 5, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		result, err = client.TSAddWithArgs(ctx, "tsal-1", 1, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		resultGet, err := client.TSGet(ctx, "tsal-1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet.Value).To(BeEquivalentTo(10))
		// FIRST
		opt = &redis.TSOptions{DuplicatePolicy: "FIRST"}
		result, err = client.TSAddWithArgs(ctx, "tsaf-1", 1, 5, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		result, err = client.TSAddWithArgs(ctx, "tsaf-1", 1, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		resultGet, err = client.TSGet(ctx, "tsaf-1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet.Value).To(BeEquivalentTo(5))
		// MAX
		opt = &redis.TSOptions{DuplicatePolicy: "MAX"}
		result, err = client.TSAddWithArgs(ctx, "tsam-1", 1, 5, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		result, err = client.TSAddWithArgs(ctx, "tsam-1", 1, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		resultGet, err = client.TSGet(ctx, "tsam-1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet.Value).To(BeEquivalentTo(10))
		// MIN
		opt = &redis.TSOptions{DuplicatePolicy: "MIN"}
		result, err = client.TSAddWithArgs(ctx, "tsami-1", 1, 5, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		result, err = client.TSAddWithArgs(ctx, "tsami-1", 1, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo(1))
		resultGet, err = client.TSGet(ctx, "tsami-1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet.Value).To(BeEquivalentTo(5))
	})

	It("should TSAlter", Label("timeseries", "tsalter"), func() {
		result, err := client.TSCreate(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		resultInfo, err := client.TSInfo(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["retentionTime"]).To(BeEquivalentTo(0))

		opt := &redis.TSAlterOptions{Retention: 10}
		resultAlter, err := client.TSAlter(ctx, "1", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAlter).To(BeEquivalentTo("OK"))

		resultInfo, err = client.TSInfo(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["retentionTime"]).To(BeEquivalentTo(10))

		resultInfo, err = client.TSInfo(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["labels"]).To(BeEquivalentTo(map[interface{}]interface{}{}))

		opt = &redis.TSAlterOptions{Labels: map[string]string{"Time": "Series"}}
		resultAlter, err = client.TSAlter(ctx, "1", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAlter).To(BeEquivalentTo("OK"))

		resultInfo, err = client.TSInfo(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["labels"].(map[interface{}]interface{})["Time"]).To(BeEquivalentTo("Series"))
		Expect(resultInfo["retentionTime"]).To(BeEquivalentTo(10))
		Expect(resultInfo["duplicatePolicy"]).To(BeEquivalentTo(redis.Nil))
		opt = &redis.TSAlterOptions{DuplicatePolicy: "min"}
		resultAlter, err = client.TSAlter(ctx, "1", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAlter).To(BeEquivalentTo("OK"))

		resultInfo, err = client.TSInfo(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["duplicatePolicy"]).To(BeEquivalentTo("min"))
	})

	It("should TSCreateRule and TSDeleteRule", Label("timeseries", "tscreaterule", "tsdeleterule"), func() {
		result, err := client.TSCreate(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		result, err = client.TSCreate(ctx, "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		result, err = client.TSCreateRule(ctx, "1", "2", redis.Avg, 100).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo("OK"))
		for i := 0; i < 50; i++ {
			resultAdd, err := client.TSAdd(ctx, "1", 100+i*2, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resultAdd).To(BeEquivalentTo(100 + i*2))
			resultAdd, err = client.TSAdd(ctx, "1", 100+i*2+1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resultAdd).To(BeEquivalentTo(100 + i*2 + 1))

		}
		resultAdd, err := client.TSAdd(ctx, "1", 100*2, 1.5).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultAdd).To(BeEquivalentTo(100 * 2))
		resultGet, err := client.TSGet(ctx, "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet.Value).To(BeEquivalentTo(1.5))
		Expect(resultGet.Timestamp).To(BeEquivalentTo(100))

		resultDeleteRule, err := client.TSDeleteRule(ctx, "1", "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultDeleteRule).To(BeEquivalentTo("OK"))
		resultInfo, err := client.TSInfo(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["rules"]).To(BeEquivalentTo(map[interface{}]interface{}{}))
	})

	It("should TSIncrBy, TSIncrByWithArgs, TSDecrBy and TSDecrByWithArgs", Label("timeseries", "tsincrby", "tsdecrby", "tsincrbyWithArgs", "tsdecrbyWithArgs"), func() {
		for i := 0; i < 100; i++ {
			_, err := client.TSIncrBy(ctx, "1", 1).Result()
			Expect(err).NotTo(HaveOccurred())
		}
		result, err := client.TSGet(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Value).To(BeEquivalentTo(100))

		for i := 0; i < 100; i++ {
			_, err := client.TSDecrBy(ctx, "1", 1).Result()
			Expect(err).NotTo(HaveOccurred())
		}
		result, err = client.TSGet(ctx, "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Value).To(BeEquivalentTo(0))

		opt := &redis.TSIncrDecrOptions{Timestamp: 5}
		_, err = client.TSIncrByWithArgs(ctx, "2", 1.5, opt).Result()
		Expect(err).NotTo(HaveOccurred())

		result, err = client.TSGet(ctx, "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Timestamp).To(BeEquivalentTo(5))
		Expect(result.Value).To(BeEquivalentTo(1.5))

		opt = &redis.TSIncrDecrOptions{Timestamp: 7}
		_, err = client.TSIncrByWithArgs(ctx, "2", 2.25, opt).Result()
		Expect(err).NotTo(HaveOccurred())

		result, err = client.TSGet(ctx, "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Timestamp).To(BeEquivalentTo(7))
		Expect(result.Value).To(BeEquivalentTo(3.75))

		opt = &redis.TSIncrDecrOptions{Timestamp: 15}
		_, err = client.TSDecrByWithArgs(ctx, "2", 1.5, opt).Result()
		Expect(err).NotTo(HaveOccurred())

		result, err = client.TSGet(ctx, "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Timestamp).To(BeEquivalentTo(15))
		Expect(result.Value).To(BeEquivalentTo(2.25))

		// Test chunk size INCRBY
		opt = &redis.TSIncrDecrOptions{ChunkSize: 128}
		_, err = client.TSIncrByWithArgs(ctx, "3", 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())

		resultInfo, err := client.TSInfo(ctx, "3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["chunkSize"]).To(BeEquivalentTo(128))

		// Test chunk size DECRBY
		opt = &redis.TSIncrDecrOptions{ChunkSize: 128}
		_, err = client.TSDecrByWithArgs(ctx, "4", 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())

		resultInfo, err = client.TSInfo(ctx, "4").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultInfo["chunkSize"]).To(BeEquivalentTo(128))
	})

	It("should TSGet", Label("timeseries", "tsget"), func() {
		opt := &redis.TSOptions{DuplicatePolicy: "max"}
		resultGet, err := client.TSAddWithArgs(ctx, "foo", 2265985, 151, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet).To(BeEquivalentTo(2265985))
		result, err := client.TSGet(ctx, "foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Timestamp).To(BeEquivalentTo(2265985))
		Expect(result.Value).To(BeEquivalentTo(151))
	})

	It("should TSGet Latest", Label("timeseries", "tsgetlatest", "NonRedisEnterprise"), func() {
		resultGet, err := client.TSCreate(ctx, "tsgl-1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet).To(BeEquivalentTo("OK"))
		resultGet, err = client.TSCreate(ctx, "tsgl-2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet).To(BeEquivalentTo("OK"))

		resultGet, err = client.TSCreateRule(ctx, "tsgl-1", "tsgl-2", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())

		Expect(resultGet).To(BeEquivalentTo("OK"))
		_, err = client.TSAdd(ctx, "tsgl-1", 1, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "tsgl-1", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "tsgl-1", 11, 7).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "tsgl-1", 13, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		result, errGet := client.TSGet(ctx, "tsgl-2").Result()
		Expect(errGet).NotTo(HaveOccurred())
		Expect(result.Timestamp).To(BeEquivalentTo(0))
		Expect(result.Value).To(BeEquivalentTo(4))
		result, errGet = client.TSGetWithArgs(ctx, "tsgl-2", &redis.TSGetOptions{Latest: true}).Result()
		Expect(errGet).NotTo(HaveOccurred())
		Expect(result.Timestamp).To(BeEquivalentTo(10))
		Expect(result.Value).To(BeEquivalentTo(8))
	})

	It("should TSInfo", Label("timeseries", "tsinfo"), func() {
		resultGet, err := client.TSAdd(ctx, "foo", 2265985, 151).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet).To(BeEquivalentTo(2265985))
		result, err := client.TSInfo(ctx, "foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["firstTimestamp"]).To(BeEquivalentTo(2265985))
	})

	It("should TSMAdd", Label("timeseries", "tsmadd"), func() {
		resultGet, err := client.TSCreate(ctx, "a").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultGet).To(BeEquivalentTo("OK"))
		ktvSlices := make([][]interface{}, 3)
		for i := 0; i < 3; i++ {
			ktvSlices[i] = make([]interface{}, 3)
			ktvSlices[i][0] = "a"
			for j := 1; j < 3; j++ {
				ktvSlices[i][j] = (i + j) * j
			}
		}
		result, err := client.TSMAdd(ctx, ktvSlices).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo([]int64{1, 2, 3}))
	})

	It("should TSMGet and TSMGetWithArgs", Label("timeseries", "tsmget", "tsmgetWithArgs", "NonRedisEnterprise"), func() {
		opt := &redis.TSOptions{Labels: map[string]string{"Test": "This"}}
		resultCreate, err := client.TSCreateWithArgs(ctx, "a", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		opt = &redis.TSOptions{Labels: map[string]string{"Test": "This", "Taste": "That"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "b", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		_, err = client.TSAdd(ctx, "a", "*", 15).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "b", "*", 25).Result()
		Expect(err).NotTo(HaveOccurred())

		result, err := client.TSMGet(ctx, []string{"Test=This"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][1].([]interface{})[1]).To(BeEquivalentTo(15))
		Expect(result["b"][1].([]interface{})[1]).To(BeEquivalentTo(25))
		mgetOpt := &redis.TSMGetOptions{WithLabels: true}
		result, err = client.TSMGetWithArgs(ctx, []string{"Test=This"}, mgetOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["b"][0]).To(BeEquivalentTo(map[interface{}]interface{}{"Test": "This", "Taste": "That"}))

		resultCreate, err = client.TSCreate(ctx, "c").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		opt = &redis.TSOptions{Labels: map[string]string{"is_compaction": "true"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "d", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		resultCreateRule, err := client.TSCreateRule(ctx, "c", "d", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreateRule).To(BeEquivalentTo("OK"))
		_, err = client.TSAdd(ctx, "c", 1, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 11, 7).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 13, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		result, err = client.TSMGet(ctx, []string{"is_compaction=true"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["d"][1]).To(BeEquivalentTo([]interface{}{int64(0), 4.0}))
		mgetOpt = &redis.TSMGetOptions{Latest: true}
		result, err = client.TSMGetWithArgs(ctx, []string{"is_compaction=true"}, mgetOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["d"][1]).To(BeEquivalentTo([]interface{}{int64(10), 8.0}))
	})

	It("should TSQueryIndex", Label("timeseries", "tsqueryindex"), func() {
		opt := &redis.TSOptions{Labels: map[string]string{"Test": "This"}}
		resultCreate, err := client.TSCreateWithArgs(ctx, "a", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		opt = &redis.TSOptions{Labels: map[string]string{"Test": "This", "Taste": "That"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "b", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		result, err := client.TSQueryIndex(ctx, []string{"Test=This"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		result, err = client.TSQueryIndex(ctx, []string{"Taste=That"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(1))
	})

	It("should TSDel and TSRange", Label("timeseries", "tsdel", "tsrange"), func() {
		for i := 0; i < 100; i++ {
			_, err := client.TSAdd(ctx, "a", i, float64(i%7)).Result()
			Expect(err).NotTo(HaveOccurred())
		}
		resultDelete, err := client.TSDel(ctx, "a", 0, 21).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultDelete).To(BeEquivalentTo(22))

		resultRange, err := client.TSRange(ctx, "a", 0, 21).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange).To(BeEquivalentTo([]redis.TSTimestampValue{}))

		resultRange, err = client.TSRange(ctx, "a", 22, 22).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 22, Value: 1}))
	})

	It("should TSRange, TSRangeWithArgs", Label("timeseries", "tsrange", "tsrangeWithArgs", "NonRedisEnterprise"), func() {
		for i := 0; i < 100; i++ {
			_, err := client.TSAdd(ctx, "a", i, float64(i%7)).Result()
			Expect(err).NotTo(HaveOccurred())

		}
		result, err := client.TSRange(ctx, "a", 0, 200).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(100))
		for i := 0; i < 100; i++ {
			client.TSAdd(ctx, "a", i+200, float64(i%7))
		}
		result, err = client.TSRange(ctx, "a", 0, 500).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(200))
		fts := make([]int, 0)
		for i := 10; i < 20; i++ {
			fts = append(fts, i)
		}
		opt := &redis.TSRangeOptions{FilterByTS: fts, FilterByValue: []int{1, 2}}
		result, err = client.TSRangeWithArgs(ctx, "a", 0, 500, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		opt = &redis.TSRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: "+"}
		result, err = client.TSRangeWithArgs(ctx, "a", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo([]redis.TSTimestampValue{{Timestamp: 0, Value: 10}, {Timestamp: 10, Value: 1}}))
		opt = &redis.TSRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: "5"}
		result, err = client.TSRangeWithArgs(ctx, "a", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo([]redis.TSTimestampValue{{Timestamp: 0, Value: 5}, {Timestamp: 5, Value: 6}}))
		opt = &redis.TSRangeOptions{Aggregator: redis.Twa, BucketDuration: 10}
		result, err = client.TSRangeWithArgs(ctx, "a", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo([]redis.TSTimestampValue{{Timestamp: 0, Value: 2.55}, {Timestamp: 10, Value: 3}}))
		// Test Range Latest
		resultCreate, err := client.TSCreate(ctx, "t1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		resultCreate, err = client.TSCreate(ctx, "t2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		resultRule, err := client.TSCreateRule(ctx, "t1", "t2", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRule).To(BeEquivalentTo("OK"))
		_, errAdd := client.TSAdd(ctx, "t1", 1, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t1", 2, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t1", 11, 7).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t1", 13, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		resultRange, err := client.TSRange(ctx, "t1", 0, 20).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 1, Value: 1}))

		opt = &redis.TSRangeOptions{Latest: true}
		resultRange, err = client.TSRangeWithArgs(ctx, "t2", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 0, Value: 4}))
		// Test Bucket Timestamp
		resultCreate, err = client.TSCreate(ctx, "t3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		_, errAdd = client.TSAdd(ctx, "t3", 15, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 17, 4).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 51, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 73, 5).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 75, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())

		opt = &redis.TSRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10}
		resultRange, err = client.TSRangeWithArgs(ctx, "t3", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 10, Value: 4}))
		Expect(len(resultRange)).To(BeEquivalentTo(3))

		opt = &redis.TSRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10, BucketTimestamp: "+"}
		resultRange, err = client.TSRangeWithArgs(ctx, "t3", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 20, Value: 4}))
		Expect(len(resultRange)).To(BeEquivalentTo(3))
		// Test Empty
		_, errAdd = client.TSAdd(ctx, "t4", 15, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 17, 4).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 51, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 73, 5).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 75, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())

		opt = &redis.TSRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10}
		resultRange, err = client.TSRangeWithArgs(ctx, "t4", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 10, Value: 4}))
		Expect(len(resultRange)).To(BeEquivalentTo(3))

		opt = &redis.TSRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10, Empty: true}
		resultRange, err = client.TSRangeWithArgs(ctx, "t4", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 10, Value: 4}))
		Expect(len(resultRange)).To(BeEquivalentTo(7))
	})

	It("should TSRevRange, TSRevRangeWithArgs", Label("timeseries", "tsrevrange", "tsrevrangeWithArgs", "NonRedisEnterprise"), func() {
		for i := 0; i < 100; i++ {
			_, err := client.TSAdd(ctx, "a", i, float64(i%7)).Result()
			Expect(err).NotTo(HaveOccurred())

		}
		result, err := client.TSRange(ctx, "a", 0, 200).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(100))
		for i := 0; i < 100; i++ {
			client.TSAdd(ctx, "a", i+200, float64(i%7))
		}
		result, err = client.TSRange(ctx, "a", 0, 500).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(200))

		opt := &redis.TSRevRangeOptions{Aggregator: redis.Avg, BucketDuration: 10}
		result, err = client.TSRevRangeWithArgs(ctx, "a", 0, 500, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(20))

		opt = &redis.TSRevRangeOptions{Count: 10}
		result, err = client.TSRevRangeWithArgs(ctx, "a", 0, 500, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(10))

		fts := make([]int, 0)
		for i := 10; i < 20; i++ {
			fts = append(fts, i)
		}
		opt = &redis.TSRevRangeOptions{FilterByTS: fts, FilterByValue: []int{1, 2}}
		result, err = client.TSRevRangeWithArgs(ctx, "a", 0, 500, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))

		opt = &redis.TSRevRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: "+"}
		result, err = client.TSRevRangeWithArgs(ctx, "a", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo([]redis.TSTimestampValue{{Timestamp: 10, Value: 1}, {Timestamp: 0, Value: 10}}))

		opt = &redis.TSRevRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: "1"}
		result, err = client.TSRevRangeWithArgs(ctx, "a", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo([]redis.TSTimestampValue{{Timestamp: 1, Value: 10}, {Timestamp: 0, Value: 1}}))

		opt = &redis.TSRevRangeOptions{Aggregator: redis.Twa, BucketDuration: 10}
		result, err = client.TSRevRangeWithArgs(ctx, "a", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeEquivalentTo([]redis.TSTimestampValue{{Timestamp: 10, Value: 3}, {Timestamp: 0, Value: 2.55}}))
		// Test Range Latest
		resultCreate, err := client.TSCreate(ctx, "t1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		resultCreate, err = client.TSCreate(ctx, "t2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		resultRule, err := client.TSCreateRule(ctx, "t1", "t2", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRule).To(BeEquivalentTo("OK"))
		_, errAdd := client.TSAdd(ctx, "t1", 1, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t1", 2, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t1", 11, 7).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t1", 13, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		resultRange, err := client.TSRange(ctx, "t2", 0, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 0, Value: 4}))
		opt = &redis.TSRevRangeOptions{Latest: true}
		resultRange, err = client.TSRevRangeWithArgs(ctx, "t2", 0, 10, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 10, Value: 8}))
		resultRange, err = client.TSRevRangeWithArgs(ctx, "t2", 0, 9, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 0, Value: 4}))
		// Test Bucket Timestamp
		resultCreate, err = client.TSCreate(ctx, "t3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		_, errAdd = client.TSAdd(ctx, "t3", 15, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 17, 4).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 51, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 73, 5).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t3", 75, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())

		opt = &redis.TSRevRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10}
		resultRange, err = client.TSRevRangeWithArgs(ctx, "t3", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 70, Value: 5}))
		Expect(len(resultRange)).To(BeEquivalentTo(3))

		opt = &redis.TSRevRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10, BucketTimestamp: "+"}
		resultRange, err = client.TSRevRangeWithArgs(ctx, "t3", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 80, Value: 5}))
		Expect(len(resultRange)).To(BeEquivalentTo(3))
		// Test Empty
		_, errAdd = client.TSAdd(ctx, "t4", 15, 1).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 17, 4).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 51, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 73, 5).Result()
		Expect(errAdd).NotTo(HaveOccurred())
		_, errAdd = client.TSAdd(ctx, "t4", 75, 3).Result()
		Expect(errAdd).NotTo(HaveOccurred())

		opt = &redis.TSRevRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10}
		resultRange, err = client.TSRevRangeWithArgs(ctx, "t4", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 70, Value: 5}))
		Expect(len(resultRange)).To(BeEquivalentTo(3))

		opt = &redis.TSRevRangeOptions{Aggregator: redis.Max, Align: 0, BucketDuration: 10, Empty: true}
		resultRange, err = client.TSRevRangeWithArgs(ctx, "t4", 0, 100, opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultRange[0]).To(BeEquivalentTo(redis.TSTimestampValue{Timestamp: 70, Value: 5}))
		Expect(len(resultRange)).To(BeEquivalentTo(7))
	})

	It("should TSMRange and TSMRangeWithArgs", Label("timeseries", "tsmrange", "tsmrangeWithArgs"), func() {
		createOpt := &redis.TSOptions{Labels: map[string]string{"Test": "This", "team": "ny"}}
		resultCreate, err := client.TSCreateWithArgs(ctx, "a", createOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		createOpt = &redis.TSOptions{Labels: map[string]string{"Test": "This", "Taste": "That", "team": "sf"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "b", createOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))

		for i := 0; i < 100; i++ {
			_, err := client.TSAdd(ctx, "a", i, float64(i%7)).Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.TSAdd(ctx, "b", i, float64(i%11)).Result()
			Expect(err).NotTo(HaveOccurred())
		}

		result, err := client.TSMRange(ctx, 0, 200, []string{"Test=This"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		Expect(len(result["a"][2].([]interface{}))).To(BeEquivalentTo(100))
		// Test Count
		mrangeOpt := &redis.TSMRangeOptions{Count: 10}
		result, err = client.TSMRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result["a"][2].([]interface{}))).To(BeEquivalentTo(10))
		// Test Aggregation and BucketDuration
		for i := 0; i < 100; i++ {
			_, err := client.TSAdd(ctx, "a", i+200, float64(i%7)).Result()
			Expect(err).NotTo(HaveOccurred())
		}
		mrangeOpt = &redis.TSMRangeOptions{Aggregator: redis.Avg, BucketDuration: 10}
		result, err = client.TSMRangeWithArgs(ctx, 0, 500, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		Expect(len(result["a"][2].([]interface{}))).To(BeEquivalentTo(20))
		// Test WithLabels
		Expect(result["a"][0]).To(BeEquivalentTo(map[interface{}]interface{}{}))
		mrangeOpt = &redis.TSMRangeOptions{WithLabels: true}
		result, err = client.TSMRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][0]).To(BeEquivalentTo(map[interface{}]interface{}{"Test": "This", "team": "ny"}))
		// Test SelectedLabels
		mrangeOpt = &redis.TSMRangeOptions{SelectedLabels: []interface{}{"team"}}
		result, err = client.TSMRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][0]).To(BeEquivalentTo(map[interface{}]interface{}{"team": "ny"}))
		Expect(result["b"][0]).To(BeEquivalentTo(map[interface{}]interface{}{"team": "sf"}))
		// Test FilterBy
		fts := make([]int, 0)
		for i := 10; i < 20; i++ {
			fts = append(fts, i)
		}
		mrangeOpt = &redis.TSMRangeOptions{FilterByTS: fts, FilterByValue: []int{1, 2}}
		result, err = client.TSMRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(15), 1.0}, []interface{}{int64(16), 2.0}}))
		// Test GroupBy
		mrangeOpt = &redis.TSMRangeOptions{GroupByLabel: "Test", Reducer: "sum"}
		result, err = client.TSMRangeWithArgs(ctx, 0, 3, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["Test=This"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 0.0}, []interface{}{int64(1), 2.0}, []interface{}{int64(2), 4.0}, []interface{}{int64(3), 6.0}}))

		mrangeOpt = &redis.TSMRangeOptions{GroupByLabel: "Test", Reducer: "max"}
		result, err = client.TSMRangeWithArgs(ctx, 0, 3, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["Test=This"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 0.0}, []interface{}{int64(1), 1.0}, []interface{}{int64(2), 2.0}, []interface{}{int64(3), 3.0}}))

		mrangeOpt = &redis.TSMRangeOptions{GroupByLabel: "team", Reducer: "min"}
		result, err = client.TSMRangeWithArgs(ctx, 0, 3, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		Expect(result["team=ny"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 0.0}, []interface{}{int64(1), 1.0}, []interface{}{int64(2), 2.0}, []interface{}{int64(3), 3.0}}))
		Expect(result["team=sf"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 0.0}, []interface{}{int64(1), 1.0}, []interface{}{int64(2), 2.0}, []interface{}{int64(3), 3.0}}))
		// Test Align
		mrangeOpt = &redis.TSMRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: "-"}
		result, err = client.TSMRangeWithArgs(ctx, 0, 10, []string{"team=ny"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 10.0}, []interface{}{int64(10), 1.0}}))

		mrangeOpt = &redis.TSMRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: 5}
		result, err = client.TSMRangeWithArgs(ctx, 0, 10, []string{"team=ny"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 5.0}, []interface{}{int64(5), 6.0}}))
	})

	It("should TSMRangeWithArgs Latest", Label("timeseries", "tsmrangeWithArgs", "tsmrangelatest", "NonRedisEnterprise"), func() {
		resultCreate, err := client.TSCreate(ctx, "a").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		opt := &redis.TSOptions{Labels: map[string]string{"is_compaction": "true"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "b", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))

		resultCreate, err = client.TSCreate(ctx, "c").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		opt = &redis.TSOptions{Labels: map[string]string{"is_compaction": "true"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "d", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))

		resultCreateRule, err := client.TSCreateRule(ctx, "a", "b", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreateRule).To(BeEquivalentTo("OK"))
		resultCreateRule, err = client.TSCreateRule(ctx, "c", "d", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreateRule).To(BeEquivalentTo("OK"))

		_, err = client.TSAdd(ctx, "a", 1, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "a", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "a", 11, 7).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "a", 13, 1).Result()
		Expect(err).NotTo(HaveOccurred())

		_, err = client.TSAdd(ctx, "c", 1, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 11, 7).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 13, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		mrangeOpt := &redis.TSMRangeOptions{Latest: true}
		result, err := client.TSMRangeWithArgs(ctx, 0, 10, []string{"is_compaction=true"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["b"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 4.0}, []interface{}{int64(10), 8.0}}))
		Expect(result["d"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(0), 4.0}, []interface{}{int64(10), 8.0}}))
	})
	It("should TSMRevRange and TSMRevRangeWithArgs", Label("timeseries", "tsmrevrange", "tsmrevrangeWithArgs"), func() {
		createOpt := &redis.TSOptions{Labels: map[string]string{"Test": "This", "team": "ny"}}
		resultCreate, err := client.TSCreateWithArgs(ctx, "a", createOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		createOpt = &redis.TSOptions{Labels: map[string]string{"Test": "This", "Taste": "That", "team": "sf"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "b", createOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))

		for i := 0; i < 100; i++ {
			_, err := client.TSAdd(ctx, "a", i, float64(i%7)).Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.TSAdd(ctx, "b", i, float64(i%11)).Result()
			Expect(err).NotTo(HaveOccurred())
		}
		result, err := client.TSMRevRange(ctx, 0, 200, []string{"Test=This"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		Expect(len(result["a"][2].([]interface{}))).To(BeEquivalentTo(100))
		// Test Count
		mrangeOpt := &redis.TSMRevRangeOptions{Count: 10}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result["a"][2].([]interface{}))).To(BeEquivalentTo(10))
		// Test Aggregation and BucketDuration
		for i := 0; i < 100; i++ {
			_, err := client.TSAdd(ctx, "a", i+200, float64(i%7)).Result()
			Expect(err).NotTo(HaveOccurred())
		}
		mrangeOpt = &redis.TSMRevRangeOptions{Aggregator: redis.Avg, BucketDuration: 10}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 500, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		Expect(len(result["a"][2].([]interface{}))).To(BeEquivalentTo(20))
		Expect(result["a"][0]).To(BeEquivalentTo(map[interface{}]interface{}{}))
		// Test WithLabels
		Expect(result["a"][0]).To(BeEquivalentTo(map[interface{}]interface{}{}))
		mrangeOpt = &redis.TSMRevRangeOptions{WithLabels: true}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][0]).To(BeEquivalentTo(map[interface{}]interface{}{"Test": "This", "team": "ny"}))
		// Test SelectedLabels
		mrangeOpt = &redis.TSMRevRangeOptions{SelectedLabels: []interface{}{"team"}}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][0]).To(BeEquivalentTo(map[interface{}]interface{}{"team": "ny"}))
		Expect(result["b"][0]).To(BeEquivalentTo(map[interface{}]interface{}{"team": "sf"}))
		// Test FilterBy
		fts := make([]int, 0)
		for i := 10; i < 20; i++ {
			fts = append(fts, i)
		}
		mrangeOpt = &redis.TSMRevRangeOptions{FilterByTS: fts, FilterByValue: []int{1, 2}}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 200, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(16), 2.0}, []interface{}{int64(15), 1.0}}))
		// Test GroupBy
		mrangeOpt = &redis.TSMRevRangeOptions{GroupByLabel: "Test", Reducer: "sum"}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 3, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["Test=This"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(3), 6.0}, []interface{}{int64(2), 4.0}, []interface{}{int64(1), 2.0}, []interface{}{int64(0), 0.0}}))

		mrangeOpt = &redis.TSMRevRangeOptions{GroupByLabel: "Test", Reducer: "max"}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 3, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["Test=This"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(3), 3.0}, []interface{}{int64(2), 2.0}, []interface{}{int64(1), 1.0}, []interface{}{int64(0), 0.0}}))

		mrangeOpt = &redis.TSMRevRangeOptions{GroupByLabel: "team", Reducer: "min"}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 3, []string{"Test=This"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(result)).To(BeEquivalentTo(2))
		Expect(result["team=ny"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(3), 3.0}, []interface{}{int64(2), 2.0}, []interface{}{int64(1), 1.0}, []interface{}{int64(0), 0.0}}))
		Expect(result["team=sf"][3]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(3), 3.0}, []interface{}{int64(2), 2.0}, []interface{}{int64(1), 1.0}, []interface{}{int64(0), 0.0}}))
		// Test Align
		mrangeOpt = &redis.TSMRevRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: "-"}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 10, []string{"team=ny"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(10), 1.0}, []interface{}{int64(0), 10.0}}))

		mrangeOpt = &redis.TSMRevRangeOptions{Aggregator: redis.Count, BucketDuration: 10, Align: 1}
		result, err = client.TSMRevRangeWithArgs(ctx, 0, 10, []string{"team=ny"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["a"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(1), 10.0}, []interface{}{int64(0), 1.0}}))
	})

	It("should TSMRevRangeWithArgs Latest", Label("timeseries", "tsmrevrangeWithArgs", "tsmrevrangelatest", "NonRedisEnterprise"), func() {
		resultCreate, err := client.TSCreate(ctx, "a").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		opt := &redis.TSOptions{Labels: map[string]string{"is_compaction": "true"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "b", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))

		resultCreate, err = client.TSCreate(ctx, "c").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))
		opt = &redis.TSOptions{Labels: map[string]string{"is_compaction": "true"}}
		resultCreate, err = client.TSCreateWithArgs(ctx, "d", opt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreate).To(BeEquivalentTo("OK"))

		resultCreateRule, err := client.TSCreateRule(ctx, "a", "b", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreateRule).To(BeEquivalentTo("OK"))
		resultCreateRule, err = client.TSCreateRule(ctx, "c", "d", redis.Sum, 10).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resultCreateRule).To(BeEquivalentTo("OK"))

		_, err = client.TSAdd(ctx, "a", 1, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "a", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "a", 11, 7).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "a", 13, 1).Result()
		Expect(err).NotTo(HaveOccurred())

		_, err = client.TSAdd(ctx, "c", 1, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 2, 3).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 11, 7).Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.TSAdd(ctx, "c", 13, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		mrangeOpt := &redis.TSMRevRangeOptions{Latest: true}
		result, err := client.TSMRevRangeWithArgs(ctx, 0, 10, []string{"is_compaction=true"}, mrangeOpt).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result["b"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(10), 8.0}, []interface{}{int64(0), 4.0}}))
		Expect(result["d"][2]).To(BeEquivalentTo([]interface{}{[]interface{}{int64(10), 8.0}, []interface{}{int64(0), 4.0}}))
	})
})
