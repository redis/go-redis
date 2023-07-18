package redis_test

import (
	"context"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

type JSONGetTestStruct struct {
	Hello string `json:"hello"`
}

var _ = Describe("JSON Commands", Label("json"), func() {

	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("arrays", Label("arrays"), func() {

		It("should JSONArrAppend", Label("json.arrappend"), func() {
			cmd1 := client.JSONSet(ctx, "append2", "$", `{"a": [10], "b": {"a": [12, 13]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrAppend(ctx, "append2", "$..a", 10)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{2, 3}))
		})

		It("should JSONArrIndex", Label("json.arrindex"), func() {
			cmd1 := client.JSONSet(ctx, "index3", "$", `{"a": [10], "b": {"a": [12, 10]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrIndex(ctx, "index3", "$.b.a", 10)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{1}))
		})

		It("should JSONArrIndexStartStop", Label("json.arrindex"), func() {
			cmd1 := client.JSONSet(ctx, "index4", "$", `{"a": [10], "b": {"a": [12, 10, 20, 12, 90, 10]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrIndexStartStop(ctx, "index4", "$.b.a", 12, 1, 4)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{3}))
		})

		It("should JSONArrInsert", Label("json.arrinsert"), func() {
			cmd1 := client.JSONSet(ctx, "insert2", "$", `[100, 200, 300, 200]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrInsert(ctx, "insert2", "$", -1, 1, 2)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{6}))

			cmd3 := client.JSONGet(ctx, "insert2")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(Equal([]interface{}{float64(100), float64(200), float64(300), float64(1), float64(2), float64(200)}))
		})

		It("should JSONArrLen", Label("json.arrlen"), func() {
			cmd1 := client.JSONSet(ctx, "length2", "$", `{"a": [10], "b": {"a": [12, 10, 20, 12, 90, 10]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrLen(ctx, "length2", "$..a")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{1, 6}))
		})

		It("should JSONArrPop", Label("json.arrpop"), func() {
			cmd1 := client.JSONSet(ctx, "pop4", "$", `[100, 200, 300, 200]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrPop(ctx, "pop4", "$", 2)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]string{"300"}))

			cmd3 := client.JSONGet(ctx, "pop4", "$")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(1))
			Expect(cmd3.Val()[0]).To(BeAssignableToTypeOf([]interface{}{1}))
			Expect(cmd3.Val()[0]).To(Equal([]interface{}{float64(100), float64(200), float64(200)}))
		})

		It("should JSONArrTrim", func() {
			cmd1 := client.JSONSet(ctx, "trim5", "$", `{"a": [100, 200, 300, 200], "b": {"a": [100, 200, 300, 200]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrTrim(ctx, "trim5", "$..a", 1, 2)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{2, 2}))

			cmd3 := client.JSONGet(ctx, "trim5", "$.a")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(1))
			Expect(cmd3.Val()[0]).To(BeAssignableToTypeOf([]interface{}{1}))
			Expect(cmd3.Val()[0]).To(Equal([]interface{}{float64(200), float64(300)}))

			cmd3 = client.JSONGet(ctx, "trim5", "$.b.a")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(1))
			Expect(cmd3.Val()[0]).To(BeAssignableToTypeOf([]interface{}{1}))
			Expect(cmd3.Val()[0]).To(Equal([]interface{}{float64(200), float64(300)}))
		})

	})

	Describe("get/set", Label("getset"), func() {
		It("should JSONSet", Label("json.set"), func() {
			cmd := client.JSONSet(ctx, "set1", "$", `{"a": 1, "b": 2, "hello": "world"}`)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			Expect(cmd.Val()).To(Equal("OK"))
		})

		It("should JSONGet", Label("json.get"), func() {
			cmd1 := client.JSONSet(ctx, "get3", "$", `{"a": 1, "b": 2, "c": {"hello": "world"}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONGet(ctx, "get3", "$.*")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(len(cmd2.Val())).To(Equal(3))
		})

		It("should Scan", Label("json.get"), func() {
			cmd1 := client.JSONSet(ctx, "get4", "$", `{"a": 1, "b": 2, "c": {"hello": "golang"}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONGet(ctx, "get4", "$.*")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(len(cmd2.Val())).To(Equal(3))

			test := JSONGetTestStruct{}
			err := cmd2.Scan(2, &test)
			Expect(err).NotTo(HaveOccurred())
			Expect(test.Hello).To(Equal("golang"))
		})

		It("should JSONMGet", func() {
			cmd1 := client.JSONSet(ctx, "mget2a", "$", `{"a": ["aa", "ab", "ac", "ad"], "b": {"a": ["ba", "bb", "bc", "bd"]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))
			cmd2 := client.JSONSet(ctx, "mget2b", "$", `{"a": [100, 200, 300, 200], "b": {"a": [100, 200, 300, 200]}}`)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal("OK"))

			cmd3 := client.JSONMGet(ctx, "$..a", "mget2a", "mget2b")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(2))
			Expect(cmd3.Val()[0]).To(BeAssignableToTypeOf([]interface{}{1}))
			Expect(cmd3.Val()[0]).To(Equal([]interface{}{
				[]interface{}{"aa", "ab", "ac", "ad"},
				[]interface{}{"ba", "bb", "bc", "bd"},
			}))
			Expect(cmd3.Val()[1]).To(BeAssignableToTypeOf([]interface{}{1}))
			Expect(cmd3.Val()[1]).To(Equal([]interface{}{
				[]interface{}{float64(100), float64(200), float64(300), float64(200)},
				[]interface{}{float64(100), float64(200), float64(300), float64(200)},
			}))
		})

	})

	Describe("Misc", Label("misc"), func() {

		It("should JSONClear", Label("json.clear"), func() {
			cmd1 := client.JSONSet(ctx, "clear1", "$", `[1]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONClear(ctx, "clear1", "$")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal(int64(1)))

			cmd3 := client.JSONGet(ctx, "clear1", "$")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(1))
			Expect(cmd3.Val()[0]).To(BeAssignableToTypeOf([]interface{}{1}))
			Expect(cmd3.Val()[0]).To(Equal([]interface{}{}))
		})

		It("should JSONDel", Label("json.del"), func() {
			cmd1 := client.JSONSet(ctx, "del1", "$", `[1]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONDel(ctx, "del1", "$")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal(int64(1)))

			cmd3 := client.JSONGet(ctx, "del1", "$")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(0))
		})

		It("should JSONForget", Label("json.forget"), func() {
			cmd1 := client.JSONSet(ctx, "forget3", "$", `{"a": [1,2,3], "b": {"a": [1,2,3], "b": "annie"}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONForget(ctx, "forget3", "$..a")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal(int64(2)))

			cmd3 := client.JSONGet(ctx, "forget3", "$")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(1))
			Expect(cmd3.Val()[0]).To(BeAssignableToTypeOf(map[string]interface{}{"foo": "bar"}))
			Expect(cmd3.Val()[0]).To(Equal(map[string]interface{}{
				"b": map[string]interface{}{
					"b": "annie",
				},
			}))

		})

		It("should JSONNumIncrBy", Label("json.numincrby"), func() {
			cmd1 := client.JSONSet(ctx, "incr3", "$", `{"a": [1, 2], "b": {"a": [0, -1]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONNumIncrBy(ctx, "incr3", "$..a[1]", float64(1))
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]interface{}{float64(3), float64(0)}))
		})

		It("should JSONObjKeys", Label("json.objkeys"), func() {
			cmd1 := client.JSONSet(ctx, "objkeys1", "$", `{"a": [1, 2], "b": {"a": [0, -1]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONObjKeys(ctx, "objkeys1", "$..*")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(7))
			Expect(cmd2.Val()).To(Equal([]interface{}{nil, []interface{}{"a"}, nil, nil, nil, nil, nil}))
		})

		It("should JSONObjLen", Label("json.objlen"), func() {
			cmd1 := client.JSONSet(ctx, "objlen2", "$", `{"a": [1, 2], "b": {"a": [0, -1]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONObjLen(ctx, "objlen2", "$..*")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(7))
			Expect(cmd2.Val()[0]).To(BeNil())
			Expect(*cmd2.Val()[1]).To(Equal(int64(1)))
		})

		It("should JSONStrLen", Label("json.strlen"), func() {
			cmd1 := client.JSONSet(ctx, "strlen2", "$", `{"a": "alice", "b": "bob", "c": {"a": "alice", "b": "bob"}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONStrLen(ctx, "strlen2", "$..*")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(5))
			var tmp int64 = 20
			Expect(cmd2.Val()[0]).To(BeAssignableToTypeOf(&tmp))
			Expect(*cmd2.Val()[0]).To(Equal(int64(5)))
			Expect(*cmd2.Val()[1]).To(Equal(int64(3)))
			Expect(cmd2.Val()[2]).To(BeNil())
			Expect(*cmd2.Val()[3]).To(Equal(int64(5)))
			Expect(*cmd2.Val()[4]).To(Equal(int64(3)))
		})

		It("should JSONToggle", Label("json.toggle"), func() {
			cmd1 := client.JSONSet(ctx, "toggle1", "$", `[true]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONToggle(ctx, "toggle1", "$[0]")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(1))
			Expect(*cmd2.Val()[0]).To(Equal(int64(0)))
		})

		It("should JSONType", Label("json.type"), func() {
			cmd1 := client.JSONSet(ctx, "type1", "$", `[true]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONType(ctx, "type1", "$[0]")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(1))
			Expect(cmd2.Val()[0]).To(Equal("boolean"))
		})
	})
})
