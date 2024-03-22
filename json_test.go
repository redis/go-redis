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
		client = redis.NewClient(&redis.Options{Addr: ":6379"})
		Expect(client.FlushAll(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("arrays", Label("arrays"), func() {
		It("should JSONArrAppend", Label("json.arrappend", "json"), func() {
			cmd1 := client.JSONSet(ctx, "append2", "$", `{"a": [10], "b": {"a": [12, 13]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrAppend(ctx, "append2", "$..a", 10)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{2, 3}))
		})

		It("should JSONArrIndex and JSONArrIndexWithArgs", Label("json.arrindex", "json"), func() {
			cmd1, err := client.JSONSet(ctx, "index1", "$", `{"a": [10], "b": {"a": [12, 10]}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd1).To(Equal("OK"))

			cmd2, err := client.JSONArrIndex(ctx, "index1", "$.b.a", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd2).To(Equal([]int64{1}))

			cmd3, err := client.JSONSet(ctx, "index2", "$", `[0,1,2,3,4]`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd3).To(Equal("OK"))

			res, err := client.JSONArrIndex(ctx, "index2", "$", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(Equal(int64(1)))

			res, err = client.JSONArrIndex(ctx, "index2", "$", 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(Equal(int64(-1)))

			res, err = client.JSONArrIndex(ctx, "index2", "$", 4).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(Equal(int64(4)))

			res, err = client.JSONArrIndexWithArgs(ctx, "index2", "$", &redis.JSONArrIndexArgs{}, 4).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(Equal(int64(4)))

			stop := 5000
			res, err = client.JSONArrIndexWithArgs(ctx, "index2", "$", &redis.JSONArrIndexArgs{Stop: &stop}, 4).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(Equal(int64(4)))

			stop = -1
			res, err = client.JSONArrIndexWithArgs(ctx, "index2", "$", &redis.JSONArrIndexArgs{Stop: &stop}, 4).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(Equal(int64(-1)))
		})

		It("should JSONArrIndex and JSONArrIndexWithArgs with $", Label("json.arrindex", "json"), func() {
			doc := `{
				"store": {
					"book": [
						{
							"category": "reference",
							"author": "Nigel Rees",
							"title": "Sayings of the Century",
							"price": 8.95,
							"size": [10, 20, 30, 40]
						},
						{
							"category": "fiction",
							"author": "Evelyn Waugh",
							"title": "Sword of Honour",
							"price": 12.99,
							"size": [50, 60, 70, 80]
						},
						{
							"category": "fiction",
							"author": "Herman Melville",
							"title": "Moby Dick",
							"isbn": "0-553-21311-3",
							"price": 8.99,
							"size": [5, 10, 20, 30]
						},
						{
							"category": "fiction",
							"author": "J. R. R. Tolkien",
							"title": "The Lord of the Rings",
							"isbn": "0-395-19395-8",
							"price": 22.99,
							"size": [5, 6, 7, 8]
						}
					],
					"bicycle": {"color": "red", "price": 19.95}
				}
			}`
			res, err := client.JSONSet(ctx, "doc1", "$", doc).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			resGet, err := client.JSONGet(ctx, "doc1", "$.store.book[?(@.price<10)].size").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal("[[10,20,30,40],[5,10,20,30]]"))

			resArr, err := client.JSONArrIndex(ctx, "doc1", "$.store.book[?(@.price<10)].size", 20).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resArr).To(Equal([]int64{1, 2}))
		})

		It("should JSONArrInsert", Label("json.arrinsert", "json"), func() {
			cmd1 := client.JSONSet(ctx, "insert2", "$", `[100, 200, 300, 200]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrInsert(ctx, "insert2", "$", -1, 1, 2)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]int64{6}))

			cmd3 := client.JSONGet(ctx, "insert2")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			// RESP2 vs RESP3
			Expect(cmd3.Val()).To(Or(
				Equal(`[100,200,300,1,2,200]`),
				Equal(`[[100,200,300,1,2,200]]`)))
		})

		It("should JSONArrLen", Label("json.arrlen", "json"), func() {
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
			Expect(cmd3.Val()).To(Equal("[[100,200,200]]"))
		})

		It("should JSONArrTrim", Label("json.arrtrim", "json"), func() {
			cmd1, err := client.JSONSet(ctx, "trim1", "$", `[0,1,2,3,4]`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd1).To(Equal("OK"))

			stop := 3
			cmd2, err := client.JSONArrTrimWithArgs(ctx, "trim1", "$", &redis.JSONArrTrimArgs{Start: 1, Stop: &stop}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd2).To(Equal([]int64{3}))

			res, err := client.JSONGet(ctx, "trim1", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(`[[1,2,3]]`))

			cmd3, err := client.JSONSet(ctx, "trim2", "$", `[0,1,2,3,4]`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd3).To(Equal("OK"))

			stop = 3
			cmd4, err := client.JSONArrTrimWithArgs(ctx, "trim2", "$", &redis.JSONArrTrimArgs{Start: -1, Stop: &stop}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd4).To(Equal([]int64{0}))

			cmd5, err := client.JSONSet(ctx, "trim3", "$", `[0,1,2,3,4]`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd5).To(Equal("OK"))

			stop = 99
			cmd6, err := client.JSONArrTrimWithArgs(ctx, "trim3", "$", &redis.JSONArrTrimArgs{Start: 3, Stop: &stop}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd6).To(Equal([]int64{2}))

			cmd7, err := client.JSONSet(ctx, "trim4", "$", `[0,1,2,3,4]`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd7).To(Equal("OK"))

			stop = 1
			cmd8, err := client.JSONArrTrimWithArgs(ctx, "trim4", "$", &redis.JSONArrTrimArgs{Start: 9, Stop: &stop}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd8).To(Equal([]int64{0}))

			cmd9, err := client.JSONSet(ctx, "trim5", "$", `[0,1,2,3,4]`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd9).To(Equal("OK"))

			stop = 11
			cmd10, err := client.JSONArrTrimWithArgs(ctx, "trim5", "$", &redis.JSONArrTrimArgs{Start: 9, Stop: &stop}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd10).To(Equal([]int64{0}))
		})

		It("should JSONArrPop", Label("json.arrpop", "json"), func() {
			cmd1 := client.JSONSet(ctx, "pop4", "$", `[100, 200, 300, 200]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONArrPop(ctx, "pop4", "$", 2)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal([]string{"300"}))

			cmd3 := client.JSONGet(ctx, "pop4", "$")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(Equal("[[100,200,200]]"))
		})
	})

	Describe("get/set", Label("getset"), func() {
		It("should JSONSet", Label("json.set", "json"), func() {
			cmd := client.JSONSet(ctx, "set1", "$", `{"a": 1, "b": 2, "hello": "world"}`)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			Expect(cmd.Val()).To(Equal("OK"))
		})

		It("should JSONGet", Label("json.get", "json"), func() {
			res, err := client.JSONSet(ctx, "get3", "$", `{"a": 1, "b": 2}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			res, err = client.JSONGetWithArgs(ctx, "get3", &redis.JSONGetArgs{Indent: "-"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(`[-{--"a":1,--"b":2-}]`))

			res, err = client.JSONGetWithArgs(ctx, "get3", &redis.JSONGetArgs{Indent: "-", Newline: `~`, Space: `!`}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(`[~-{~--"a":!1,~--"b":!2~-}~]`))
		})

		It("should JSONMerge", Label("json.merge", "json"), func() {
			res, err := client.JSONSet(ctx, "merge1", "$", `{"a": 1, "b": 2}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			res, err = client.JSONMerge(ctx, "merge1", "$", `{"b": 3, "c": 4}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			res, err = client.JSONGet(ctx, "merge1", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(`[{"a":1,"b":3,"c":4}]`))
		})

		It("should JSONMSet", Label("json.mset", "json", "NonRedisEnterprise"), func() {
			doc1 := redis.JSONSetArgs{Key: "mset1", Path: "$", Value: `{"a": 1}`}
			doc2 := redis.JSONSetArgs{Key: "mset2", Path: "$", Value: 2}
			docs := []redis.JSONSetArgs{doc1, doc2}

			mSetResult, err := client.JSONMSetArgs(ctx, docs).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(mSetResult).To(Equal("OK"))

			res, err := client.JSONMGet(ctx, "$", "mset1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]interface{}{`[{"a":1}]`}))

			res, err = client.JSONMGet(ctx, "$", "mset1", "mset2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal([]interface{}{`[{"a":1}]`, "[2]"}))

			_, err = client.JSONMSet(ctx, "mset1", "$.a", 2, "mset3", "$", `[1]`).Result()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should JSONMGet", Label("json.mget", "json", "NonRedisEnterprise"), func() {
			cmd1 := client.JSONSet(ctx, "mget2a", "$", `{"a": ["aa", "ab", "ac", "ad"], "b": {"a": ["ba", "bb", "bc", "bd"]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))
			cmd2 := client.JSONSet(ctx, "mget2b", "$", `{"a": [100, 200, 300, 200], "b": {"a": [100, 200, 300, 200]}}`)
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal("OK"))

			cmd3 := client.JSONMGet(ctx, "$..a", "mget2a", "mget2b")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(HaveLen(2))
			Expect(cmd3.Val()[0]).To(Equal(`[["aa","ab","ac","ad"],["ba","bb","bc","bd"]]`))
			Expect(cmd3.Val()[1]).To(Equal(`[[100,200,300,200],[100,200,300,200]]`))
		})

		It("should JSONMget with $", Label("json.mget", "json", "NonRedisEnterprise"), func() {
			res, err := client.JSONSet(ctx, "doc1", "$", `{"a": 1, "b": 2, "nested": {"a": 3}, "c": "", "nested2": {"a": ""}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			res, err = client.JSONSet(ctx, "doc2", "$", `{"a": 4, "b": 5, "nested": {"a": 6}, "c": "", "nested2": {"a": [""]}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err := client.JSONMGet(ctx, "$..a", "doc1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal([]interface{}{`[1,3,""]`}))

			iRes, err = client.JSONMGet(ctx, "$..a", "doc1", "doc2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal([]interface{}{`[1,3,""]`, `[4,6,[""]]`}))

			iRes, err = client.JSONMGet(ctx, "$..a", "non_existing_doc", "non_existing_doc1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal([]interface{}{nil, nil}))
		})
	})

	Describe("Misc", Label("misc"), func() {
		It("should JSONClear", Label("json.clear", "json"), func() {
			cmd1 := client.JSONSet(ctx, "clear1", "$", `[1]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONClear(ctx, "clear1", "$")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal(int64(1)))

			cmd3 := client.JSONGet(ctx, "clear1", "$")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(Equal(`[[]]`))
		})

		It("should JSONClear with $", Label("json.clear", "json"), func() {
			doc := `{
				"nested1": {"a": {"foo": 10, "bar": 20}},
				"a": ["foo"],
				"nested2": {"a": "claro"},
				"nested3": {"a": {"baz": 50}}
			}`
			res, err := client.JSONSet(ctx, "doc1", "$", doc).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err := client.JSONClear(ctx, "doc1", "$..a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(3)))

			resGet, err := client.JSONGet(ctx, "doc1", `$`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(`[{"nested1":{"a":{}},"a":[],"nested2":{"a":"claro"},"nested3":{"a":{}}}]`))

			res, err = client.JSONSet(ctx, "doc1", "$", doc).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err = client.JSONClear(ctx, "doc1", "$.nested1.a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(1)))

			resGet, err = client.JSONGet(ctx, "doc1", `$`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(`[{"nested1":{"a":{}},"a":["foo"],"nested2":{"a":"claro"},"nested3":{"a":{"baz":50}}}]`))
		})

		It("should JSONDel", Label("json.del", "json"), func() {
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

		It("should JSONDel with $", Label("json.del", "json"), func() {
			res, err := client.JSONSet(ctx, "del1", "$", `{"a": 1, "nested": {"a": 2, "b": 3}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err := client.JSONDel(ctx, "del1", "$..a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(2)))

			resGet, err := client.JSONGet(ctx, "del1", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(`[{"nested":{"b":3}}]`))

			res, err = client.JSONSet(ctx, "del2", "$", `{"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [true, "a", "b"]}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err = client.JSONDel(ctx, "del2", "$..a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(1)))

			resGet, err = client.JSONGet(ctx, "del2", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(`[{"nested":{"b":[true,"a","b"]},"b":["a","b"]}]`))

			doc := `[
				{
					"ciao": ["non ancora"],
					"nested": [
						{"ciao": [1, "a"]},
						{"ciao": [2, "a"]},
						{"ciaoc": [3, "non", "ciao"]},
						{"ciao": [4, "a"]},
						{"e": [5, "non", "ciao"]}
					]
				}
			]`
			res, err = client.JSONSet(ctx, "del3", "$", doc).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err = client.JSONDel(ctx, "del3", `$.[0]["nested"]..ciao`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(3)))

			resVal := `[[{"ciao":["non ancora"],"nested":[{},{},{"ciaoc":[3,"non","ciao"]},{},{"e":[5,"non","ciao"]}]}]]`
			resGet, err = client.JSONGet(ctx, "del3", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(resVal))
		})

		It("should JSONForget", Label("json.forget", "json"), func() {
			cmd1 := client.JSONSet(ctx, "forget3", "$", `{"a": [1,2,3], "b": {"a": [1,2,3], "b": "annie"}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONForget(ctx, "forget3", "$..a")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal(int64(2)))

			cmd3 := client.JSONGet(ctx, "forget3", "$")
			Expect(cmd3.Err()).NotTo(HaveOccurred())
			Expect(cmd3.Val()).To(Equal(`[{"b":{"b":"annie"}}]`))
		})

		It("should JSONForget with $", Label("json.forget", "json"), func() {
			res, err := client.JSONSet(ctx, "doc1", "$", `{"a": 1, "nested": {"a": 2, "b": 3}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err := client.JSONForget(ctx, "doc1", "$..a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(2)))

			resGet, err := client.JSONGet(ctx, "doc1", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(`[{"nested":{"b":3}}]`))

			res, err = client.JSONSet(ctx, "doc2", "$", `{"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [true, "a", "b"]}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err = client.JSONForget(ctx, "doc2", "$..a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(1)))

			resGet, err = client.JSONGet(ctx, "doc2", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(`[{"nested":{"b":[true,"a","b"]},"b":["a","b"]}]`))

			doc := `[
				{
					"ciao": ["non ancora"],
					"nested": [
						{"ciao": [1, "a"]},
						{"ciao": [2, "a"]},
						{"ciaoc": [3, "non", "ciao"]},
						{"ciao": [4, "a"]},
						{"e": [5, "non", "ciao"]}
					]
				}
			]`
			res, err = client.JSONSet(ctx, "doc3", "$", doc).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			iRes, err = client.JSONForget(ctx, "doc3", `$.[0]["nested"]..ciao`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(iRes).To(Equal(int64(3)))

			resVal := `[[{"ciao":["non ancora"],"nested":[{},{},{"ciaoc":[3,"non","ciao"]},{},{"e":[5,"non","ciao"]}]}]]`
			resGet, err = client.JSONGet(ctx, "doc3", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resGet).To(Equal(resVal))
		})

		It("should JSONNumIncrBy", Label("json.numincrby", "json"), func() {
			cmd1 := client.JSONSet(ctx, "incr3", "$", `{"a": [1, 2], "b": {"a": [0, -1]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONNumIncrBy(ctx, "incr3", "$..a[1]", float64(1))
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(Equal(`[3,0]`))
		})

		It("should JSONNumIncrBy with $", Label("json.numincrby", "json"), func() {
			res, err := client.JSONSet(ctx, "doc1", "$", `{"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			res, err = client.JSONNumIncrBy(ctx, "doc1", "$.b[1].a", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(`[7]`))

			res, err = client.JSONNumIncrBy(ctx, "doc1", "$.b[1].a", 3.5).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(`[10.5]`))

			res, err = client.JSONSet(ctx, "doc2", "$", `{"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			res, err = client.JSONNumIncrBy(ctx, "doc2", "$.b[0].a", 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(`[5]`))
		})

		It("should JSONObjKeys", Label("json.objkeys", "json"), func() {
			cmd1 := client.JSONSet(ctx, "objkeys1", "$", `{"a": [1, 2], "b": {"a": [0, -1]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONObjKeys(ctx, "objkeys1", "$..*")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(7))
			Expect(cmd2.Val()).To(Equal([]interface{}{nil, []interface{}{"a"}, nil, nil, nil, nil, nil}))
		})

		It("should JSONObjKeys with $", Label("json.objkeys", "json"), func() {
			doc := `{
				"nested1": {"a": {"foo": 10, "bar": 20}},
				"a": ["foo"],
				"nested2": {"a": {"baz": 50}}
			}`
			cmd1, err := client.JSONSet(ctx, "objkeys1", "$", doc).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd1).To(Equal("OK"))

			cmd2, err := client.JSONObjKeys(ctx, "objkeys1", "$.nested1.a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd2).To(Equal([]interface{}{[]interface{}{"foo", "bar"}}))

			cmd2, err = client.JSONObjKeys(ctx, "objkeys1", ".*.a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd2).To(Equal([]interface{}{"foo", "bar"}))

			cmd2, err = client.JSONObjKeys(ctx, "objkeys1", ".nested2.a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd2).To(Equal([]interface{}{"baz"}))

			_, err = client.JSONObjKeys(ctx, "non_existing_doc", "..a").Result()
			Expect(err).To(HaveOccurred())
		})

		It("should JSONObjLen", Label("json.objlen", "json"), func() {
			cmd1 := client.JSONSet(ctx, "objlen2", "$", `{"a": [1, 2], "b": {"a": [0, -1]}}`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONObjLen(ctx, "objlen2", "$..*")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(7))
			Expect(cmd2.Val()[0]).To(BeNil())
			Expect(*cmd2.Val()[1]).To(Equal(int64(1)))
		})

		It("should JSONStrLen", Label("json.strlen", "json"), func() {
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

		It("should JSONStrAppend", Label("json.strappend", "json"), func() {
			cmd1, err := client.JSONSet(ctx, "strapp1", "$", `"foo"`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd1).To(Equal("OK"))
			cmd2, err := client.JSONStrAppend(ctx, "strapp1", "$", `"bar"`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(*cmd2[0]).To(Equal(int64(6)))
			cmd3, err := client.JSONGet(ctx, "strapp1", "$").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmd3).To(Equal(`["foobar"]`))
		})

		It("should JSONStrAppend and JSONStrLen with $", Label("json.strappend", "json.strlen", "json"), func() {
			res, err := client.JSONSet(ctx, "doc1", "$", `{"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			intArrayResult, err := client.JSONStrAppend(ctx, "doc1", "$.nested1.a", `"baz"`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(*intArrayResult[0]).To(Equal(int64(8)))

			res, err = client.JSONSet(ctx, "doc2", "$", `{"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}`).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))

			intResult, err := client.JSONStrLen(ctx, "doc2", "$.nested1.a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(*intResult[0]).To(Equal(int64(5)))
		})

		It("should JSONToggle", Label("json.toggle", "json"), func() {
			cmd1 := client.JSONSet(ctx, "toggle1", "$", `[true]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONToggle(ctx, "toggle1", "$[0]")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(1))
			Expect(*cmd2.Val()[0]).To(Equal(int64(0)))
		})

		It("should JSONType", Label("json.type", "json"), func() {
			cmd1 := client.JSONSet(ctx, "type1", "$", `[true]`)
			Expect(cmd1.Err()).NotTo(HaveOccurred())
			Expect(cmd1.Val()).To(Equal("OK"))

			cmd2 := client.JSONType(ctx, "type1", "$[0]")
			Expect(cmd2.Err()).NotTo(HaveOccurred())
			Expect(cmd2.Val()).To(HaveLen(1))
			// RESP2 v RESP3
			Expect(cmd2.Val()[0]).To(Or(Equal([]interface{}{"boolean"}), Equal("boolean")))
		})
	})
})
