package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

func WaitForIndexing(c *redis.Client, index string) {
	for {
		res, err := c.FTInfo(context.Background(), index).Result()
		Expect(err).NotTo(HaveOccurred())
		if res["indexing"].(float64) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var _ = Describe("RediSearch commands", Label("search"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379"})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should FTCreate and FTSearch WithScores ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo baz")
		client.HSet(ctx, "doc2", "txt", "foo bar")
		res, err := client.FTSearchWithArgs(ctx, "txt", "foo ~bar", &redis.FTSearchOptions{WithScores: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult := res.(map[interface{}]interface{})
		Expect(searchResult["total_results"]).To(BeEquivalentTo(int64(2)))
		Expect(searchResult["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))
		Expect(searchResult["results"].([]interface{})[0].(map[interface{}]interface{})["score"]).To(BeEquivalentTo(float64(3.0)))
		Expect(searchResult["results"].([]interface{})[1].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
	})

	It("should FTCreate and FTSearch stopwords ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{StopWords: []interface{}{"foo", "bar", "baz"}}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo baz")
		client.HSet(ctx, "doc2", "txt", "hello world")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "foo bar", &redis.FTSearchOptions{NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1["total_results"]).To(BeEquivalentTo(int64(0)))
		res2, err := client.FTSearchWithArgs(ctx, "txt", "foo bar hello world", &redis.FTSearchOptions{NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult2 := res2.(map[interface{}]interface{})
		Expect(searchResult2["total_results"]).To(BeEquivalentTo(int64(1)))

	})

	It("should FTCreate and FTSearch filters ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT"}, &redis.FieldSchema{FieldName: "num", FieldType: "NUMERIC"}, &redis.FieldSchema{FieldName: "loc", FieldType: "GEO"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo bar", "num", 3.141, "loc", "-0.441,51.458")
		client.HSet(ctx, "doc2", "txt", "foo baz", "num", 2, "loc", "-0.1,51.2")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{Filters: []redis.FTSearchFilter{{FieldName: "num", Min: 0, Max: 2}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1["total_results"]).To(BeEquivalentTo(int64(1)))
		Expect(searchResult1["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))
		res2, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{Filters: []redis.FTSearchFilter{{FieldName: "num", Min: 0, Max: "+inf"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult2 := res2.(map[interface{}]interface{})
		Expect(searchResult2["total_results"]).To(BeEquivalentTo(int64(2)))
		Expect(searchResult2["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		// Test Geo filter
		geoFilter1 := redis.FTSearchGeoFilter{FieldName: "loc", Longitude: -0.44, Latitude: 51.45, Radius: 10, Unit: "km"}
		geoFilter2 := redis.FTSearchGeoFilter{FieldName: "loc", Longitude: -0.44, Latitude: 51.45, Radius: 100, Unit: "km"}
		res3, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{GeoFilter: []redis.FTSearchGeoFilter{geoFilter1}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult3 := res3.(map[interface{}]interface{})
		Expect(searchResult3["total_results"]).To(BeEquivalentTo(int64(1)))
		Expect(searchResult3["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		res4, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{GeoFilter: []redis.FTSearchGeoFilter{geoFilter2}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult4 := res4.(map[interface{}]interface{})
		Expect(searchResult4["total_results"]).To(BeEquivalentTo(int64(2)))
		docs := []interface{}{searchResult4["results"].([]interface{})[0].(map[interface{}]interface{})["id"], searchResult4["results"].([]interface{})[1].(map[interface{}]interface{})["id"]}
		Expect(docs).To(ContainElement("doc1"))
		Expect(docs).To(ContainElement("doc2"))

	})

	It("should FTCreate and FTSearch sortby ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "num", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT"}, &redis.FieldSchema{FieldName: "num", FieldType: "NUMERIC", Sortable: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "num")
		client.HSet(ctx, "doc1", "txt", "foo bar", "num", 1)
		client.HSet(ctx, "doc2", "txt", "foo baz", "num", 2)
		client.HSet(ctx, "doc3", "txt", "foo qux", "num", 3)

		sortBy1 := redis.FTSearchSortBy{FieldName: "num", Asc: true}
		sortBy2 := redis.FTSearchSortBy{FieldName: "num", Desc: true}
		res1, err := client.FTSearchWithArgs(ctx, "num", "foo", &redis.FTSearchOptions{NoContent: true, SortBy: []redis.FTSearchSortBy{sortBy1}}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1["total_results"]).To(BeEquivalentTo(int64(3)))
		Expect(searchResult1["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		Expect(searchResult1["results"].([]interface{})[1].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))
		Expect(searchResult1["results"].([]interface{})[2].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc3"))

		res2, err := client.FTSearchWithArgs(ctx, "num", "foo", &redis.FTSearchOptions{NoContent: true, SortBy: []redis.FTSearchSortBy{sortBy2}}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult2 := res2.(map[interface{}]interface{})
		Expect(searchResult2["total_results"]).To(BeEquivalentTo(int64(3)))
		Expect(searchResult2["results"].([]interface{})[2].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		Expect(searchResult2["results"].([]interface{})[1].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))
		Expect(searchResult2["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc3"))

	})

	It("should FTCreate and FTSearch example ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "title", FieldType: "TEXT", Weight: 5}, &redis.FieldSchema{FieldName: "body", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "title", "RediSearch", "body", "Redisearch impements a search engine on top of redis")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "search engine", &redis.FTSearchOptions{NoContent: true, Verbatim: true, LimitOffset: 0, Limit: 5}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1).ToNot(BeEmpty())

	})

	It("should FTCreate NoIndex ", Label("search", "ftcreate", "ftsearch"), func() {
		text1 := &redis.FieldSchema{FieldName: "field", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "text", FieldType: "TEXT", NoIndex: true, Sortable: true}
		num := &redis.FieldSchema{FieldName: "numeric", FieldType: "NUMERIC", NoIndex: true, Sortable: true}
		geo := &redis.FieldSchema{FieldName: "geo", FieldType: "GEO", NoIndex: true, Sortable: true}
		tag := &redis.FieldSchema{FieldName: "tag", FieldType: "TAG", NoIndex: true, Sortable: true}
		val, err := client.FTCreate(ctx, "idx", &redis.FTCreateOptions{}, text1, text2, num, geo, tag).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx")
		client.HSet(ctx, "doc1", "field", "aaa", "text", "1", "numeric", 1, "geo", "1,1", "tag", "1")
		client.HSet(ctx, "doc2", "field", "aab", "text", "2", "numeric", 2, "geo", "2,2", "tag", "2")
		res1, err := client.FTSearch(ctx, "idx", "@text:aa*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(0)))
		res2, err := client.FTSearch(ctx, "idx", "@field:aa*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(2)))
		res3, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "text", Desc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(2)))
		Expect(res3.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))
		res4, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "text", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res4.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(2)))
		Expect(res4.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		res5, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "numeric", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res5.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		res6, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "geo", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res6.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		res7, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "tag", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res7.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))

	})

	It("should FTExplain ", Label("search", "ftexplain"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: "TEXT"}
		text3 := &redis.FieldSchema{FieldName: "f3", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, text1, text2, text3).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		res1, err := client.FTExplain(ctx, "txt", "@f3:f3_val @f2:f2_val @f1:f1_val").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1).ToNot(BeEmpty())

	})

	It("should FTAlias ", Label("search", "ftexplain"), func() {
		text1 := &redis.FieldSchema{FieldName: "name", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "name", FieldType: "TEXT"}
		val1, err := client.FTCreate(ctx, "testAlias", &redis.FTCreateOptions{Prefix: []interface{}{"index1:"}}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val1).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "testAlias")
		val2, err := client.FTCreate(ctx, "testAlias2", &redis.FTCreateOptions{Prefix: []interface{}{"index2:"}}, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val2).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "testAlias2")

		client.HSet(ctx, "index1:lonestar", "name", "lonestar")
		client.HSet(ctx, "index2:yogurt", "name", "yogurt")

		res1, err := client.FTSearch(ctx, "testAlias", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("index1:lonestar"))

		aliasAddRes, err := client.FTAliasAdd(ctx, "testAlias", "mj23").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasAddRes).To(BeEquivalentTo("OK"))

		res1, err = client.FTSearch(ctx, "mj23", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("index1:lonestar"))

		aliasUpdateRes, err := client.FTAliasUpdate(ctx, "testAlias2", "kb24").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasUpdateRes).To(BeEquivalentTo("OK"))

		res3, err := client.FTSearch(ctx, "kb24", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("index2:yogurt"))

		aliasDelRes, err := client.FTAliasDel(ctx, "mj23").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasDelRes).To(BeEquivalentTo("OK"))

	})

	It("should FTCreate and FTSearch textfield, sortable and nostem ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT", Sortable: true, NoStem: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resInfo, err := client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resInfo["attributes"].([]interface{})[0].(map[interface{}]interface{})["flags"]).To(ContainElements("SORTABLE", "NOSTEM"))

	})

	It("should FTAlter ", Label("search", "ftcreate", "ftsearch", "ftalter"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resAlter, err := client.FTAlter(ctx, "idx1", false, []interface{}{"body", "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resAlter).To(BeEquivalentTo("OK"))

		client.HSet(ctx, "doc1", "title", "MyTitle", "body", "Some content only in the body")
		res1, err := client.FTSearch(ctx, "idx1", "only in the body").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(1)))

	})

	It("should FTSpellCheck", Label("search", "ftcreate", "ftsearch", "ftspellcheck"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "f1", "some valid content", "f2", "this is sample text")
		client.HSet(ctx, "doc2", "f1", "very important", "f2", "lorem ipsum")

		resSpellCheck, err := client.FTSpellCheck(ctx, "idx1", "impornant").Result()
		Expect(err).NotTo(HaveOccurred())
		res := resSpellCheck["results"].(map[interface{}]interface{})["impornant"].([]interface{})[0].(map[interface{}]interface{})
		Expect("important").To(BeKeyOf(res))

		resSpellCheck2, err := client.FTSpellCheck(ctx, "idx1", "contnt").Result()
		Expect(err).NotTo(HaveOccurred())
		res2 := resSpellCheck2["results"].(map[interface{}]interface{})["contnt"].([]interface{})[0].(map[interface{}]interface{})
		Expect("content").To(BeKeyOf(res2))

		// test spellcheck with Levenshtein distance
		resSpellCheck3, err := client.FTSpellCheck(ctx, "idx1", "vlis").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck3["results"]).To(BeEquivalentTo(map[interface{}]interface{}{"vlis": []interface{}{}}))

		resSpellCheck4, err := client.FTSpellCheckWithArgs(ctx, "idx1", "vlis", &redis.FTSpellCheckOptions{Distance: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect("valid").To(BeKeyOf(resSpellCheck4["results"].(map[interface{}]interface{})["vlis"].([]interface{})[0].(map[interface{}]interface{})))

		// test spellcheck include
		dict := []interface{}{"lore", "lorem", "lorm"}
		resDictAdd, err := client.FTDictAdd(ctx, "dict", dict).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictAdd).To(BeEquivalentTo(3))
		terms := redis.SpellCheckTerms{Include: true, Dictionary: "dict"}
		resSpellCheck5, err := client.FTSpellCheckWithArgs(ctx, "idx1", "lorm", &redis.FTSpellCheckOptions{Terms: terms}).Result()
		Expect(err).NotTo(HaveOccurred())
		lorm := resSpellCheck5["results"].(map[interface{}]interface{})["lorm"].([]interface{})
		Expect(len(lorm)).To(BeEquivalentTo(3))
		Expect(lorm[0].(map[interface{}]interface{})["lorem"]).To(BeEquivalentTo(0.5))
		Expect(lorm[1].(map[interface{}]interface{})["lore"]).To(BeEquivalentTo(0))
		Expect(lorm[2].(map[interface{}]interface{})["lorm"]).To(BeEquivalentTo(0))

		terms2 := redis.SpellCheckTerms{Exclude: true, Dictionary: "dict"}
		resSpellCheck6, err := client.FTSpellCheckWithArgs(ctx, "idx1", "lorm", &redis.FTSpellCheckOptions{Terms: terms2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck6["results"]).To(BeEmpty())
	})

	It("should FTDict opreations ", Label("search", "ftdictdump", "ftdictdel", "ftdictadd"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		dict := []interface{}{"item1", "item2", "item3"}
		resDictAdd, err := client.FTDictAdd(ctx, "custom_dict", dict).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictAdd).To(BeEquivalentTo(3))

		resDictDel, err := client.FTDictDel(ctx, "custom_dict", []interface{}{"item2"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDel).To(BeEquivalentTo(1))

		resDictDump, err := client.FTDictDump(ctx, "custom_dict").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDump).To(BeEquivalentTo([]string{"item1", "item3"}))

		resDictDel2, err := client.FTDictDel(ctx, "custom_dict", []interface{}{"item1", "item3"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDel2).To(BeEquivalentTo(2))
	})

	It("should FTSearch phonetic matcher ", Label("search", "ftsearch"), func() {
		text1 := &redis.FieldSchema{FieldName: "name", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Jon")
		client.HSet(ctx, "doc2", "name", "John")

		res1, err := client.FTSearch(ctx, "idx1", "Jon").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(1)))
		name := res1.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})["name"]
		Expect(name).To(BeEquivalentTo("Jon"))

		client.FlushDB(ctx)
		text2 := &redis.FieldSchema{FieldName: "name", FieldType: "TEXT", PhoneticMatcher: "dm:en"}
		val2, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val2).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Jon")
		client.HSet(ctx, "doc2", "name", "John")

		res2, err := client.FTSearch(ctx, "idx1", "Jon").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(2)))
		results2 := res2.(map[interface{}]interface{})["results"].([]interface{})
		n1 := results2[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})["name"]
		n2 := results2[1].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})["name"]
		names := []interface{}{n1, n2}
		Expect(names).To(ContainElement("Jon"))
		Expect(names).To(ContainElement("John"))
	})

	It("should FTSearch WithScores", Label("search", "ftsearch"), func() {
		text1 := &redis.FieldSchema{FieldName: "description", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "description", "The quick brown fox jumps over the lazy dog")
		client.HSet(ctx, "doc2", "description", "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do.")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		result := res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(1))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "TFIDF"}).Result()
		Expect(err).NotTo(HaveOccurred())
		result = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(1))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "TFIDF.DOCNORM"}).Result()
		Expect(err).NotTo(HaveOccurred())
		result = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(0.14285714285714285))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "BM25"}).Result()
		Expect(err).NotTo(HaveOccurred())
		result = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(0.22471909420069797))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "DISMAX"}).Result()
		Expect(err).NotTo(HaveOccurred())
		result = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(2))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "DOCSCORE"}).Result()
		Expect(err).NotTo(HaveOccurred())
		result = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(1))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "HAMMING"}).Result()
		Expect(err).NotTo(HaveOccurred())
		result = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(0))
	})

	It("should FTConfigSet and FTConfigGet ", Label("search", "ftconfigget", "ftconfigset"), func() {
		val, err := client.FTConfigSet(ctx, "TIMEOUT", "100").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		res, err := client.FTConfigGet(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res["TIMEOUT"]).To(BeEquivalentTo("100"))

		res, err = client.FTConfigGet(ctx, "TIMEOUT").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo(map[string]interface{}{"TIMEOUT": "100"}))

	})

	It("should FTAggregate GroupBy ", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "title", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "body", FieldType: "TEXT"}
		text3 := &redis.FieldSchema{FieldName: "parent", FieldType: "TEXT"}
		num := &redis.FieldSchema{FieldName: "random_num", FieldType: "NUMERIC"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2, text3, num).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "search", "title", "RediSearch",
			"body", "Redisearch impements a search engine on top of redis",
			"parent", "redis",
			"random_num", 10)
		client.HSet(ctx, "ai", "title", "RedisAI",
			"body", "RedisAI executes Deep Learning/Machine Learning models and managing their data.",
			"parent", "redis",
			"random_num", 3)
		client.HSet(ctx, "json", "title", "RedisJson",
			"body", "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",
			"parent", "redis",
			"random_num", 8)

		reducer := redis.FTAggregateReducer{Reducer: redis.SearchCount}
		options := &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr := res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliascount"]).To(BeEquivalentTo("3"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchCountDistinct, Args: []interface{}{"@title"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliascount_distincttitle"]).To(BeEquivalentTo("3"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchSum, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliassumrandom_num"]).To(BeEquivalentTo("21"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchMin, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliasminrandom_num"]).To(BeEquivalentTo("3"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchMax, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliasmaxrandom_num"]).To(BeEquivalentTo("10"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchAvg, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliasavgrandom_num"]).To(BeEquivalentTo("7"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchStdDev, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliasstddevrandom_num"]).To(BeEquivalentTo("3.60555127546"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchQuantile, Args: []interface{}{"@random_num", 0.5}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliasquantilerandom_num,0.5"]).To(BeEquivalentTo("8"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchToList, Args: []interface{}{"@title"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["__generated_aliastolisttitle"].([]interface{})).To(ContainElements("RediSearch", "RedisAI", "RedisJson"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchFirstValue, Args: []interface{}{"@title"}, As: "first"}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(extraAttr["first"]).To(BeEquivalentTo("RediSearch"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchRandomSample, Args: []interface{}{"@title", 2}, As: "random"}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr["parent"]).To(BeEquivalentTo("redis"))
		Expect(len(extraAttr["random"].([]interface{}))).To(BeEquivalentTo(2))
		Expect(extraAttr["random"].([]interface{})[0]).To(BeElementOf([]string{"RediSearch", "RedisAI", "RedisJson"}))

	})

	It("should FTAggregate sort and limit ", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "t1", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "t2", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "t1", "a", "t2", "b")
		client.HSet(ctx, "doc2", "t1", "b", "t2", "a")

		options := &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t2", Asc: true}, {FieldName: "@t1", Desc: true}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr0 := res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		extraAttr1 := res["results"].([]interface{})[1].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"t1": "b", "t2": "a"}))
		Expect(extraAttr1).To(BeEquivalentTo(map[interface{}]interface{}{"t1": "a", "t2": "b"}))

		options = &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t1"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr0 = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		extraAttr1 = res["results"].([]interface{})[1].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"t1": "a"}))
		Expect(extraAttr1).To(BeEquivalentTo(map[interface{}]interface{}{"t1": "b"}))

		options = &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t1"}}, SortByMax: 1}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		results := res["results"].([]interface{})
		Expect(len(results)).To(BeEquivalentTo(1))

		options = &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t1"}}, Limit: 1, LimitOffset: 1}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		results = res["results"].([]interface{})
		extraAttr0 = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(len(results)).To(BeEquivalentTo(1))
		Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"t1": "b"}))
	})

	It("should FTAggregate load ", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "t1", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "t2", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "t1", "hello", "t2", "world")

		options := &redis.FTAggregateOptions{Load: []redis.FTAggregateLoad{{Field: "t1"}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr0 := res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"t1": "hello"}))

		options = &redis.FTAggregateOptions{Load: []redis.FTAggregateLoad{{Field: "t2"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr0 = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"t2": "world"}))

		options = &redis.FTAggregateOptions{LoadAll: true}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr0 = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"t2": "world", "t1": "hello"}))
	})

	It("should FTAggregate apply", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "PrimaryKey", FieldType: "TEXT", Sortable: true}
		num1 := &redis.FieldSchema{FieldName: "CreatedDateTimeUTC", FieldType: "NUMERIC", Sortable: true}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "PrimaryKey", "9::362330", "CreatedDateTimeUTC", "637387878524969984")
		client.HSet(ctx, "doc2", "PrimaryKey", "9::362329", "CreatedDateTimeUTC", "637387875859270016")

		options := &redis.FTAggregateOptions{Apply: []redis.FTAggregateApply{{Field: "@CreatedDateTimeUTC * 10", As: "CreatedDateTimeUTC"}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		extraAttr0 := res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		extraAttr1 := res["results"].([]interface{})[1].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
		Expect([]interface{}{extraAttr0["CreatedDateTimeUTC"], extraAttr1["CreatedDateTimeUTC"]}).To(ContainElements("6373878785249699840", "6373878758592700416"))

	})

	It("should FTAggregate filter", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "name", FieldType: "TEXT", Sortable: true}
		num1 := &redis.FieldSchema{FieldName: "age", FieldType: "NUMERIC", Sortable: true}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "bar", "age", "25")
		client.HSet(ctx, "doc2", "name", "foo", "age", "19")

		for _, dlc := range []int{1, 2} {
			options := &redis.FTAggregateOptions{Filter: "@name=='foo' && @age < 20", DialectVersion: dlc}
			res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res["results"].([]interface{}))).To(BeEquivalentTo(1))
			extraAttr0 := res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
			Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"name": "foo", "age": "19"}))

			options = &redis.FTAggregateOptions{Filter: "@age > 15", DialectVersion: dlc, SortBy: []redis.FTAggregateSortBy{{FieldName: "@age"}}}
			res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res["results"].([]interface{}))).To(BeEquivalentTo(2))
			extraAttr0 = res["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
			extraAttr1 := res["results"].([]interface{})[1].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})
			Expect(extraAttr0).To(BeEquivalentTo(map[interface{}]interface{}{"age": "19"}))
			Expect(extraAttr1).To(BeEquivalentTo(map[interface{}]interface{}{"age": "25"}))
		}

	})

	It("should FTSearch SkipInitalScan", Label("search", "ftsearch"), func() {
		client.HSet(ctx, "doc1", "foo", "bar")

		text1 := &redis.FieldSchema{FieldName: "foo", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{SkipInitalScan: true}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTSearch(ctx, "idx1", "@foo:bar").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(0)))
	})

	It("should FTCreate json", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: "$.name", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry"}`)
		client.JSONSet(ctx, "king:2", "$", `{"name": "james"}`)

		res, err := client.FTSearch(ctx, "idx1", "henry").Result()
		Expect(err).NotTo(HaveOccurred())
		totalResults := res.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(1))
		results0 := res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("king:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["$"]).To(BeEquivalentTo(`{"name":"henry"}`))
	})

	It("should FTCreate json fields as names", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: "$.name", FieldType: "TEXT", As: "name"}
		num1 := &redis.FieldSchema{FieldName: "$.age", FieldType: "NUMERIC", As: "just_a_number"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "doc:1", "$", `{"name": "Jon", "age": 25}`)

		res, err := client.FTSearchWithArgs(ctx, "idx1", "Jon", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "name"}, {FieldName: "just_a_number"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		results := res.(map[interface{}]interface{})["results"].([]interface{})
		Expect(len(results)).To(BeEquivalentTo(1))
		results0 := results[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("doc:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["name"]).To(BeEquivalentTo("Jon"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["just_a_number"]).To(BeEquivalentTo("25"))
	})

	It("should FTCreate CaseSensitive", Label("search", "ftcreate"), func() {

		tag1 := &redis.FieldSchema{FieldName: "t", FieldType: "TAG", CaseSensitive: false}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, tag1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "1", "t", "HELLO")
		client.HSet(ctx, "2", "t", "hello")

		res, err := client.FTSearch(ctx, "idx1", "@t:{HELLO}").Result()
		Expect(err).NotTo(HaveOccurred())
		results := res.(map[interface{}]interface{})["results"].([]interface{})
		Expect(len(results)).To(BeEquivalentTo(2))
		results0 := results[0].(map[interface{}]interface{})
		results1 := results[1].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("1"))
		Expect(results1["id"]).To(BeEquivalentTo("2"))

		res, err = client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo("OK"))

		tag2 := &redis.FieldSchema{FieldName: "t", FieldType: "TAG", CaseSensitive: true}
		val, err = client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, tag2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTSearch(ctx, "idx1", "@t:{HELLO}").Result()
		Expect(err).NotTo(HaveOccurred())
		results = res.(map[interface{}]interface{})["results"].([]interface{})
		Expect(len(results)).To(BeEquivalentTo(1))
		results0 = results[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("1"))

	})

	It("should FTSearch ReturnFields", Label("search", "ftsearch"), func() {
		resJson, err := client.JSONSet(ctx, "doc:1", "$", `{"t": "riceratops","t2": "telmatosaurus", "n": 9072, "flt": 97.2}`).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resJson).To(BeEquivalentTo("OK"))

		text1 := &redis.FieldSchema{FieldName: "$.t", FieldType: "TEXT"}
		num1 := &redis.FieldSchema{FieldName: "$.flt", FieldType: "NUMERIC"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "*", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "$.t", As: "txt"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		results := res.(map[interface{}]interface{})["results"].([]interface{})
		Expect(len(results)).To(BeEquivalentTo(1))
		results0 := results[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("doc:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["txt"]).To(BeEquivalentTo("riceratops"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "*", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "$.t2", As: "txt"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		results = res.(map[interface{}]interface{})["results"].([]interface{})
		Expect(len(results)).To(BeEquivalentTo(1))
		results0 = results[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("doc:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["txt"]).To(BeEquivalentTo("telmatosaurus"))
	})

	It("should FTSynUpdate", Label("search", "ftsynupdate"), func() {

		text1 := &redis.FieldSchema{FieldName: "title", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "body", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnHash: true}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resSynUpdate, err := client.FTSynUpdateWithArgs(ctx, "idx1", "id1", &redis.FTSynUpdateOptions{SkipInitialScan: true}, []interface{}{"boy", "child", "offspring"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))
		client.HSet(ctx, "doc1", "title", "he is a baby", "body", "this is a test")

		resSynUpdate, err = client.FTSynUpdateWithArgs(ctx, "idx1", "id1", &redis.FTSynUpdateOptions{SkipInitialScan: true}, []interface{}{"baby"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))
		client.HSet(ctx, "doc2", "title", "he is another baby", "body", "another test")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "child", &redis.FTSearchOptions{Expander: "SYNONYM"}).Result()
		Expect(err).NotTo(HaveOccurred())
		results0 := res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("doc2"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["title"]).To(BeEquivalentTo("he is another baby"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["body"]).To(BeEquivalentTo("another test"))
	})

	It("should FTSynDump", Label("search", "ftsyndump"), func() {

		text1 := &redis.FieldSchema{FieldName: "title", FieldType: "TEXT"}
		text2 := &redis.FieldSchema{FieldName: "body", FieldType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnHash: true}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resSynUpdate, err := client.FTSynUpdate(ctx, "idx1", "id1", []interface{}{"boy", "child", "offspring"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))

		resSynUpdate, err = client.FTSynUpdate(ctx, "idx1", "id1", []interface{}{"baby", "child"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))

		resSynUpdate, err = client.FTSynUpdate(ctx, "idx1", "id1", []interface{}{"tree", "wood"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))

		resSynDump, err := client.FTSynDump(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynDump).To(BeEquivalentTo(map[string][]interface{}{
			"offspring": {"id1"},
			"baby":      {"id1"},
			"wood":      {"id1"},
			"boy":       {"id1"},
			"tree":      {"id1"},
			"child":     {"id1", "id1"}}))
	})

	It("should FTCreate json with alias", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: "$.name", FieldType: "TEXT", As: "name"}
		num1 := &redis.FieldSchema{FieldName: "$.num", FieldType: "NUMERIC", As: "num"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry", "num": 42}`)
		client.JSONSet(ctx, "king:2", "$", `{"name": "james", "num": 3.14}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:henry").Result()
		Expect(err).NotTo(HaveOccurred())
		totalResults := res.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(1))
		results0 := res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("king:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["$"]).To(BeEquivalentTo(`{"name":"henry","num":42}`))

		res, err = client.FTSearch(ctx, "idx1", "@num:[0 10]").Result()
		Expect(err).NotTo(HaveOccurred())
		totalResults = res.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(1))
		results0 = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("king:2"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["$"]).To(BeEquivalentTo(`{"name":"james","num":3.14}`))
	})

	It("should FTCreate json with multipath", Label("search", "ftcreate"), func() {

		tag1 := &redis.FieldSchema{FieldName: "$..name", FieldType: "TAG", As: "name"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, tag1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry", "country": {"name": "england"}}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:{england}").Result()
		Expect(err).NotTo(HaveOccurred())
		totalResults := res.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(1))
		results0 := res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("king:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["$"]).To(BeEquivalentTo(`{"name":"henry","country":{"name":"england"}}`))
	})

	It("should FTCreate json with jsonpath", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: `$["prod:name"]`, FieldType: "TEXT", As: "name"}
		text2 := &redis.FieldSchema{FieldName: `$.prod:name`, FieldType: "TEXT", As: "name_unsupported"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "doc:1", "$", `{"prod:name": "RediSearch"}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:RediSearch").Result()
		Expect(err).NotTo(HaveOccurred())
		totalResults := res.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(1))
		results0 := res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("doc:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["$"]).To(BeEquivalentTo(`{"prod:name":"RediSearch"}`))

		res, err = client.FTSearch(ctx, "idx1", "@name_unsupported:RediSearch").Result()
		Expect(err).NotTo(HaveOccurred())
		totalResults = res.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(1))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "@name:RediSearch", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "name"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		totalResults = res.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(1))
		results0 = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("doc:1"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["name"]).To(BeEquivalentTo("RediSearch"))

	})

	It("should FTProfile Search and Aggregate", Label("search", "ftprofile"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "t", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "1", "t", "hello")
		client.HSet(ctx, "2", "t", "world")

		// FTProfile Search
		query := redis.FTSearchQuery("hello|world", &redis.FTSearchOptions{NoContent: true})
		res1, err := client.FTProfile(ctx, "idx1", false, query).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(res1["results"].([]interface{}))).To(BeEquivalentTo(2))
		resProfile := res1["profile"].(map[interface{}]interface{})
		Expect(resProfile["Parsing time"].(float64) < 0.5).To(BeTrue())
		iterProfile0 := resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
		Expect(iterProfile0["Counter"]).To(BeEquivalentTo(2.0))
		Expect(iterProfile0["Type"]).To(BeEquivalentTo("UNION"))

		// FTProfile Aggregate
		aggQuery := redis.FTAggregateQuery("*", &redis.FTAggregateOptions{
			Load:  []redis.FTAggregateLoad{{Field: "t"}},
			Apply: []redis.FTAggregateApply{{Field: "startswith(@t, 'hel')", As: "prefix"}}})
		res2, err := client.FTProfile(ctx, "idx1", false, aggQuery).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(res2["results"].([]interface{}))).To(BeEquivalentTo(2))
		resProfile = res2["profile"].(map[interface{}]interface{})
		iterProfile0 = resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
		Expect(iterProfile0["Counter"]).To(BeEquivalentTo(2))
		Expect(iterProfile0["Type"]).To(BeEquivalentTo("WILDCARD"))
	})

	It("should FTProfile Search Limited", Label("search", "ftprofile"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "t", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "1", "t", "hello")
		client.HSet(ctx, "2", "t", "hell")
		client.HSet(ctx, "3", "t", "help")
		client.HSet(ctx, "4", "t", "helowa")

		// FTProfile Search
		query := redis.FTSearchQuery("%hell% hel*", &redis.FTSearchOptions{})
		res1, err := client.FTProfile(ctx, "idx1", true, query).Result()
		Expect(err).NotTo(HaveOccurred())
		resProfile := res1["profile"].(map[interface{}]interface{})
		iterProfile0 := resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
		Expect(iterProfile0["Type"]).To(BeEquivalentTo("INTERSECT"))
		Expect(len(res1["results"].([]interface{}))).To(BeEquivalentTo(3))
		Expect(iterProfile0["Child iterators"].([]interface{})[0].(map[interface{}]interface{})["Child iterators"]).To(BeEquivalentTo("The number of iterators in the union is 3"))
		Expect(iterProfile0["Child iterators"].([]interface{})[1].(map[interface{}]interface{})["Child iterators"]).To(BeEquivalentTo("The number of iterators in the union is 4"))
	})

	It("should FTProfile Search query params", Label("search", "ftprofile"), func() {
		hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
		val, err := client.FTCreate(ctx, "idx1",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "v", FieldType: "VECTOR", VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "a", "v", "aaaaaaaa")
		client.HSet(ctx, "b", "v", "aaaabaaa")
		client.HSet(ctx, "c", "v", "aaaaabaa")

		// FTProfile Search
		searchOptions := &redis.FTSearchOptions{
			Return:         []redis.FTSearchReturn{{FieldName: "__v_score"}},
			SortBy:         []redis.FTSearchSortBy{{FieldName: "__v_score", Asc: true}},
			DialectVersion: 2,
			Params:         map[string]interface{}{"vec": "aaaaaaaa"},
		}
		query := redis.FTSearchQuery("*=>[KNN 2 @v $vec]", searchOptions)
		res1, err := client.FTProfile(ctx, "idx1", false, query).Result()
		Expect(err).NotTo(HaveOccurred())
		resProfile := res1["profile"].(map[interface{}]interface{})
		iterProfile0 := resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
		Expect(iterProfile0["Counter"]).To(BeEquivalentTo(2))
		Expect(iterProfile0["Type"]).To(BeEquivalentTo("VECTOR"))
		Expect(res1["total_results"]).To(BeEquivalentTo(2))
		results0 := res1["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("a"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["__v_score"]).To(BeEquivalentTo("0"))
	})

	It("should FTCreate VECTOR", Label("search", "ftcreate"), func() {
		hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
		val, err := client.FTCreate(ctx, "idx1",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "v", FieldType: "VECTOR", VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "a", "v", "aaaaaaaa")
		client.HSet(ctx, "b", "v", "aaaabaaa")
		client.HSet(ctx, "c", "v", "aaaaabaa")

		searchOptions := &redis.FTSearchOptions{
			Return:         []redis.FTSearchReturn{{FieldName: "__v_score"}},
			SortBy:         []redis.FTSearchSortBy{{FieldName: "__v_score", Asc: true}},
			DialectVersion: 2,
			Params:         map[string]interface{}{"vec": "aaaaaaaa"},
		}
		res, err := client.FTSearchWithArgs(ctx, "idx1", "*=>[KNN 2 @v $vec]", searchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		res1 := res.(map[interface{}]interface{})
		results0 := res1["results"].([]interface{})[0].(map[interface{}]interface{})
		Expect(results0["id"]).To(BeEquivalentTo("a"))
		Expect(results0["extra_attributes"].(map[interface{}]interface{})["__v_score"]).To(BeEquivalentTo("0"))
	})

	It("should FTCreate and FTSearch text params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "name", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Alice")
		client.HSet(ctx, "doc2", "name", "Bob")
		client.HSet(ctx, "doc3", "name", "Carol")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@name:($name1 | $name2 )", &redis.FTSearchOptions{Params: map[string]interface{}{"name1": "Alice", "name2": "Bob"}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1["total_results"]).To(BeEquivalentTo(int64(2)))
		Expect(searchResult1["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		Expect(searchResult1["results"].([]interface{})[1].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))

	})

	It("should FTCreate and FTSearch numeric params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "numval", FieldType: "NUMERIC"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "numval", 101)
		client.HSet(ctx, "doc2", "numval", 102)
		client.HSet(ctx, "doc3", "numval", 103)

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@numval:[$min $max]", &redis.FTSearchOptions{Params: map[string]interface{}{"min": 101, "max": 102}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1["total_results"]).To(BeEquivalentTo(int64(2)))
		Expect(searchResult1["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		Expect(searchResult1["results"].([]interface{})[1].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))

	})

	It("should FTCreate and FTSearch geo params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "g", FieldType: "GEO"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "g", "29.69465, 34.95126")
		client.HSet(ctx, "doc2", "g", "29.69350, 34.94737")
		client.HSet(ctx, "doc3", "g", "29.68746, 34.94882")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@g:[$lon $lat $radius $units]", &redis.FTSearchOptions{Params: map[string]interface{}{"lat": "34.95126", "lon": "29.69465", "radius": 1000, "units": "km"}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1["total_results"]).To(BeEquivalentTo(int64(3)))
		Expect(searchResult1["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc1"))
		Expect(searchResult1["results"].([]interface{})[1].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc2"))
		Expect(searchResult1["results"].([]interface{})[2].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("doc3"))

	})

	It("should FTConfigSet and FTConfigGet dialect", Label("search", "ftconfigget", "ftconfigset"), func() {
		res, err := client.FTConfigSet(ctx, "DEFAULT_DIALECT", "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo("OK"))

		defDialect, err := client.FTConfigGet(ctx, "DEFAULT_DIALECT").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(defDialect).To(BeEquivalentTo(map[string]interface{}{"DEFAULT_DIALECT": "1"}))

		res, err = client.FTConfigSet(ctx, "DEFAULT_DIALECT", "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo("OK"))

		defDialect, err = client.FTConfigGet(ctx, "DEFAULT_DIALECT").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(defDialect).To(BeEquivalentTo(map[string]interface{}{"DEFAULT_DIALECT": "2"}))
	})

	It("should FTCreate WithSuffixtrie", Label("search", "ftcreate", "ftinfo"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		resAttr0 := res["attributes"].([]interface{})[0].(map[interface{}]interface{})
		Expect(resAttr0["flags"]).To(BeEmpty())

		resDrop, err := client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		// create withsuffixtrie index - text field
		val, err = client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: "TEXT", WithSuffixtrie: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		resAttr0 = res["attributes"].([]interface{})[0].(map[interface{}]interface{})
		Expect(resAttr0["flags"].([]interface{})).To(ContainElement("WITHSUFFIXTRIE"))

		resDrop, err = client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		// create withsuffixtrie index - tag field
		val, err = client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "t", FieldType: "TAG", WithSuffixtrie: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		resAttr0 = res["attributes"].([]interface{})[0].(map[interface{}]interface{})
		Expect(resAttr0["flags"].([]interface{})).To(ContainElement("WITHSUFFIXTRIE"))
	})

	It("should FTCreate GeoShape", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "geom", FieldType: "GEOSHAPE", GeoShapeFieldType: "FLAT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "small", "geom", "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))")
		client.HSet(ctx, "large", "geom", "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@geom:[WITHIN $poly]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"poly": "POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1["total_results"]).To(BeEquivalentTo(int64(1)))
		Expect(searchResult1["results"].([]interface{})[0].(map[interface{}]interface{})["id"]).To(BeEquivalentTo("small"))

		res2, err := client.FTSearchWithArgs(ctx, "idx1", "@geom:[CONTAINS $poly]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"poly": "POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult2 := res2.(map[interface{}]interface{})
		Expect(searchResult2["total_results"]).To(BeEquivalentTo(int64(2)))
	})
})
