package redis_test

import (
	"context"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

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
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.SearchSchema{Identifier: "txt", AttributeType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
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
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{StopWords: []interface{}{"foo", "bar", "baz"}}, &redis.SearchSchema{Identifier: "txt", AttributeType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
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
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.SearchSchema{Identifier: "txt", AttributeType: "TEXT"}, &redis.SearchSchema{Identifier: "num", AttributeType: "NUMERIC"}, &redis.SearchSchema{Identifier: "loc", AttributeType: "GEO"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
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
		val, err := client.FTCreate(ctx, "num", &redis.FTCreateOptions{}, &redis.SearchSchema{Identifier: "txt", AttributeType: "TEXT"}, &redis.SearchSchema{Identifier: "num", AttributeType: "NUMERIC", Sortable: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
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
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.SearchSchema{Identifier: "title", AttributeType: "TEXT", Weight: 5}, &redis.SearchSchema{Identifier: "body", AttributeType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		client.HSet(ctx, "doc1", "title", "RediSearch", "body", "Redisearch impements a search engine on top of redis")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "search engine", &redis.FTSearchOptions{NoContent: true, Verbatim: true, LimitOffset: 0, Limit: 5}).Result()
		Expect(err).NotTo(HaveOccurred())
		searchResult1 := res1.(map[interface{}]interface{})
		Expect(searchResult1).ToNot(BeEmpty())

	})

	It("should FTCreate NoIndex ", Label("search", "ftcreate", "ftsearch"), func() {
		text1 := &redis.SearchSchema{Identifier: "field", AttributeType: "TEXT"}
		text2 := &redis.SearchSchema{Identifier: "text", AttributeType: "TEXT", NoIndex: true, Sortable: true}
		num := &redis.SearchSchema{Identifier: "numeric", AttributeType: "NUMERIC", NoIndex: true, Sortable: true}
		geo := &redis.SearchSchema{Identifier: "geo", AttributeType: "GEO", NoIndex: true, Sortable: true}
		tag := &redis.SearchSchema{Identifier: "tag", AttributeType: "TAG", NoIndex: true, Sortable: true}
		val, err := client.FTCreate(ctx, "idx", &redis.FTCreateOptions{}, text1, text2, num, geo, tag).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
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
		text1 := &redis.SearchSchema{Identifier: "f1", AttributeType: "TEXT"}
		text2 := &redis.SearchSchema{Identifier: "f2", AttributeType: "TEXT"}
		text3 := &redis.SearchSchema{Identifier: "f3", AttributeType: "TEXT"}
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, text1, text2, text3).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		res1, err := client.FTExplain(ctx, "txt", "@f3:f3_val @f2:f2_val @f1:f1_val").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1).ToNot(BeEmpty())

	})

	It("should FTAlias ", Label("search", "ftexplain"), func() {
		text1 := &redis.SearchSchema{Identifier: "name", AttributeType: "TEXT"}
		text2 := &redis.SearchSchema{Identifier: "name", AttributeType: "TEXT"}
		val1, err := client.FTCreate(ctx, "testAlias", &redis.FTCreateOptions{Prefix: []interface{}{"index1:"}}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val1).To(BeEquivalentTo("OK"))
		val2, err := client.FTCreate(ctx, "testAlias2", &redis.FTCreateOptions{Prefix: []interface{}{"index2:"}}, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val2).To(BeEquivalentTo("OK"))

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
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.SearchSchema{Identifier: "txt", AttributeType: "TEXT", Sortable: true, NoStem: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		resInfo, err := client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resInfo["attributes"].([]interface{})[0].(map[interface{}]interface{})["flags"]).To(ContainElements("SORTABLE", "NOSTEM"))

	})

	It("should FTAlter ", Label("search", "ftcreate", "ftsearch", "ftalter"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.SearchSchema{Identifier: "txt", AttributeType: "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		resAlter, err := client.FTAlter(ctx, "idx1", false, []interface{}{"body", "TEXT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resAlter).To(BeEquivalentTo("OK"))

		client.HSet(ctx, "doc1", "title", "MyTitle", "body", "Some content only in the body")
		res1, err := client.FTSearch(ctx, "idx1", "only in the body").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(1)))

	})

	It("should FTSpellCheck", Label("search", "ftcreate", "ftsearch", "ftspellcheck"), func() {
		text1 := &redis.SearchSchema{Identifier: "f1", AttributeType: "TEXT"}
		text2 := &redis.SearchSchema{Identifier: "f2", AttributeType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

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
		text1 := &redis.SearchSchema{Identifier: "f1", AttributeType: "TEXT"}
		text2 := &redis.SearchSchema{Identifier: "f2", AttributeType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

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
		text1 := &redis.SearchSchema{Identifier: "name", AttributeType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		client.HSet(ctx, "doc1", "name", "Jon")
		client.HSet(ctx, "doc2", "name", "John")

		res1, err := client.FTSearch(ctx, "idx1", "Jon").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.(map[interface{}]interface{})["total_results"]).To(BeEquivalentTo(int64(1)))
		name := res1.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})["name"]
		Expect(name).To(BeEquivalentTo("Jon"))

		client.FlushDB(ctx)
		text2 := &redis.SearchSchema{Identifier: "name", AttributeType: "TEXT", PhoneticMatcher: "dm:en"}
		val2, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val2).To(BeEquivalentTo("OK"))

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
		text1 := &redis.SearchSchema{Identifier: "description", AttributeType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

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
		Expect(result).To(BeEquivalentTo(0.1111111111111111))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "BM25"}).Result()
		Expect(err).NotTo(HaveOccurred())
		result = res.(map[interface{}]interface{})["results"].([]interface{})[0].(map[interface{}]interface{})["score"]
		Expect(result).To(BeEquivalentTo(0.17699114465425977))

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
		text1 := &redis.SearchSchema{Identifier: "title", AttributeType: "TEXT"}
		text2 := &redis.SearchSchema{Identifier: "body", AttributeType: "TEXT"}
		text3 := &redis.SearchSchema{Identifier: "parent", AttributeType: "TEXT"}
		num := &redis.SearchSchema{Identifier: "random_num", AttributeType: "NUMERIC"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2, text3, num).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

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
		text1 := &redis.SearchSchema{Identifier: "t1", AttributeType: "TEXT"}
		text2 := &redis.SearchSchema{Identifier: "t2", AttributeType: "TEXT"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

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
})
