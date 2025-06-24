package redis_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/helper"
)

func WaitForIndexing(c *redis.Client, index string) {
	for {
		res, err := c.FTInfo(context.Background(), index).Result()
		Expect(err).NotTo(HaveOccurred())
		if c.Options().Protocol == 2 {
			if res.Indexing == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		} else {
			return
		}
	}
}

func encodeFloat32Vector(vec []float32) []byte {
	buf := new(bytes.Buffer)
	for _, v := range vec {
		binary.Write(buf, binary.LittleEndian, v)
	}
	return buf.Bytes()
}

var _ = Describe("RediSearch commands Resp 2", Label("search"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 2})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should FTCreate and FTSearch WithScores", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo baz")
		client.HSet(ctx, "doc2", "txt", "foo bar")
		res, err := client.FTSearchWithArgs(ctx, "txt", "foo ~bar", &redis.FTSearchOptions{WithScores: true}).Result()

		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(int64(2)))
		for _, doc := range res.Docs {
			Expect(*doc.Score).To(BeNumerically(">", 0))
			Expect(doc.ID).To(Or(Equal("doc1"), Equal("doc2")))
		}
	})

	It("should FTCreate and FTSearch stopwords", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{StopWords: []interface{}{"foo", "bar", "baz"}}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo baz")
		client.HSet(ctx, "doc2", "txt", "hello world")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "foo bar", &redis.FTSearchOptions{NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(0)))
		res2, err := client.FTSearchWithArgs(ctx, "txt", "foo bar hello world", &redis.FTSearchOptions{NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(1)))
	})

	It("should FTCreate and FTSearch filters", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}, &redis.FieldSchema{FieldName: "num", FieldType: redis.SearchFieldTypeNumeric}, &redis.FieldSchema{FieldName: "loc", FieldType: redis.SearchFieldTypeGeo}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo bar", "num", 3.141, "loc", "-0.441,51.458")
		client.HSet(ctx, "doc2", "txt", "foo baz", "num", 2, "loc", "-0.1,51.2")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{Filters: []redis.FTSearchFilter{{FieldName: "num", Min: 0, Max: 2}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc2"))
		res2, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{Filters: []redis.FTSearchFilter{{FieldName: "num", Min: 0, Max: "+inf"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
		Expect(res2.Docs[0].ID).To(BeEquivalentTo("doc1"))
		// Test Geo filter
		geoFilter1 := redis.FTSearchGeoFilter{FieldName: "loc", Longitude: -0.44, Latitude: 51.45, Radius: 10, Unit: "km"}
		geoFilter2 := redis.FTSearchGeoFilter{FieldName: "loc", Longitude: -0.44, Latitude: 51.45, Radius: 100, Unit: "km"}
		res3, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{GeoFilter: []redis.FTSearchGeoFilter{geoFilter1}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Total).To(BeEquivalentTo(int64(1)))
		Expect(res3.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res4, err := client.FTSearchWithArgs(ctx, "txt", "foo", &redis.FTSearchOptions{GeoFilter: []redis.FTSearchGeoFilter{geoFilter2}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res4.Total).To(BeEquivalentTo(int64(2)))
		docs := []interface{}{res4.Docs[0].ID, res4.Docs[1].ID}
		Expect(docs).To(ContainElement("doc1"))
		Expect(docs).To(ContainElement("doc2"))

	})

	It("should FTCreate and FTSearch sortby", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "num", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}, &redis.FieldSchema{FieldName: "num", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}).Result()
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
		Expect(res1.Total).To(BeEquivalentTo(int64(3)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))
		Expect(res1.Docs[2].ID).To(BeEquivalentTo("doc3"))

		res2, err := client.FTSearchWithArgs(ctx, "num", "foo", &redis.FTSearchOptions{NoContent: true, SortBy: []redis.FTSearchSortBy{sortBy2}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(3)))
		Expect(res2.Docs[2].ID).To(BeEquivalentTo("doc1"))
		Expect(res2.Docs[1].ID).To(BeEquivalentTo("doc2"))
		Expect(res2.Docs[0].ID).To(BeEquivalentTo("doc3"))

		res3, err := client.FTSearchWithArgs(ctx, "num", "foo", &redis.FTSearchOptions{NoContent: true, SortBy: []redis.FTSearchSortBy{sortBy2}, SortByWithCount: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Total).To(BeEquivalentTo(int64(3)))

		res4, err := client.FTSearchWithArgs(ctx, "num", "notpresentf00", &redis.FTSearchOptions{NoContent: true, SortBy: []redis.FTSearchSortBy{sortBy2}, SortByWithCount: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res4.Total).To(BeEquivalentTo(int64(0)))
	})

	It("should FTCreate and FTSearch example", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText, Weight: 5}, &redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "title", "RediSearch", "body", "Redisearch implements a search engine on top of redis")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "search engine", &redis.FTSearchOptions{NoContent: true, Verbatim: true, LimitOffset: 0, Limit: 5}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))

	})

	It("should FTCreate NoIndex", Label("search", "ftcreate", "ftsearch"), func() {
		text1 := &redis.FieldSchema{FieldName: "field", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "text", FieldType: redis.SearchFieldTypeText, NoIndex: true, Sortable: true}
		num := &redis.FieldSchema{FieldName: "numeric", FieldType: redis.SearchFieldTypeNumeric, NoIndex: true, Sortable: true}
		geo := &redis.FieldSchema{FieldName: "geo", FieldType: redis.SearchFieldTypeGeo, NoIndex: true, Sortable: true}
		tag := &redis.FieldSchema{FieldName: "tag", FieldType: redis.SearchFieldTypeTag, NoIndex: true, Sortable: true}
		val, err := client.FTCreate(ctx, "idx", &redis.FTCreateOptions{}, text1, text2, num, geo, tag).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx")
		client.HSet(ctx, "doc1", "field", "aaa", "text", "1", "numeric", 1, "geo", "1,1", "tag", "1")
		client.HSet(ctx, "doc2", "field", "aab", "text", "2", "numeric", 2, "geo", "2,2", "tag", "2")
		res1, err := client.FTSearch(ctx, "idx", "@text:aa*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(0)))
		res2, err := client.FTSearch(ctx, "idx", "@field:aa*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
		res3, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "text", Desc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Total).To(BeEquivalentTo(int64(2)))
		Expect(res3.Docs[0].ID).To(BeEquivalentTo("doc2"))
		res4, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "text", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res4.Total).To(BeEquivalentTo(int64(2)))
		Expect(res4.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res5, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "numeric", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res5.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res6, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "geo", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res6.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res7, err := client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{SortBy: []redis.FTSearchSortBy{{FieldName: "tag", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res7.Docs[0].ID).To(BeEquivalentTo("doc1"))

	})

	It("should FTExplain", Label("search", "ftexplain"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: redis.SearchFieldTypeText}
		text3 := &redis.FieldSchema{FieldName: "f3", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, text1, text2, text3).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		res1, err := client.FTExplain(ctx, "txt", "@f3:f3_val @f2:f2_val @f1:f1_val").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1).ToNot(BeEmpty())

	})

	It("should FTAlias", Label("search", "ftexplain"), func() {
		text1 := &redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText}
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
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("index1:lonestar"))

		aliasAddRes, err := client.FTAliasAdd(ctx, "testAlias", "mj23").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasAddRes).To(BeEquivalentTo("OK"))

		res1, err = client.FTSearch(ctx, "mj23", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("index1:lonestar"))

		aliasUpdateRes, err := client.FTAliasUpdate(ctx, "testAlias2", "kb24").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasUpdateRes).To(BeEquivalentTo("OK"))

		res3, err := client.FTSearch(ctx, "kb24", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Docs[0].ID).To(BeEquivalentTo("index2:yogurt"))

		aliasDelRes, err := client.FTAliasDel(ctx, "mj23").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasDelRes).To(BeEquivalentTo("OK"))

	})

	It("should FTCreate and FTSearch textfield, sortable and nostem ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText, Sortable: true, NoStem: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resInfo, err := client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resInfo.Attributes[0].Sortable).To(BeTrue())
		Expect(resInfo.Attributes[0].NoStem).To(BeTrue())

	})

	It("should FTAlter", Label("search", "ftcreate", "ftsearch", "ftalter"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resAlter, err := client.FTAlter(ctx, "idx1", false, []interface{}{"body", redis.SearchFieldTypeText.String()}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resAlter).To(BeEquivalentTo("OK"))

		client.HSet(ctx, "doc1", "title", "MyTitle", "body", "Some content only in the body")
		res1, err := client.FTSearch(ctx, "idx1", "only in the body").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))

		_, err = client.FTSearch(ctx, "idx_not_exist", "only in the body").Result()
		Expect(err).To(HaveOccurred())
	})

	It("should FTSpellCheck", Label("search", "ftcreate", "ftsearch", "ftspellcheck"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "f1", "some valid content", "f2", "this is sample text")
		client.HSet(ctx, "doc2", "f1", "very important", "f2", "lorem ipsum")

		resSpellCheck, err := client.FTSpellCheck(ctx, "idx1", "impornant").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck[0].Suggestions[0].Suggestion).To(BeEquivalentTo("important"))

		resSpellCheck2, err := client.FTSpellCheck(ctx, "idx1", "contnt").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck2[0].Suggestions[0].Suggestion).To(BeEquivalentTo("content"))

		// test spellcheck with Levenshtein distance
		resSpellCheck3, err := client.FTSpellCheck(ctx, "idx1", "vlis").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck3[0].Term).To(BeEquivalentTo("vlis"))

		resSpellCheck4, err := client.FTSpellCheckWithArgs(ctx, "idx1", "vlis", &redis.FTSpellCheckOptions{Distance: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck4[0].Suggestions[0].Suggestion).To(BeEquivalentTo("valid"))

		// test spellcheck include
		resDictAdd, err := client.FTDictAdd(ctx, "dict", "lore", "lorem", "lorm").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictAdd).To(BeEquivalentTo(3))
		terms := &redis.FTSpellCheckTerms{Inclusion: "INCLUDE", Dictionary: "dict"}
		resSpellCheck5, err := client.FTSpellCheckWithArgs(ctx, "idx1", "lorm", &redis.FTSpellCheckOptions{Terms: terms}).Result()
		Expect(err).NotTo(HaveOccurred())
		lorm := resSpellCheck5[0].Suggestions
		Expect(len(lorm)).To(BeEquivalentTo(3))
		Expect(lorm[0].Score).To(BeEquivalentTo(0.5))
		Expect(lorm[1].Score).To(BeEquivalentTo(0))
		Expect(lorm[2].Score).To(BeEquivalentTo(0))

		terms2 := &redis.FTSpellCheckTerms{Inclusion: "EXCLUDE", Dictionary: "dict"}
		resSpellCheck6, err := client.FTSpellCheckWithArgs(ctx, "idx1", "lorm", &redis.FTSpellCheckOptions{Terms: terms2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck6).To(BeEmpty())
	})

	It("should FTDict opreations", Label("search", "ftdictdump", "ftdictdel", "ftdictadd"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resDictAdd, err := client.FTDictAdd(ctx, "custom_dict", "item1", "item2", "item3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictAdd).To(BeEquivalentTo(3))

		resDictDel, err := client.FTDictDel(ctx, "custom_dict", "item2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDel).To(BeEquivalentTo(1))

		resDictDump, err := client.FTDictDump(ctx, "custom_dict").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDump).To(BeEquivalentTo([]string{"item1", "item3"}))

		resDictDel2, err := client.FTDictDel(ctx, "custom_dict", "item1", "item3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDel2).To(BeEquivalentTo(2))
	})

	It("should FTSearch phonetic matcher", Label("search", "ftsearch"), func() {
		text1 := &redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Jon")
		client.HSet(ctx, "doc2", "name", "John")

		res1, err := client.FTSearch(ctx, "idx1", "Jon").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))
		Expect(res1.Docs[0].Fields["name"]).To(BeEquivalentTo("Jon"))

		client.FlushDB(ctx)
		text2 := &redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText, PhoneticMatcher: "dm:en"}
		val2, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val2).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Jon")
		client.HSet(ctx, "doc2", "name", "John")

		res2, err := client.FTSearch(ctx, "idx1", "Jon").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
		names := []interface{}{res2.Docs[0].Fields["name"], res2.Docs[1].Fields["name"]}
		Expect(names).To(ContainElement("Jon"))
		Expect(names).To(ContainElement("John"))
	})

	// up until redis 8 the default scorer was TFIDF, in redis 8 it is BM25
	// this test expect redis major version >= 8
	It("should FTSearch WithScores", Label("search", "ftsearch"), func() {
		SkipBeforeRedisVersion(7.9, "default scorer is not BM25STD")

		text1 := &redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "description", "The quick brown fox jumps over the lazy dog")
		client.HSet(ctx, "doc2", "description", "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do.")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeNumerically("<=", 0.236))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "TFIDF"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "TFIDF.DOCNORM"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(0.14285714285714285))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "BM25"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeNumerically("<=", 0.22471909420069797))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "DISMAX"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(2)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "DOCSCORE"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "HAMMING"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(0)))
	})

	// up until redis 8 the default scorer was TFIDF, in redis 8 it is BM25
	// this test expect redis version < 8.0
	It("should FTSearch WithScores", Label("search", "ftsearch"), func() {
		SkipAfterRedisVersion(7.9, "default scorer is not TFIDF")
		text1 := &redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "description", "The quick brown fox jumps over the lazy dog")
		client.HSet(ctx, "doc2", "description", "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do.")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "TFIDF"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "TFIDF.DOCNORM"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(0.14285714285714285))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "BM25"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeNumerically("<=", 0.22471909420069797))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "DISMAX"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(2)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "DOCSCORE"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &redis.FTSearchOptions{WithScores: true, Scorer: "HAMMING"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(0)))
	})

	It("should FTConfigSet and FTConfigGet ", Label("search", "ftconfigget", "ftconfigset", "NonRedisEnterprise"), func() {
		val, err := client.FTConfigSet(ctx, "MINPREFIX", "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		res, err := client.FTConfigGet(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res["MINPREFIX"]).To(BeEquivalentTo("1"))

		res, err = client.FTConfigGet(ctx, "MINPREFIX").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo(map[string]interface{}{"MINPREFIX": "1"}))

	})

	It("should FTAggregate GroupBy ", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}
		text3 := &redis.FieldSchema{FieldName: "parent", FieldType: redis.SearchFieldTypeText}
		num := &redis.FieldSchema{FieldName: "random_num", FieldType: redis.SearchFieldTypeNumeric}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2, text3, num).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "search", "title", "RediSearch",
			"body", "Redisearch implements a search engine on top of redis",
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
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliascount"]).To(BeEquivalentTo("3"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchCountDistinct, Args: []interface{}{"@title"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliascount_distincttitle"]).To(BeEquivalentTo("3"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchSum, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliassumrandom_num"]).To(BeEquivalentTo("21"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchMin, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasminrandom_num"]).To(BeEquivalentTo("3"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchMax, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasmaxrandom_num"]).To(BeEquivalentTo("10"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchAvg, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasavgrandom_num"]).To(BeEquivalentTo("7"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchStdDev, Args: []interface{}{"@random_num"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasstddevrandom_num"]).To(BeEquivalentTo("3.60555127546"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchQuantile, Args: []interface{}{"@random_num", 0.5}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasquantilerandom_num,0.5"]).To(BeEquivalentTo("8"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchToList, Args: []interface{}{"@title"}}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliastolisttitle"]).To(ContainElements("RediSearch", "RedisAI", "RedisJson"))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchFirstValue, Args: []interface{}{"@title"}, As: "first"}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["first"]).To(Or(BeEquivalentTo("RediSearch"), BeEquivalentTo("RedisAI"), BeEquivalentTo("RedisJson")))

		reducer = redis.FTAggregateReducer{Reducer: redis.SearchRandomSample, Args: []interface{}{"@title", 2}, As: "random"}
		options = &redis.FTAggregateOptions{GroupBy: []redis.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []redis.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["random"]).To(Or(
			ContainElement("RediSearch"),
			ContainElement("RedisAI"),
			ContainElement("RedisJson"),
		))

	})

	It("should FTAggregate sort and limit", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "t1", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "t2", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "t1", "a", "t2", "b")
		client.HSet(ctx, "doc2", "t1", "b", "t2", "a")

		options := &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t2", Asc: true}, {FieldName: "@t1", Desc: true}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("b"))
		Expect(res.Rows[1].Fields["t1"]).To(BeEquivalentTo("a"))
		Expect(res.Rows[0].Fields["t2"]).To(BeEquivalentTo("a"))
		Expect(res.Rows[1].Fields["t2"]).To(BeEquivalentTo("b"))

		options = &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t1"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("a"))
		Expect(res.Rows[1].Fields["t1"]).To(BeEquivalentTo("b"))

		options = &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t1"}}, SortByMax: 1}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("a"))

		options = &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t1"}}, Limit: 1, LimitOffset: 1}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("b"))

		options = &redis.FTAggregateOptions{SortBy: []redis.FTAggregateSortBy{{FieldName: "@t1"}}, Limit: 1, LimitOffset: 0}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("a"))
	})

	It("should FTAggregate load ", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "t1", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "t2", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "t1", "hello", "t2", "world")

		options := &redis.FTAggregateOptions{Load: []redis.FTAggregateLoad{{Field: "t1"}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("hello"))

		options = &redis.FTAggregateOptions{Load: []redis.FTAggregateLoad{{Field: "t2"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t2"]).To(BeEquivalentTo("world"))

		options = &redis.FTAggregateOptions{Load: []redis.FTAggregateLoad{{Field: "t2", As: "t2alias"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t2alias"]).To(BeEquivalentTo("world"))

		options = &redis.FTAggregateOptions{Load: []redis.FTAggregateLoad{{Field: "t1"}, {Field: "t2", As: "t2alias"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("hello"))
		Expect(res.Rows[0].Fields["t2alias"]).To(BeEquivalentTo("world"))

		options = &redis.FTAggregateOptions{LoadAll: true}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("hello"))
		Expect(res.Rows[0].Fields["t2"]).To(BeEquivalentTo("world"))

		_, err = client.FTAggregateWithArgs(ctx, "idx_not_exist", "*", &redis.FTAggregateOptions{}).Result()
		Expect(err).To(HaveOccurred())
	})

	It("should FTAggregate with scorer and addscores", Label("search", "ftaggregate", "NonRedisEnterprise"), func() {
		SkipBeforeRedisVersion(7.4, "no addscores support")
		title := &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText, Sortable: false}
		description := &redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText, Sortable: false}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnHash: true, Prefix: []interface{}{"product:"}}, title, description).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "product:1", "title", "New Gaming Laptop", "description", "this is not a desktop")
		client.HSet(ctx, "product:2", "title", "Super Old Not Gaming Laptop", "description", "this laptop is not a new laptop but it is a laptop")
		client.HSet(ctx, "product:3", "title", "Office PC", "description", "office desktop pc")

		options := &redis.FTAggregateOptions{
			AddScores: true,
			Scorer:    "BM25",
			SortBy: []redis.FTAggregateSortBy{{
				FieldName: "@__score",
				Desc:      true,
			}},
		}

		res, err := client.FTAggregateWithArgs(ctx, "idx1", "laptop", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).ToNot(BeNil())
		Expect(len(res.Rows)).To(BeEquivalentTo(2))
		score1, err := helper.ParseFloat(fmt.Sprintf("%s", res.Rows[0].Fields["__score"]))
		Expect(err).NotTo(HaveOccurred())
		score2, err := helper.ParseFloat(fmt.Sprintf("%s", res.Rows[1].Fields["__score"]))
		Expect(err).NotTo(HaveOccurred())
		Expect(score1).To(BeNumerically(">", score2))

		optionsDM := &redis.FTAggregateOptions{
			AddScores: true,
			Scorer:    "DISMAX",
			SortBy: []redis.FTAggregateSortBy{{
				FieldName: "@__score",
				Desc:      true,
			}},
		}

		resDM, err := client.FTAggregateWithArgs(ctx, "idx1", "laptop", optionsDM).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDM).ToNot(BeNil())
		Expect(len(resDM.Rows)).To(BeEquivalentTo(2))
		score1DM, err := helper.ParseFloat(fmt.Sprintf("%s", resDM.Rows[0].Fields["__score"]))
		Expect(err).NotTo(HaveOccurred())
		score2DM, err := helper.ParseFloat(fmt.Sprintf("%s", resDM.Rows[1].Fields["__score"]))
		Expect(err).NotTo(HaveOccurred())
		Expect(score1DM).To(BeNumerically(">", score2DM))

		Expect(score1DM).To(BeEquivalentTo(float64(4)))
		Expect(score2DM).To(BeEquivalentTo(float64(1)))
		Expect(score1).NotTo(BeEquivalentTo(score1DM))
		Expect(score2).NotTo(BeEquivalentTo(score2DM))
	})

	It("should FTAggregate apply and groupby", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "PrimaryKey", FieldType: redis.SearchFieldTypeText, Sortable: true}
		num1 := &redis.FieldSchema{FieldName: "CreatedDateTimeUTC", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		// 6 feb
		client.HSet(ctx, "doc1", "PrimaryKey", "9::362330", "CreatedDateTimeUTC", "1738823999")

		// 12 feb
		client.HSet(ctx, "doc2", "PrimaryKey", "9::362329", "CreatedDateTimeUTC", "1739342399")
		client.HSet(ctx, "doc3", "PrimaryKey", "9::362329", "CreatedDateTimeUTC", "1739353199")

		reducer := redis.FTAggregateReducer{Reducer: redis.SearchCount, As: "perDay"}

		options := &redis.FTAggregateOptions{
			Apply: []redis.FTAggregateApply{{Field: "floor(@CreatedDateTimeUTC /(60*60*24))", As: "TimestampAsDay"}},
			GroupBy: []redis.FTAggregateGroupBy{{
				Fields: []interface{}{"@TimestampAsDay"},
				Reduce: []redis.FTAggregateReducer{reducer},
			}},
			SortBy: []redis.FTAggregateSortBy{{
				FieldName: "@perDay",
				Desc:      true,
			}},
		}

		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).ToNot(BeNil())
		Expect(len(res.Rows)).To(BeEquivalentTo(2))
		Expect(res.Rows[0].Fields["perDay"]).To(BeEquivalentTo("2"))
		Expect(res.Rows[1].Fields["perDay"]).To(BeEquivalentTo("1"))
	})

	It("should FTAggregate apply", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "PrimaryKey", FieldType: redis.SearchFieldTypeText, Sortable: true}
		num1 := &redis.FieldSchema{FieldName: "CreatedDateTimeUTC", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "PrimaryKey", "9::362330", "CreatedDateTimeUTC", "637387878524969984")
		client.HSet(ctx, "doc2", "PrimaryKey", "9::362329", "CreatedDateTimeUTC", "637387875859270016")

		options := &redis.FTAggregateOptions{Apply: []redis.FTAggregateApply{{Field: "@CreatedDateTimeUTC * 10", As: "CreatedDateTimeUTC"}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["CreatedDateTimeUTC"]).To(Or(BeEquivalentTo("6373878785249699840"), BeEquivalentTo("6373878758592700416")))
		Expect(res.Rows[1].Fields["CreatedDateTimeUTC"]).To(Or(BeEquivalentTo("6373878785249699840"), BeEquivalentTo("6373878758592700416")))

	})

	It("should FTAggregate filter", Label("search", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText, Sortable: true}
		num1 := &redis.FieldSchema{FieldName: "age", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}
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
			Expect(res.Total).To(Or(BeEquivalentTo(2), BeEquivalentTo(1)))
			Expect(res.Rows[0].Fields["name"]).To(BeEquivalentTo("foo"))

			options = &redis.FTAggregateOptions{Filter: "@age > 15", DialectVersion: dlc, SortBy: []redis.FTAggregateSortBy{{FieldName: "@age"}}}
			res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Total).To(BeEquivalentTo(2))
			Expect(res.Rows[0].Fields["age"]).To(BeEquivalentTo("19"))
			Expect(res.Rows[1].Fields["age"]).To(BeEquivalentTo("25"))
		}
	})

	It("should return only the base query when options is nil", Label("search", "ftaggregate"), func() {
		args := redis.FTAggregateQuery("testQuery", nil)
		Expect(args).To(Equal(redis.AggregateQuery{"testQuery"}))
	})

	It("should include VERBATIM and SCORER when options are set", Label("search", "ftaggregate"), func() {
		options := &redis.FTAggregateOptions{
			Verbatim: true,
			Scorer:   "BM25",
		}
		args := redis.FTAggregateQuery("testQuery", options)
		Expect(args[0]).To(Equal("testQuery"))
		Expect(args).To(ContainElement("VERBATIM"))
		Expect(args).To(ContainElement("SCORER"))
		Expect(args).To(ContainElement("BM25"))
	})

	It("should include ADDSCORES when AddScores is true", Label("search", "ftaggregate"), func() {
		options := &redis.FTAggregateOptions{
			AddScores: true,
		}
		args := redis.FTAggregateQuery("q", options)
		Expect(args).To(ContainElement("ADDSCORES"))
	})

	It("should include LOADALL when LoadAll is true", Label("search", "ftaggregate"), func() {
		options := &redis.FTAggregateOptions{
			LoadAll: true,
		}
		args := redis.FTAggregateQuery("q", options)
		Expect(args).To(ContainElement("LOAD"))
		Expect(args).To(ContainElement("*"))
	})

	It("should include LOAD when Load is provided", Label("search", "ftaggregate"), func() {
		options := &redis.FTAggregateOptions{
			Load: []redis.FTAggregateLoad{
				{Field: "field1", As: "alias1"},
				{Field: "field2"},
			},
		}
		args := redis.FTAggregateQuery("q", options)
		// Verify LOAD options related arguments
		Expect(args).To(ContainElement("LOAD"))
		// Check that field names and aliases are present
		Expect(args).To(ContainElement("field1"))
		Expect(args).To(ContainElement("alias1"))
		Expect(args).To(ContainElement("field2"))
	})

	It("should include TIMEOUT when Timeout > 0", Label("search", "ftaggregate"), func() {
		options := &redis.FTAggregateOptions{
			Timeout: 500,
		}
		args := redis.FTAggregateQuery("q", options)
		Expect(args).To(ContainElement("TIMEOUT"))
		found := false
		for i, a := range args {
			if fmt.Sprintf("%s", a) == "TIMEOUT" {
				Expect(fmt.Sprintf("%d", args[i+1])).To(Equal("500"))
				found = true
				break
			}
		}
		Expect(found).To(BeTrue())
	})

	It("should FTSearch SkipInitialScan", Label("search", "ftsearch"), func() {
		client.HSet(ctx, "doc1", "foo", "bar")

		text1 := &redis.FieldSchema{FieldName: "foo", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{SkipInitialScan: true}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTSearch(ctx, "idx1", "@foo:bar").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(int64(0)))
	})

	It("should FTCreate json", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: "$.name", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry"}`)
		client.JSONSet(ctx, "king:2", "$", `{"name": "james"}`)

		res, err := client.FTSearch(ctx, "idx1", "henry").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"henry"}`))
	})

	It("should FTCreate json fields as names", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: "$.name", FieldType: redis.SearchFieldTypeText, As: "name"}
		num1 := &redis.FieldSchema{FieldName: "$.age", FieldType: redis.SearchFieldTypeNumeric, As: "just_a_number"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "doc:1", "$", `{"name": "Jon", "age": 25}`)

		res, err := client.FTSearchWithArgs(ctx, "idx1", "Jon", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "name"}, {FieldName: "just_a_number"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["name"]).To(BeEquivalentTo("Jon"))
		Expect(res.Docs[0].Fields["just_a_number"]).To(BeEquivalentTo("25"))
	})

	It("should FTCreate CaseSensitive", Label("search", "ftcreate"), func() {

		tag1 := &redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeTag, CaseSensitive: false}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, tag1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "1", "t", "HELLO")
		client.HSet(ctx, "2", "t", "hello")

		res, err := client.FTSearch(ctx, "idx1", "@t:{HELLO}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(2))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("1"))
		Expect(res.Docs[1].ID).To(BeEquivalentTo("2"))

		resDrop, err := client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		tag2 := &redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeTag, CaseSensitive: true}
		val, err = client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, tag2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTSearch(ctx, "idx1", "@t:{HELLO}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("1"))

	})

	It("should FTSearch ReturnFields", Label("search", "ftsearch"), func() {
		resJson, err := client.JSONSet(ctx, "doc:1", "$", `{"t": "riceratops","t2": "telmatosaurus", "n": 9072, "flt": 97.2}`).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resJson).To(BeEquivalentTo("OK"))

		text1 := &redis.FieldSchema{FieldName: "$.t", FieldType: redis.SearchFieldTypeText}
		num1 := &redis.FieldSchema{FieldName: "$.flt", FieldType: redis.SearchFieldTypeNumeric}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "*", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "$.t", As: "txt"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["txt"]).To(BeEquivalentTo("riceratops"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "*", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "$.t2", As: "txt"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["txt"]).To(BeEquivalentTo("telmatosaurus"))
	})

	It("should FTSynUpdate", Label("search", "ftsynupdate"), func() {

		text1 := &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}
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
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc2"))
		Expect(res.Docs[0].Fields["title"]).To(BeEquivalentTo("he is another baby"))
		Expect(res.Docs[0].Fields["body"]).To(BeEquivalentTo("another test"))
	})

	It("should FTSynDump", Label("search", "ftsyndump"), func() {

		text1 := &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}
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
		Expect(resSynDump[0].Term).To(BeEquivalentTo("baby"))
		Expect(resSynDump[0].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[1].Term).To(BeEquivalentTo("wood"))
		Expect(resSynDump[1].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[2].Term).To(BeEquivalentTo("boy"))
		Expect(resSynDump[2].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[3].Term).To(BeEquivalentTo("tree"))
		Expect(resSynDump[3].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[4].Term).To(BeEquivalentTo("child"))
		Expect(resSynDump[4].Synonyms).To(Or(BeEquivalentTo([]string{"id1"}), BeEquivalentTo([]string{"id1", "id1"})))
		Expect(resSynDump[5].Term).To(BeEquivalentTo("offspring"))
		Expect(resSynDump[5].Synonyms).To(BeEquivalentTo([]string{"id1"}))

	})

	It("should FTCreate json with alias", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: "$.name", FieldType: redis.SearchFieldTypeText, As: "name"}
		num1 := &redis.FieldSchema{FieldName: "$.num", FieldType: redis.SearchFieldTypeNumeric, As: "num"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry", "num": 42}`)
		client.JSONSet(ctx, "king:2", "$", `{"name": "james", "num": 3.14}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:henry").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"henry","num":42}`))

		res, err = client.FTSearch(ctx, "idx1", "@num:[0 10]").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:2"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"james","num":3.14}`))
	})

	It("should FTCreate json with multipath", Label("search", "ftcreate"), func() {

		tag1 := &redis.FieldSchema{FieldName: "$..name", FieldType: redis.SearchFieldTypeTag, As: "name"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, tag1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry", "country": {"name": "england"}}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:{england}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"henry","country":{"name":"england"}}`))
	})

	It("should FTCreate json with jsonpath", Label("search", "ftcreate"), func() {

		text1 := &redis.FieldSchema{FieldName: `$["prod:name"]`, FieldType: redis.SearchFieldTypeText, As: "name"}
		text2 := &redis.FieldSchema{FieldName: `$.prod:name`, FieldType: redis.SearchFieldTypeText, As: "name_unsupported"}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{OnJSON: true}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "doc:1", "$", `{"prod:name": "RediSearch"}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:RediSearch").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"prod:name":"RediSearch"}`))

		res, err = client.FTSearch(ctx, "idx1", "@name_unsupported:RediSearch").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "@name:RediSearch", &redis.FTSearchOptions{Return: []redis.FTSearchReturn{{FieldName: "name"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["name"]).To(BeEquivalentTo("RediSearch"))

	})

	It("should FTCreate VECTOR", Label("search", "ftcreate"), func() {
		hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
		val, err := client.FTCreate(ctx, "idx1",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "v", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}).Result()
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
		Expect(res.Docs[0].ID).To(BeEquivalentTo("a"))
		Expect(res.Docs[0].Fields["__v_score"]).To(BeEquivalentTo("0"))
	})

	It("should FTCreate VECTOR with dialect 1 ", Label("search", "ftcreate"), func() {
		hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
		val, err := client.FTCreate(ctx, "idx1",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "v", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "a", "v", "aaaaaaaa")
		client.HSet(ctx, "b", "v", "aaaabaaa")
		client.HSet(ctx, "c", "v", "aaaaabaa")

		searchOptions := &redis.FTSearchOptions{
			Return:         []redis.FTSearchReturn{{FieldName: "v"}},
			SortBy:         []redis.FTSearchSortBy{{FieldName: "v", Asc: true}},
			Limit:          10,
			DialectVersion: 1,
		}
		res, err := client.FTSearchWithArgs(ctx, "idx1", "*", searchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("a"))
		Expect(res.Docs[0].Fields["v"]).To(BeEquivalentTo("aaaaaaaa"))
	})

	It("should FTCreate VECTOR with default dialect", Label("search", "ftcreate"), func() {
		hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
		val, err := client.FTCreate(ctx, "idx1",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "v", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "a", "v", "aaaaaaaa")
		client.HSet(ctx, "b", "v", "aaaabaaa")
		client.HSet(ctx, "c", "v", "aaaaabaa")

		searchOptions := &redis.FTSearchOptions{
			Return: []redis.FTSearchReturn{{FieldName: "__v_score"}},
			SortBy: []redis.FTSearchSortBy{{FieldName: "__v_score", Asc: true}},
			Params: map[string]interface{}{"vec": "aaaaaaaa"},
		}
		res, err := client.FTSearchWithArgs(ctx, "idx1", "*=>[KNN 2 @v $vec]", searchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("a"))
		Expect(res.Docs[0].Fields["__v_score"]).To(BeEquivalentTo("0"))
	})

	It("should FTCreate and FTSearch text params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Alice")
		client.HSet(ctx, "doc2", "name", "Bob")
		client.HSet(ctx, "doc3", "name", "Carol")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@name:($name1 | $name2 )", &redis.FTSearchOptions{Params: map[string]interface{}{"name1": "Alice", "name2": "Bob"}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(2)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))

	})

	It("should FTCreate and FTSearch numeric params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "numval", FieldType: redis.SearchFieldTypeNumeric}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "numval", 101)
		client.HSet(ctx, "doc2", "numval", 102)
		client.HSet(ctx, "doc3", "numval", 103)

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@numval:[$min $max]", &redis.FTSearchOptions{Params: map[string]interface{}{"min": 101, "max": 102}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(2)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))

	})

	It("should FTCreate and FTSearch geo params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "g", FieldType: redis.SearchFieldTypeGeo}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "g", "29.69465, 34.95126")
		client.HSet(ctx, "doc2", "g", "29.69350, 34.94737")
		client.HSet(ctx, "doc3", "g", "29.68746, 34.94882")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@g:[$lon $lat $radius $units]", &redis.FTSearchOptions{Params: map[string]interface{}{"lat": "34.95126", "lon": "29.69465", "radius": 1000, "units": "km"}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(3)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))
		Expect(res1.Docs[2].ID).To(BeEquivalentTo("doc3"))

	})

	It("should FTConfigGet return multiple fields", Label("search", "NonRedisEnterprise"), func() {
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

	It("should FTConfigSet and FTConfigGet dialect", Label("search", "ftconfigget", "ftconfigset", "NonRedisEnterprise"), func() {
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
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Attributes[0].Attribute).To(BeEquivalentTo("txt"))

		resDrop, err := client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		// create withsuffixtrie index - text field
		val, err = client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText, WithSuffixtrie: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Attributes[0].WithSuffixtrie).To(BeTrue())

		resDrop, err = client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		// create withsuffixtrie index - tag field
		val, err = client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeTag, WithSuffixtrie: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Attributes[0].WithSuffixtrie).To(BeTrue())
	})

	It("should test dialect 4", Label("search", "ftcreate", "ftsearch", "NonRedisEnterprise"), func() {
		SkipBeforeRedisVersion(7.4, "doesn't work with older redis stack images")
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{
			Prefix: []interface{}{"resource:"},
		}, &redis.FieldSchema{
			FieldName: "uuid",
			FieldType: redis.SearchFieldTypeTag,
		}, &redis.FieldSchema{
			FieldName: "tags",
			FieldType: redis.SearchFieldTypeTag,
		}, &redis.FieldSchema{
			FieldName: "description",
			FieldType: redis.SearchFieldTypeText,
		}, &redis.FieldSchema{
			FieldName: "rating",
			FieldType: redis.SearchFieldTypeNumeric,
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		client.HSet(ctx, "resource:1", map[string]interface{}{
			"uuid":        "123e4567-e89b-12d3-a456-426614174000",
			"tags":        "finance|crypto|$btc|blockchain",
			"description": "Analysis of blockchain technologies & Bitcoin's potential.",
			"rating":      5,
		})
		client.HSet(ctx, "resource:2", map[string]interface{}{
			"uuid":        "987e6543-e21c-12d3-a456-426614174999",
			"tags":        "health|well-being|fitness|new-year's-resolutions",
			"description": "Health trends for the new year, including fitness regimes.",
			"rating":      4,
		})

		res, err := client.FTSearchWithArgs(ctx, "idx1", "@uuid:{$uuid}",
			&redis.FTSearchOptions{
				DialectVersion: 2,
				Params:         map[string]interface{}{"uuid": "123e4567-e89b-12d3-a456-426614174000"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(int64(1)))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("resource:1"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "@uuid:{$uuid}",
			&redis.FTSearchOptions{
				DialectVersion: 4,
				Params:         map[string]interface{}{"uuid": "123e4567-e89b-12d3-a456-426614174000"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(int64(1)))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("resource:1"))

		client.HSet(ctx, "test:1", map[string]interface{}{
			"uuid":  "3d3586fe-0416-4572-8ce",
			"email": "adriano@acme.com.ie",
			"num":   5,
		})

		// Create the index
		ftCreateOptions := &redis.FTCreateOptions{
			Prefix: []interface{}{"test:"},
		}
		schema := []*redis.FieldSchema{
			{
				FieldName: "uuid",
				FieldType: redis.SearchFieldTypeTag,
			},
			{
				FieldName: "email",
				FieldType: redis.SearchFieldTypeTag,
			},
			{
				FieldName: "num",
				FieldType: redis.SearchFieldTypeNumeric,
			},
		}

		val, err = client.FTCreate(ctx, "idx_hash", ftCreateOptions, schema...).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("OK"))
		WaitForIndexing(client, "idx_hash")

		ftSearchOptions := &redis.FTSearchOptions{
			DialectVersion: 4,
			Params: map[string]interface{}{
				"uuid":  "3d3586fe-0416-4572-8ce",
				"email": "adriano@acme.com.ie",
			},
		}

		res, err = client.FTSearchWithArgs(ctx, "idx_hash", "@uuid:{$uuid}", ftSearchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("test:1"))
		Expect(res.Docs[0].Fields["uuid"]).To(BeEquivalentTo("3d3586fe-0416-4572-8ce"))

		res, err = client.FTSearchWithArgs(ctx, "idx_hash", "@email:{$email}", ftSearchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("test:1"))
		Expect(res.Docs[0].Fields["email"]).To(BeEquivalentTo("adriano@acme.com.ie"))

		ftSearchOptions.Params = map[string]interface{}{"num": 5}
		res, err = client.FTSearchWithArgs(ctx, "idx_hash", "@num:[5]", ftSearchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("test:1"))
		Expect(res.Docs[0].Fields["num"]).To(BeEquivalentTo("5"))
	})

	It("should FTCreate GeoShape", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "geom", FieldType: redis.SearchFieldTypeGeoShape, GeoShapeFieldType: "FLAT"}).Result()
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
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("small"))

		res2, err := client.FTSearchWithArgs(ctx, "idx1", "@geom:[CONTAINS $poly]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"poly": "POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
	})

	It("should create search index with FLOAT16 and BFLOAT16 vectors", Label("search", "ftcreate", "NonRedisEnterprise"), func() {
		SkipBeforeRedisVersion(7.4, "doesn't work with older redis stack images")
		val, err := client.FTCreate(ctx, "index", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "float16", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{FlatOptions: &redis.FTFlatOptions{Type: "FLOAT16", Dim: 768, DistanceMetric: "COSINE"}}},
			&redis.FieldSchema{FieldName: "bfloat16", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{FlatOptions: &redis.FTFlatOptions{Type: "BFLOAT16", Dim: 768, DistanceMetric: "COSINE"}}},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "index")
	})

	It("should test geoshapes query intersects and disjoint", Label("NonRedisEnterprise"), func() {
		SkipBeforeRedisVersion(7.4, "doesn't work with older redis stack images")
		_, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{
			FieldName:         "g",
			FieldType:         redis.SearchFieldTypeGeoShape,
			GeoShapeFieldType: "FLAT",
		}).Result()
		Expect(err).NotTo(HaveOccurred())

		client.HSet(ctx, "doc_point1", "g", "POINT (10 10)")
		client.HSet(ctx, "doc_point2", "g", "POINT (50 50)")
		client.HSet(ctx, "doc_polygon1", "g", "POLYGON ((20 20, 25 35, 35 25, 20 20))")
		client.HSet(ctx, "doc_polygon2", "g", "POLYGON ((60 60, 65 75, 70 70, 65 55, 60 60))")

		intersection, err := client.FTSearchWithArgs(ctx, "idx1", "@g:[intersects $shape]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		_assert_geosearch_result(&intersection, []string{"doc_point2", "doc_polygon1"})

		disjunction, err := client.FTSearchWithArgs(ctx, "idx1", "@g:[disjoint $shape]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		_assert_geosearch_result(&disjunction, []string{"doc_point1", "doc_polygon2"})
	})

	It("should test geoshapes query contains and within", func() {
		_, err := client.FTCreate(ctx, "idx2", &redis.FTCreateOptions{}, &redis.FieldSchema{
			FieldName:         "g",
			FieldType:         redis.SearchFieldTypeGeoShape,
			GeoShapeFieldType: "FLAT",
		}).Result()
		Expect(err).NotTo(HaveOccurred())

		client.HSet(ctx, "doc_point1", "g", "POINT (10 10)")
		client.HSet(ctx, "doc_point2", "g", "POINT (50 50)")
		client.HSet(ctx, "doc_polygon1", "g", "POLYGON ((20 20, 25 35, 35 25, 20 20))")
		client.HSet(ctx, "doc_polygon2", "g", "POLYGON ((60 60, 65 75, 70 70, 65 55, 60 60))")

		containsA, err := client.FTSearchWithArgs(ctx, "idx2", "@g:[contains $shape]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"shape": "POINT(25 25)"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		_assert_geosearch_result(&containsA, []string{"doc_polygon1"})

		containsB, err := client.FTSearchWithArgs(ctx, "idx2", "@g:[contains $shape]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"shape": "POLYGON((24 24, 24 26, 25 25, 24 24))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		_assert_geosearch_result(&containsB, []string{"doc_polygon1"})

		within, err := client.FTSearchWithArgs(ctx, "idx2", "@g:[within $shape]",
			&redis.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		_assert_geosearch_result(&within, []string{"doc_point2", "doc_polygon1"})
	})

	It("should search missing fields", Label("search", "ftcreate", "ftsearch", "NonRedisEnterprise"), func() {
		SkipBeforeRedisVersion(7.4, "doesn't work with older redis stack images")
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{Prefix: []interface{}{"property:"}},
			&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText, Sortable: true},
			&redis.FieldSchema{FieldName: "features", FieldType: redis.SearchFieldTypeTag, IndexMissing: true},
			&redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText, IndexMissing: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "property:1", map[string]interface{}{
			"title":       "Luxury Villa in Malibu",
			"features":    "pool,sea view,modern",
			"description": "A stunning modern villa overlooking the Pacific Ocean.",
		})

		client.HSet(ctx, "property:2", map[string]interface{}{
			"title":       "Downtown Flat",
			"description": "Modern flat in central Paris with easy access to metro.",
		})

		client.HSet(ctx, "property:3", map[string]interface{}{
			"title":    "Beachfront Bungalow",
			"features": "beachfront,sun deck",
		})

		res, err := client.FTSearchWithArgs(ctx, "idx1", "ismissing(@features)", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:2"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "-ismissing(@features)", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:1"))
		Expect(res.Docs[1].ID).To(BeEquivalentTo("property:3"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "ismissing(@description)", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:3"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "-ismissing(@description)", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:1"))
		Expect(res.Docs[1].ID).To(BeEquivalentTo("property:2"))
	})

	It("should search empty fields", Label("search", "ftcreate", "ftsearch", "NonRedisEnterprise"), func() {
		SkipBeforeRedisVersion(7.4, "doesn't work with older redis stack images")
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{Prefix: []interface{}{"property:"}},
			&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText, Sortable: true},
			&redis.FieldSchema{FieldName: "features", FieldType: redis.SearchFieldTypeTag, IndexEmpty: true},
			&redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText, IndexEmpty: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "property:1", map[string]interface{}{
			"title":       "Luxury Villa in Malibu",
			"features":    "pool,sea view,modern",
			"description": "A stunning modern villa overlooking the Pacific Ocean.",
		})

		client.HSet(ctx, "property:2", map[string]interface{}{
			"title":       "Downtown Flat",
			"features":    "",
			"description": "Modern flat in central Paris with easy access to metro.",
		})

		client.HSet(ctx, "property:3", map[string]interface{}{
			"title":       "Beachfront Bungalow",
			"features":    "beachfront,sun deck",
			"description": "",
		})

		res, err := client.FTSearchWithArgs(ctx, "idx1", "@features:{\"\"}", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:2"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "-@features:{\"\"}", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:1"))
		Expect(res.Docs[1].ID).To(BeEquivalentTo("property:3"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "@description:''", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:3"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "-@description:''", &redis.FTSearchOptions{DialectVersion: 4, Return: []redis.FTSearchReturn{{FieldName: "id"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("property:1"))
		Expect(res.Docs[1].ID).To(BeEquivalentTo("property:2"))
	})

	It("should FTCreate VECTOR with int8 and uint8 types", Label("search", "ftcreate"), func() {
		SkipBeforeRedisVersion(7.9, "doesn't work with older redis")
		// Define INT8 vector field
		hnswOptionsInt8 := &redis.FTHNSWOptions{
			Type:           "INT8",
			Dim:            2,
			DistanceMetric: "L2",
		}

		// Define UINT8 vector field
		hnswOptionsUint8 := &redis.FTHNSWOptions{
			Type:           "UINT8",
			Dim:            2,
			DistanceMetric: "L2",
		}

		// Create index with INT8 and UINT8 vector fields
		val, err := client.FTCreate(ctx, "idx1",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "int8_vector", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptionsInt8}},
			&redis.FieldSchema{FieldName: "uint8_vector", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptionsUint8}},
		).Result()

		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		// Insert vectors in int8 and uint8 format
		client.HSet(ctx, "doc1", "int8_vector", "\x01\x02", "uint8_vector", "\x01\x02")
		client.HSet(ctx, "doc2", "int8_vector", "\x03\x04", "uint8_vector", "\x03\x04")

		// Perform KNN search on INT8 vector
		searchOptionsInt8 := &redis.FTSearchOptions{
			Return:         []redis.FTSearchReturn{{FieldName: "int8_vector"}},
			SortBy:         []redis.FTSearchSortBy{{FieldName: "int8_vector", Asc: true}},
			DialectVersion: 2,
			Params:         map[string]interface{}{"vec": "\x01\x02"},
		}

		resInt8, err := client.FTSearchWithArgs(ctx, "idx1", "*=>[KNN 1 @int8_vector $vec]", searchOptionsInt8).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resInt8.Docs[0].ID).To(BeEquivalentTo("doc1"))

		// Perform KNN search on UINT8 vector
		searchOptionsUint8 := &redis.FTSearchOptions{
			Return:         []redis.FTSearchReturn{{FieldName: "uint8_vector"}},
			SortBy:         []redis.FTSearchSortBy{{FieldName: "uint8_vector", Asc: true}},
			DialectVersion: 2,
			Params:         map[string]interface{}{"vec": "\x01\x02"},
		}

		resUint8, err := client.FTSearchWithArgs(ctx, "idx1", "*=>[KNN 1 @uint8_vector $vec]", searchOptionsUint8).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resUint8.Docs[0].ID).To(BeEquivalentTo("doc1"))
	})

	It("should return special float scores in FT.SEARCH vecsim", Label("search", "ftsearch", "vecsim"), func() {
		SkipBeforeRedisVersion(7.4, "doesn't work with older redis stack images")

		vecField := &redis.FTFlatOptions{
			Type:           "FLOAT32",
			Dim:            2,
			DistanceMetric: "IP",
		}
		_, err := client.FTCreate(ctx, "idx_vec",
			&redis.FTCreateOptions{OnHash: true, Prefix: []interface{}{"doc:"}},
			&redis.FieldSchema{FieldName: "vector", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{FlatOptions: vecField}}).Result()
		Expect(err).NotTo(HaveOccurred())
		WaitForIndexing(client, "idx_vec")

		bigPos := []float32{1e38, 1e38}
		bigNeg := []float32{-1e38, -1e38}
		nanVec := []float32{float32(math.NaN()), 0}
		negNanVec := []float32{float32(math.Copysign(math.NaN(), -1)), 0}

		client.HSet(ctx, "doc:1", "vector", encodeFloat32Vector(bigPos))
		client.HSet(ctx, "doc:2", "vector", encodeFloat32Vector(bigNeg))
		client.HSet(ctx, "doc:3", "vector", encodeFloat32Vector(nanVec))
		client.HSet(ctx, "doc:4", "vector", encodeFloat32Vector(negNanVec))

		searchOptions := &redis.FTSearchOptions{WithScores: true, Params: map[string]interface{}{"vec": encodeFloat32Vector(bigPos)}}
		res, err := client.FTSearchWithArgs(ctx, "idx_vec", "*=>[KNN 4 @vector $vec]", searchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(4))

		var scores []float64
		for _, row := range res.Docs {
			raw := fmt.Sprintf("%v", row.Fields["__vector_score"])
			f, err := helper.ParseFloat(raw)
			Expect(err).NotTo(HaveOccurred())
			scores = append(scores, f)
		}

		Expect(scores).To(ContainElement(BeNumerically("==", math.Inf(1))))
		Expect(scores).To(ContainElement(BeNumerically("==", math.Inf(-1))))

		// For NaN values, use a custom check since NaN != NaN in floating point math
		nanCount := 0
		for _, score := range scores {
			if math.IsNaN(score) {
				nanCount++
			}
		}
		Expect(nanCount).To(Equal(2))
	})

	It("should fail when using a non-zero offset with a zero limit", Label("search", "ftsearch"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "testIdx", &redis.FTCreateOptions{}, &redis.FieldSchema{
			FieldName: "txt",
			FieldType: redis.SearchFieldTypeText,
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "testIdx")

		client.HSet(ctx, "doc1", "txt", "hello world")

		// Attempt to search with a non-zero offset and zero limit.
		_, err = client.FTSearchWithArgs(ctx, "testIdx", "hello", &redis.FTSearchOptions{
			LimitOffset: 5,
			Limit:       0,
		}).Result()
		Expect(err).To(HaveOccurred())
	})

	It("should evaluate exponentiation precedence in APPLY expressions correctly", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "txns", &redis.FTCreateOptions{}, &redis.FieldSchema{
			FieldName: "dummy",
			FieldType: redis.SearchFieldTypeText,
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txns")

		client.HSet(ctx, "doc1", "dummy", "dummy")

		correctOptions := &redis.FTAggregateOptions{
			Apply: []redis.FTAggregateApply{
				{Field: "(2*3^2)", As: "Value"},
			},
			Limit:       1,
			LimitOffset: 0,
		}
		correctRes, err := client.FTAggregateWithArgs(ctx, "txns", "*", correctOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(correctRes.Rows[0].Fields["Value"]).To(BeEquivalentTo("18"))
	})

	It("should return a syntax error when empty strings are used for numeric parameters", Label("search", "ftsearch"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "idx", &redis.FTCreateOptions{}, &redis.FieldSchema{
			FieldName: "n",
			FieldType: redis.SearchFieldTypeNumeric,
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx")

		client.HSet(ctx, "doc1", "n", 0)

		_, err = client.FTSearchWithArgs(ctx, "idx", "*", &redis.FTSearchOptions{
			Filters: []redis.FTSearchFilter{{
				FieldName: "n",
				Min:       "",
				Max:       "",
			}},
			DialectVersion: 2,
		}).Result()
		Expect(err).To(HaveOccurred())
	})

	It("should return NaN as default for AVG reducer when no numeric values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestAvg", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestAvg")

		client.HSet(ctx, "doc1", "grp", "g1")

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchAvg, Args: []interface{}{"@n"}, As: "avg"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestAvg", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())

		Expect(res.Rows[0].Fields["avg"]).To(SatisfyAny(Equal("nan"), Equal("NaN")))
	})

	It("should return 1 as default for COUNT reducer when no numeric values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestCount", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestCount")

		client.HSet(ctx, "doc1", "grp", "g1")

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchCount, As: "cnt"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestCount", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())

		Expect(res.Rows[0].Fields["cnt"]).To(BeEquivalentTo("1"))
	})

	It("should return NaN as default for SUM reducer when no numeric values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestSum", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestSum")

		client.HSet(ctx, "doc1", "grp", "g1")

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchSum, Args: []interface{}{"@n"}, As: "sum"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestSum", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())

		Expect(res.Rows[0].Fields["sum"]).To(SatisfyAny(Equal("nan"), Equal("NaN")))
	})

	It("should return the full requested number of results by re-running the query when some results expire", Label("search", "ftsearch"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggExpired", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "order", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggExpired")

		for i := 1; i <= 15; i++ {
			key := fmt.Sprintf("doc%d", i)
			_, err := client.HSet(ctx, key, "order", i).Result()
			Expect(err).NotTo(HaveOccurred())
		}

		_, err = client.Del(ctx, "doc3", "doc7").Result()
		Expect(err).NotTo(HaveOccurred())

		options := &redis.FTSearchOptions{
			SortBy:      []redis.FTSearchSortBy{{FieldName: "order", Asc: true}},
			LimitOffset: 0,
			Limit:       10,
		}
		res, err := client.FTSearchWithArgs(ctx, "aggExpired", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())

		Expect(len(res.Docs)).To(BeEquivalentTo(10))

		for _, doc := range res.Docs {
			Expect(doc.ID).ToNot(Or(Equal("doc3"), Equal("doc7")))
		}
	})

	It("should stop processing and return an error when a timeout occurs", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTimeoutHeavy", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTimeoutHeavy")

		const totalDocs = 100000
		for i := 0; i < totalDocs; i++ {
			key := fmt.Sprintf("doc%d", i)
			_, err := client.HSet(ctx, key, "n", i).Result()
			Expect(err).NotTo(HaveOccurred())
		}
		// default behaviour was changed in 8.0.1, set to fail to validate the timeout was triggered
		err = client.ConfigSet(ctx, "search-on-timeout", "fail").Err()
		Expect(err).NotTo(HaveOccurred())

		options := &redis.FTAggregateOptions{
			SortBy:      []redis.FTAggregateSortBy{{FieldName: "@n", Desc: true}},
			LimitOffset: 0,
			Limit:       100000,
			Timeout:     1, // 1 ms timeout, expected to trigger a timeout error.
		}
		_, err = client.FTAggregateWithArgs(ctx, "aggTimeoutHeavy", "*", options).Result()
		Expect(err).To(HaveOccurred())
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("timeout"))
	})

	It("should return 0 as default for COUNT_DISTINCT reducer when no values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestCountDistinct", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "x", FieldType: redis.SearchFieldTypeText},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestCountDistinct")

		client.HSet(ctx, "doc1", "grp", "g1")

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchCountDistinct, Args: []interface{}{"@x"}, As: "distinct_count"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}

		res, err := client.FTAggregateWithArgs(ctx, "aggTestCountDistinct", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())
		Expect(res.Rows[0].Fields["distinct_count"]).To(BeEquivalentTo("0"))
	})

	It("should return 0 as default for COUNT_DISTINCTISH reducer when no values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestCountDistinctIsh", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "y", FieldType: redis.SearchFieldTypeText},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestCountDistinctIsh")

		_, err = client.HSet(ctx, "doc1", "grp", "g1").Result()
		Expect(err).NotTo(HaveOccurred())

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchCountDistinctish, Args: []interface{}{"@y"}, As: "distinctish_count"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestCountDistinctIsh", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())
		Expect(res.Rows[0].Fields["distinctish_count"]).To(BeEquivalentTo("0"))
	})

	It("should use BM25 as the default scorer", Label("search", "ftsearch"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "scoringTest", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "scoringTest")

		_, err = client.HSet(ctx, "doc1", "description", "red apple").Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.HSet(ctx, "doc2", "description", "green apple").Result()
		Expect(err).NotTo(HaveOccurred())

		resDefault, err := client.FTSearchWithArgs(ctx, "scoringTest", "apple", &redis.FTSearchOptions{WithScores: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDefault.Total).To(BeNumerically(">", 0))

		resBM25, err := client.FTSearchWithArgs(ctx, "scoringTest", "apple", &redis.FTSearchOptions{WithScores: true, Scorer: "BM25"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resBM25.Total).To(BeNumerically(">", 0))
		Expect(resDefault.Total).To(BeEquivalentTo(resBM25.Total))
		Expect(resDefault.Docs[0].ID).To(BeElementOf("doc1", "doc2"))
		Expect(resDefault.Docs[1].ID).To(BeElementOf("doc1", "doc2"))
	})

	It("should return 0 as default for STDDEV reducer when no numeric values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestStddev", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestStddev")

		_, err = client.HSet(ctx, "doc1", "grp", "g1").Result()
		Expect(err).NotTo(HaveOccurred())

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchStdDev, Args: []interface{}{"@n"}, As: "stddev"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestStddev", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())

		Expect(res.Rows[0].Fields["stddev"]).To(BeEquivalentTo("0"))
	})

	It("should return NaN as default for QUANTILE reducer when no numeric values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestQuantile", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestQuantile")

		_, err = client.HSet(ctx, "doc1", "grp", "g1").Result()
		Expect(err).NotTo(HaveOccurred())

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchQuantile, Args: []interface{}{"@n", 0.5}, As: "quantile"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestQuantile", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())
		Expect(res.Rows[0].Fields["quantile"]).To(SatisfyAny(Equal("nan"), Equal("NaN")))
	})

	It("should return nil as default for FIRST_VALUE reducer when no values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestFirstValue", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeText},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestFirstValue")

		_, err = client.HSet(ctx, "doc1", "grp", "g1").Result()
		Expect(err).NotTo(HaveOccurred())

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchFirstValue, Args: []interface{}{"@t"}, As: "first_val"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestFirstValue", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())
		Expect(res.Rows[0].Fields["first_val"]).To(BeNil())
	})

	It("should fail to add an alias that is an existing index name", Label("search", "ftalias"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		val, err = client.FTCreate(ctx, "idx2", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx2")

		_, err = client.FTAliasAdd(ctx, "idx2", "idx1").Result()
		Expect(err).To(HaveOccurred())
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("alias"))
	})

	It("should test ft.search with CountOnly param", Label("search", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txtIndex", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txtIndex")

		_, err = client.HSet(ctx, "doc1", "txt", "hello world").Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.HSet(ctx, "doc2", "txt", "hello go").Result()
		Expect(err).NotTo(HaveOccurred())
		_, err = client.HSet(ctx, "doc3", "txt", "hello redis").Result()
		Expect(err).NotTo(HaveOccurred())

		optsCountOnly := &redis.FTSearchOptions{
			CountOnly:      true,
			LimitOffset:    0,
			Limit:          2, // even though we limit to 2, with count-only no docs are returned
			DialectVersion: 2,
		}
		resCountOnly, err := client.FTSearchWithArgs(ctx, "txtIndex", "hello", optsCountOnly).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resCountOnly.Total).To(BeEquivalentTo(3))
		Expect(len(resCountOnly.Docs)).To(BeEquivalentTo(0))

		optsLimit := &redis.FTSearchOptions{
			CountOnly:      false,
			LimitOffset:    0,
			Limit:          2, // we expect to get 2 documents even though total count is 3
			DialectVersion: 2,
		}
		resLimit, err := client.FTSearchWithArgs(ctx, "txtIndex", "hello", optsLimit).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resLimit.Total).To(BeEquivalentTo(3))
		Expect(len(resLimit.Docs)).To(BeEquivalentTo(2))
	})

	It("should reject deprecated configuration keys", Label("search", "ftconfig"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		// List of deprecated configuration keys.
		deprecatedKeys := []string{
			"_FREE_RESOURCE_ON_THREAD",
			"_NUMERIC_COMPRESS",
			"_NUMERIC_RANGES_PARENTS",
			"_PRINT_PROFILE_CLOCK",
			"_PRIORITIZE_INTERSECT_UNION_CHILDREN",
			"BG_INDEX_SLEEP_GAP",
			"CONN_PER_SHARD",
			"CURSOR_MAX_IDLE",
			"CURSOR_REPLY_THRESHOLD",
			"DEFAULT_DIALECT",
			"EXTLOAD",
			"FORK_GC_CLEAN_THRESHOLD",
			"FORK_GC_RETRY_INTERVAL",
			"FORK_GC_RUN_INTERVAL",
			"FORKGC_SLEEP_BEFORE_EXIT",
			"FRISOINI",
			"GC_POLICY",
			"GCSCANSIZE",
			"INDEX_CURSOR_LIMIT",
			"MAXAGGREGATERESULTS",
			"MAXDOCTABLESIZE",
			"MAXPREFIXEXPANSIONS",
			"MAXSEARCHRESULTS",
			"MIN_OPERATION_WORKERS",
			"MIN_PHONETIC_TERM_LEN",
			"MINPREFIX",
			"MINSTEMLEN",
			"NO_MEM_POOLS",
			"NOGC",
			"ON_TIMEOUT",
			"MULTI_TEXT_SLOP",
			"PARTIAL_INDEXED_DOCS",
			"RAW_DOCID_ENCODING",
			"SEARCH_THREADS",
			"TIERED_HNSW_BUFFER_LIMIT",
			"TIMEOUT",
			"TOPOLOGY_VALIDATION_TIMEOUT",
			"UNION_ITERATOR_HEAP",
			"VSS_MAX_RESIZE",
			"WORKERS",
			"WORKERS_PRIORITY_BIAS_THRESHOLD",
			"MT_MODE",
			"WORKER_THREADS",
		}

		for _, key := range deprecatedKeys {
			_, err := client.FTConfigSet(ctx, key, "test_value").Result()
			Expect(err).To(HaveOccurred())
		}

		val, err := client.ConfigGet(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		// Since FT.CONFIG is deprecated since redis 8, use CONFIG instead with new search parameters.
		keys := make([]string, 0, len(val))
		for key := range val {
			keys = append(keys, key)
		}
		Expect(keys).To(ContainElement(ContainSubstring("search")))
	})

	It("should return INF for MIN reducer and -INF for MAX reducer when no numeric values are present", Label("search", "ftaggregate"), func() {
		SkipBeforeRedisVersion(7.9, "requires Redis 8.x")
		val, err := client.FTCreate(ctx, "aggTestMinMax", &redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "grp", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric},
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "aggTestMinMax")

		_, err = client.HSet(ctx, "doc1", "grp", "g1").Result()
		Expect(err).NotTo(HaveOccurred())

		reducers := []redis.FTAggregateReducer{
			{Reducer: redis.SearchMin, Args: []interface{}{"@n"}, As: "minValue"},
			{Reducer: redis.SearchMax, Args: []interface{}{"@n"}, As: "maxValue"},
		}
		groupBy := []redis.FTAggregateGroupBy{
			{Fields: []interface{}{"@grp"}, Reduce: reducers},
		}
		options := &redis.FTAggregateOptions{GroupBy: groupBy}
		res, err := client.FTAggregateWithArgs(ctx, "aggTestMinMax", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows).ToNot(BeEmpty())

		Expect(res.Rows[0].Fields["minValue"]).To(BeEquivalentTo("inf"))
		Expect(res.Rows[0].Fields["maxValue"]).To(BeEquivalentTo("-inf"))
	})

})

func _assert_geosearch_result(result *redis.FTSearchResult, expectedDocIDs []string) {
	ids := make([]string, len(result.Docs))
	for i, doc := range result.Docs {
		ids[i] = doc.ID
	}
	Expect(ids).To(ConsistOf(expectedDocIDs))
	Expect(result.Total).To(BeEquivalentTo(len(expectedDocIDs)))
}

var _ = Describe("RediSearch FT.Config with Resp2 and Resp3", Label("search", "NonRedisEnterprise"), func() {

	var clientResp2 *redis.Client
	var clientResp3 *redis.Client
	BeforeEach(func() {
		clientResp2 = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 2})
		clientResp3 = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 3, UnstableResp3: true})
		Expect(clientResp3.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(clientResp2.Close()).NotTo(HaveOccurred())
		Expect(clientResp3.Close()).NotTo(HaveOccurred())
	})

	It("should FTConfigSet and FTConfigGet with resp2 and resp3", Label("search", "ftconfigget", "ftconfigset", "NonRedisEnterprise"), func() {
		val, err := clientResp3.FTConfigSet(ctx, "MINPREFIX", "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		res2, err := clientResp2.FTConfigGet(ctx, "MINPREFIX").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2).To(BeEquivalentTo(map[string]interface{}{"MINPREFIX": "1"}))

		res3, err := clientResp3.FTConfigGet(ctx, "MINPREFIX").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3).To(BeEquivalentTo(map[string]interface{}{"MINPREFIX": "1"}))
	})

	It("should FTConfigGet all resp2 and resp3", Label("search", "NonRedisEnterprise"), func() {
		res2, err := clientResp2.FTConfigGet(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())

		res3, err := clientResp3.FTConfigGet(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(res3)).To(BeEquivalentTo(len(res2)))
		Expect(res2["DEFAULT_DIALECT"]).To(BeEquivalentTo(res2["DEFAULT_DIALECT"]))
	})
})

var _ = Describe("RediSearch commands Resp 3", Label("search"), func() {
	ctx := context.TODO()
	var client *redis.Client
	var client2 *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 3, UnstableResp3: true})
		client2 = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 3})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should handle FTAggregate with Unstable RESP3 Search Module and without stability", Label("search", "ftcreate", "ftaggregate"), func() {
		text1 := &redis.FieldSchema{FieldName: "PrimaryKey", FieldType: redis.SearchFieldTypeText, Sortable: true}
		num1 := &redis.FieldSchema{FieldName: "CreatedDateTimeUTC", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "PrimaryKey", "9::362330", "CreatedDateTimeUTC", "637387878524969984")
		client.HSet(ctx, "doc2", "PrimaryKey", "9::362329", "CreatedDateTimeUTC", "637387875859270016")

		options := &redis.FTAggregateOptions{Apply: []redis.FTAggregateApply{{Field: "@CreatedDateTimeUTC * 10", As: "CreatedDateTimeUTC"}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).RawResult()
		results := res.(map[interface{}]interface{})["results"].([]interface{})
		Expect(results[0].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})["CreatedDateTimeUTC"]).
			To(Or(BeEquivalentTo("6373878785249699840"), BeEquivalentTo("6373878758592700416")))
		Expect(results[1].(map[interface{}]interface{})["extra_attributes"].(map[interface{}]interface{})["CreatedDateTimeUTC"]).
			To(Or(BeEquivalentTo("6373878785249699840"), BeEquivalentTo("6373878758592700416")))

		rawVal := client.FTAggregateWithArgs(ctx, "idx1", "*", options).RawVal()
		rawValResults := rawVal.(map[interface{}]interface{})["results"].([]interface{})
		Expect(err).NotTo(HaveOccurred())
		Expect(rawValResults[0]).To(Or(BeEquivalentTo(results[0]), BeEquivalentTo(results[1])))
		Expect(rawValResults[1]).To(Or(BeEquivalentTo(results[0]), BeEquivalentTo(results[1])))

		// Test with UnstableResp3 false
		Expect(func() {
			options = &redis.FTAggregateOptions{Apply: []redis.FTAggregateApply{{Field: "@CreatedDateTimeUTC * 10", As: "CreatedDateTimeUTC"}}}
			rawRes, _ := client2.FTAggregateWithArgs(ctx, "idx1", "*", options).RawResult()
			rawVal = client2.FTAggregateWithArgs(ctx, "idx1", "*", options).RawVal()
			Expect(rawRes).To(BeNil())
			Expect(rawVal).To(BeNil())
		}).Should(Panic())

	})

	It("should handle FTInfo with Unstable RESP3 Search Module and without stability", Label("search", "ftcreate", "ftinfo"), func() {
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText, Sortable: true, NoStem: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resInfo, err := client.FTInfo(ctx, "idx1").RawResult()
		Expect(err).NotTo(HaveOccurred())
		attributes := resInfo.(map[interface{}]interface{})["attributes"].([]interface{})
		flags := attributes[0].(map[interface{}]interface{})["flags"].([]interface{})
		Expect(flags).To(ConsistOf("SORTABLE", "NOSTEM"))

		valInfo := client.FTInfo(ctx, "idx1").RawVal()
		attributes = valInfo.(map[interface{}]interface{})["attributes"].([]interface{})
		flags = attributes[0].(map[interface{}]interface{})["flags"].([]interface{})
		Expect(flags).To(ConsistOf("SORTABLE", "NOSTEM"))

		// Test with UnstableResp3 false
		Expect(func() {
			rawResInfo, _ := client2.FTInfo(ctx, "idx1").RawResult()
			rawValInfo := client2.FTInfo(ctx, "idx1").RawVal()
			Expect(rawResInfo).To(BeNil())
			Expect(rawValInfo).To(BeNil())
		}).Should(Panic())
	})

	It("should handle FTSpellCheck with Unstable RESP3 Search Module and without stability", Label("search", "ftcreate", "ftspellcheck"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "f1", "some valid content", "f2", "this is sample text")
		client.HSet(ctx, "doc2", "f1", "very important", "f2", "lorem ipsum")

		resSpellCheck, err := client.FTSpellCheck(ctx, "idx1", "impornant").RawResult()
		valSpellCheck := client.FTSpellCheck(ctx, "idx1", "impornant").RawVal()
		Expect(err).NotTo(HaveOccurred())
		Expect(valSpellCheck).To(BeEquivalentTo(resSpellCheck))
		results := resSpellCheck.(map[interface{}]interface{})["results"].(map[interface{}]interface{})
		Expect(results["impornant"].([]interface{})[0].(map[interface{}]interface{})["important"]).To(BeEquivalentTo(0.5))

		// Test with UnstableResp3 false
		Expect(func() {
			rawResSpellCheck, _ := client2.FTSpellCheck(ctx, "idx1", "impornant").RawResult()
			rawValSpellCheck := client2.FTSpellCheck(ctx, "idx1", "impornant").RawVal()
			Expect(rawResSpellCheck).To(BeNil())
			Expect(rawValSpellCheck).To(BeNil())
		}).Should(Panic())
	})

	It("should handle FTSearch with Unstable RESP3 Search Module and without stability", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{StopWords: []interface{}{"foo", "bar", "baz"}}, &redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo baz")
		client.HSet(ctx, "doc2", "txt", "hello world")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "foo bar", &redis.FTSearchOptions{NoContent: true}).RawResult()
		val1 := client.FTSearchWithArgs(ctx, "txt", "foo bar", &redis.FTSearchOptions{NoContent: true}).RawVal()
		Expect(err).NotTo(HaveOccurred())
		Expect(val1).To(BeEquivalentTo(res1))
		totalResults := res1.(map[interface{}]interface{})["total_results"]
		Expect(totalResults).To(BeEquivalentTo(int64(0)))
		res2, err := client.FTSearchWithArgs(ctx, "txt", "foo bar hello world", &redis.FTSearchOptions{NoContent: true}).RawResult()
		Expect(err).NotTo(HaveOccurred())
		totalResults2 := res2.(map[interface{}]interface{})["total_results"]
		Expect(totalResults2).To(BeEquivalentTo(int64(1)))

		// Test with UnstableResp3 false
		Expect(func() {
			rawRes2, _ := client2.FTSearchWithArgs(ctx, "txt", "foo bar hello world", &redis.FTSearchOptions{NoContent: true}).RawResult()
			rawVal2 := client2.FTSearchWithArgs(ctx, "txt", "foo bar hello world", &redis.FTSearchOptions{NoContent: true}).RawVal()
			Expect(rawRes2).To(BeNil())
			Expect(rawVal2).To(BeNil())
		}).Should(Panic())
	})
	It("should handle FTSynDump with Unstable RESP3 Search Module and without stability", Label("search", "ftsyndump"), func() {
		text1 := &redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}
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

		resSynDump, err := client.FTSynDump(ctx, "idx1").RawResult()
		valSynDump := client.FTSynDump(ctx, "idx1").RawVal()
		Expect(err).NotTo(HaveOccurred())
		Expect(valSynDump).To(BeEquivalentTo(resSynDump))
		Expect(resSynDump.(map[interface{}]interface{})["baby"]).To(BeEquivalentTo([]interface{}{"id1"}))

		// Test with UnstableResp3 false
		Expect(func() {
			rawResSynDump, _ := client2.FTSynDump(ctx, "idx1").RawResult()
			rawValSynDump := client2.FTSynDump(ctx, "idx1").RawVal()
			Expect(rawResSynDump).To(BeNil())
			Expect(rawValSynDump).To(BeNil())
		}).Should(Panic())
	})

	It("should test not affected Resp 3 Search method - FTExplain", Label("search", "ftexplain"), func() {
		text1 := &redis.FieldSchema{FieldName: "f1", FieldType: redis.SearchFieldTypeText}
		text2 := &redis.FieldSchema{FieldName: "f2", FieldType: redis.SearchFieldTypeText}
		text3 := &redis.FieldSchema{FieldName: "f3", FieldType: redis.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "txt", &redis.FTCreateOptions{}, text1, text2, text3).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		res1, err := client.FTExplain(ctx, "txt", "@f3:f3_val @f2:f2_val @f1:f1_val").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1).ToNot(BeEmpty())

		// Test with UnstableResp3 false
		Expect(func() {
			res2, err := client2.FTExplain(ctx, "txt", "@f3:f3_val @f2:f2_val @f1:f1_val").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res2).ToNot(BeEmpty())
		}).ShouldNot(Panic())
	})
})
