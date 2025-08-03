package redis_test

import (
	"context"
	"fmt"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("RediSearch Builders", Label("search", "builders"), func() {
	ctx := context.Background()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 2})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		expectCloseErr := client.Close()
		Expect(expectCloseErr).NotTo(HaveOccurred())
	})

	It("should create index and search with scores using builders", Label("search", "ftcreate", "ftsearch"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx1").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "foo", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))

		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "foo", "hello world")
		client.HSet(ctx, "doc2", "foo", "hello redis")

		res, err := client.NewSearchBuilder(ctx, "idx1", "hello").WithScores().Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(Equal(2))
		for _, doc := range res.Docs {
			Expect(*doc.Score).To(BeNumerically(">", 0))
		}
	})

	It("should aggregate using builders", Label("search", "ftaggregate"), func() {
		_, err := client.NewCreateIndexBuilder(ctx, "idx2").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		WaitForIndexing(client, "idx2")

		client.HSet(ctx, "d1", "n", 1)
		client.HSet(ctx, "d2", "n", 2)

		agg, err := client.NewAggregateBuilder(ctx, "idx2", "*").
			GroupBy("@n").
			ReduceAs(redis.SearchCount, "count").
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(agg.Rows)).To(Equal(2))
	})

	It("should drop index using builder", Label("search", "ftdropindex"), func() {
		Expect(client.NewCreateIndexBuilder(ctx, "idx3").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "x", FieldType: redis.SearchFieldTypeText}).
			Run()).To(Equal("OK"))
		WaitForIndexing(client, "idx3")

		dropVal, err := client.NewDropIndexBuilder(ctx, "idx3").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(dropVal).To(Equal("OK"))
	})

	It("should manage aliases using builder", Label("search", "ftalias"), func() {
		Expect(client.NewCreateIndexBuilder(ctx, "idx4").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeText}).
			Run()).To(Equal("OK"))
		WaitForIndexing(client, "idx4")

		addVal, err := client.NewAliasBuilder(ctx, "alias1").Add("idx4").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(addVal).To(Equal("OK"))

		_, err = client.NewSearchBuilder(ctx, "alias1", "*").Run()
		Expect(err).NotTo(HaveOccurred())

		delVal, err := client.NewAliasBuilder(ctx, "alias1").Del().Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(delVal).To(Equal("OK"))
	})

	It("should explain query using ExplainBuilder", Label("search", "builders", "ftexplain"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_explain").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "foo", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_explain")

		expl, err := client.NewExplainBuilder(ctx, "idx_explain", "foo").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(expl).To(ContainSubstring("UNION"))
	})

	It("should retrieve info using SearchInfo builder", Label("search", "builders", "ftinfo"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_info").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "foo", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_info")

		i, err := client.NewSearchInfoBuilder(ctx, "idx_info").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(i.IndexName).To(Equal("idx_info"))
	})

	It("should spellcheck using builder", Label("search", "builders", "ftspellcheck"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_spell").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "foo", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_spell")

		client.HSet(ctx, "doc1", "foo", "bar")

		_, err = client.NewSpellCheckBuilder(ctx, "idx_spell", "ba").Distance(1).Run()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should manage dictionary using DictBuilder", Label("search", "ftdict"), func() {
		addCount, err := client.NewDictBuilder(ctx, "dict1").Add("a", "b").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(addCount).To(Equal(int64(2)))

		dump, err := client.NewDictBuilder(ctx, "dict1").Dump().Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(dump).To(ContainElements("a", "b"))

		delCount, err := client.NewDictBuilder(ctx, "dict1").Del("a").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(delCount).To(Equal(int64(1)))
	})

	It("should tag values using TagValsBuilder", Label("search", "builders", "fttagvals"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_tag").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "tags", FieldType: redis.SearchFieldTypeTag}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_tag")

		client.HSet(ctx, "doc1", "tags", "red,blue")
		client.HSet(ctx, "doc2", "tags", "green,blue")

		vals, err := client.NewTagValsBuilder(ctx, "idx_tag", "tags").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(BeAssignableToTypeOf([]string{}))
	})

	It("should cursor read and delete using CursorBuilder", Label("search", "builders", "ftcursor"), func() {
		Expect(client.NewCreateIndexBuilder(ctx, "idx5").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "f", FieldType: redis.SearchFieldTypeText}).
			Run()).To(Equal("OK"))
		WaitForIndexing(client, "idx5")
		client.HSet(ctx, "doc1", "f", "hello")
		client.HSet(ctx, "doc2", "f", "world")

		cursorBuilder := client.NewCursorBuilder(ctx, "idx5", 1)
		Expect(cursorBuilder).NotTo(BeNil())

		cursorBuilder = cursorBuilder.Count(10)
		Expect(cursorBuilder).NotTo(BeNil())

		delBuilder := client.NewCursorBuilder(ctx, "idx5", 1)
		Expect(delBuilder).NotTo(BeNil())
	})

	It("should update synonyms using SynUpdateBuilder", Label("search", "builders", "ftsynupdate"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_syn").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "foo", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_syn")

		syn, err := client.NewSynUpdateBuilder(ctx, "idx_syn", "grp1").Terms("a", "b").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(syn).To(Equal("OK"))
	})

	It("should test SearchBuilder with NoContent and Verbatim", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_nocontent").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText, Weight: 5}).
			Schema(&redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_nocontent")

		client.HSet(ctx, "doc1", "title", "RediSearch", "body", "Redisearch implements a search engine on top of redis")

		res, err := client.NewSearchBuilder(ctx, "idx_nocontent", "search engine").
			NoContent().
			Verbatim().
			Limit(0, 5).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(Equal(1))
		Expect(res.Docs[0].ID).To(Equal("doc1"))
		// NoContent means no fields should be returned
		Expect(res.Docs[0].Fields).To(BeEmpty())
	})

	It("should test SearchBuilder with NoStopWords", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_nostop").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_nostop")

		client.HSet(ctx, "doc1", "txt", "hello world")
		client.HSet(ctx, "doc2", "txt", "test document")

		// Test that NoStopWords method can be called and search works
		res, err := client.NewSearchBuilder(ctx, "idx_nostop", "hello").NoContent().NoStopWords().Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(Equal(1))
	})

	It("should test SearchBuilder with filters", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_filters").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).
			Schema(&redis.FieldSchema{FieldName: "num", FieldType: redis.SearchFieldTypeNumeric}).
			Schema(&redis.FieldSchema{FieldName: "loc", FieldType: redis.SearchFieldTypeGeo}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_filters")

		client.HSet(ctx, "doc1", "txt", "foo bar", "num", 3.141, "loc", "-0.441,51.458")
		client.HSet(ctx, "doc2", "txt", "foo baz", "num", 2, "loc", "-0.1,51.2")

		// Test numeric filter
		res1, err := client.NewSearchBuilder(ctx, "idx_filters", "foo").
			Filter("num", 2, 4).
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(2))

		// Test geo filter
		res2, err := client.NewSearchBuilder(ctx, "idx_filters", "foo").
			GeoFilter("loc", -0.44, 51.45, 10, "km").
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(1))
	})

	It("should test SearchBuilder with sorting", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_sort").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).
			Schema(&redis.FieldSchema{FieldName: "num", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_sort")

		client.HSet(ctx, "doc1", "txt", "foo bar", "num", 1)
		client.HSet(ctx, "doc2", "txt", "foo baz", "num", 2)
		client.HSet(ctx, "doc3", "txt", "foo qux", "num", 3)

		// Test ascending sort
		res1, err := client.NewSearchBuilder(ctx, "idx_sort", "foo").
			SortBy("num", true).
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(3))
		Expect(res1.Docs[0].ID).To(Equal("doc1"))
		Expect(res1.Docs[1].ID).To(Equal("doc2"))
		Expect(res1.Docs[2].ID).To(Equal("doc3"))

		// Test descending sort
		res2, err := client.NewSearchBuilder(ctx, "idx_sort", "foo").
			SortBy("num", false).
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(3))
		Expect(res2.Docs[0].ID).To(Equal("doc3"))
		Expect(res2.Docs[1].ID).To(Equal("doc2"))
		Expect(res2.Docs[2].ID).To(Equal("doc1"))
	})

	It("should test SearchBuilder with InKeys and InFields", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_in").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}).
			Schema(&redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_in")

		client.HSet(ctx, "doc1", "title", "hello world", "body", "lorem ipsum")
		client.HSet(ctx, "doc2", "title", "foo bar", "body", "hello world")
		client.HSet(ctx, "doc3", "title", "baz qux", "body", "dolor sit")

		// Test InKeys
		res1, err := client.NewSearchBuilder(ctx, "idx_in", "hello").
			InKeys("doc1", "doc2").
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(2))

		// Test InFields
		res2, err := client.NewSearchBuilder(ctx, "idx_in", "hello").
			InFields("title").
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(1))
		Expect(res2.Docs[0].ID).To(Equal("doc1"))
	})

	It("should test SearchBuilder with Return fields", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_return").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}).
			Schema(&redis.FieldSchema{FieldName: "body", FieldType: redis.SearchFieldTypeText}).
			Schema(&redis.FieldSchema{FieldName: "num", FieldType: redis.SearchFieldTypeNumeric}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_return")

		client.HSet(ctx, "doc1", "title", "hello", "body", "world", "num", 42)

		// Test ReturnFields
		res1, err := client.NewSearchBuilder(ctx, "idx_return", "hello").
			ReturnFields("title", "num").
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(1))
		Expect(res1.Docs[0].Fields).To(HaveKey("title"))
		Expect(res1.Docs[0].Fields).To(HaveKey("num"))
		Expect(res1.Docs[0].Fields).NotTo(HaveKey("body"))

		// Test ReturnAs
		res2, err := client.NewSearchBuilder(ctx, "idx_return", "hello").
			ReturnAs("title", "doc_title").
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(1))
		Expect(res2.Docs[0].Fields).To(HaveKey("doc_title"))
		Expect(res2.Docs[0].Fields).NotTo(HaveKey("title"))
	})

	It("should test SearchBuilder with advanced options", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_advanced").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_advanced")

		client.HSet(ctx, "doc1", "description", "The quick brown fox jumps over the lazy dog")
		client.HSet(ctx, "doc2", "description", "Quick alice was beginning to get very tired of sitting by her quick sister on the bank")

		// Test with scores and different scorers
		res1, err := client.NewSearchBuilder(ctx, "idx_advanced", "quick").
			WithScores().
			Scorer("TFIDF").
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(2))
		for _, doc := range res1.Docs {
			Expect(*doc.Score).To(BeNumerically(">", 0))
		}

		res2, err := client.NewSearchBuilder(ctx, "idx_advanced", "quick").
			WithScores().
			Payload("test_payload").
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(2))

		// Test with Slop and InOrder
		res3, err := client.NewSearchBuilder(ctx, "idx_advanced", "quick brown").
			Slop(1).
			InOrder().
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Total).To(Equal(1))

		// Test with Language and Expander
		res4, err := client.NewSearchBuilder(ctx, "idx_advanced", "quick").
			Language("english").
			Expander("SYNONYM").
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res4.Total).To(BeNumerically(">=", 0))

		// Test with Timeout
		res5, err := client.NewSearchBuilder(ctx, "idx_advanced", "quick").
			Timeout(1000).
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res5.Total).To(Equal(2))
	})

	It("should test SearchBuilder with Params and Dialect", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_params").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "name", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_params")

		client.HSet(ctx, "doc1", "name", "Alice")
		client.HSet(ctx, "doc2", "name", "Bob")
		client.HSet(ctx, "doc3", "name", "Carol")

		// Test with single param
		res1, err := client.NewSearchBuilder(ctx, "idx_params", "@name:$name").
			Param("name", "Alice").
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(1))
		Expect(res1.Docs[0].ID).To(Equal("doc1"))

		// Test with multiple params using ParamsMap
		params := map[string]interface{}{
			"name1": "Bob",
			"name2": "Carol",
		}
		res2, err := client.NewSearchBuilder(ctx, "idx_params", "@name:($name1|$name2)").
			ParamsMap(params).
			Dialect(2).
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(2))
	})

	It("should test SearchBuilder with Limit and CountOnly", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_limit").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_limit")

		for i := 1; i <= 10; i++ {
			client.HSet(ctx, fmt.Sprintf("doc%d", i), "txt", "test document")
		}

		// Test with Limit
		res1, err := client.NewSearchBuilder(ctx, "idx_limit", "test").
			Limit(2, 3).
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(10))
		Expect(len(res1.Docs)).To(Equal(3))

		// Test with CountOnly
		res2, err := client.NewSearchBuilder(ctx, "idx_limit", "test").
			CountOnly().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(10))
		Expect(len(res2.Docs)).To(Equal(0))
	})

	It("should test SearchBuilder with WithSortByCount and SortBy", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_payloads").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).
			Schema(&redis.FieldSchema{FieldName: "num", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_payloads")

		client.HSet(ctx, "doc1", "txt", "hello", "num", 1)
		client.HSet(ctx, "doc2", "txt", "world", "num", 2)

		// Test WithSortByCount and SortBy
		res, err := client.NewSearchBuilder(ctx, "idx_payloads", "*").
			SortBy("num", true).
			WithSortByCount().
			NoContent().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(Equal(2))
	})

	It("should test SearchBuilder with JSON", Label("search", "ftsearch", "builders", "json"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_json").
			OnJSON().
			Prefix("king:").
			Schema(&redis.FieldSchema{FieldName: "$.name", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_json")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry"}`)
		client.JSONSet(ctx, "king:2", "$", `{"name": "james"}`)

		res, err := client.NewSearchBuilder(ctx, "idx_json", "henry").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(Equal(1))
		Expect(res.Docs[0].ID).To(Equal("king:1"))
		Expect(res.Docs[0].Fields["$"]).To(Equal(`{"name":"henry"}`))
	})

	It("should test SearchBuilder with vector search", Label("search", "ftsearch", "builders", "vector"), func() {
		hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_vector").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "v", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_vector")

		client.HSet(ctx, "a", "v", "aaaaaaaa")
		client.HSet(ctx, "b", "v", "aaaabaaa")
		client.HSet(ctx, "c", "v", "aaaaabaa")

		res, err := client.NewSearchBuilder(ctx, "idx_vector", "*=>[KNN 2 @v $vec]").
			ReturnFields("__v_score").
			SortBy("__v_score", true).
			Dialect(2).
			Param("vec", "aaaaaaaa").
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(Equal("a"))
		Expect(res.Docs[0].Fields["__v_score"]).To(Equal("0"))
	})

	It("should test SearchBuilder with complex filtering and aggregation", Label("search", "ftsearch", "builders"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_complex").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "category", FieldType: redis.SearchFieldTypeTag}).
			Schema(&redis.FieldSchema{FieldName: "price", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}).
			Schema(&redis.FieldSchema{FieldName: "location", FieldType: redis.SearchFieldTypeGeo}).
			Schema(&redis.FieldSchema{FieldName: "description", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_complex")

		client.HSet(ctx, "product1", "category", "electronics", "price", 100, "location", "-0.1,51.5", "description", "smartphone device")
		client.HSet(ctx, "product2", "category", "electronics", "price", 200, "location", "-0.2,51.6", "description", "laptop computer")
		client.HSet(ctx, "product3", "category", "books", "price", 20, "location", "-0.3,51.7", "description", "programming guide")

		res, err := client.NewSearchBuilder(ctx, "idx_complex", "@category:{electronics} @description:(device|computer)").
			Filter("price", 50, 250).
			GeoFilter("location", -0.15, 51.55, 50, "km").
			SortBy("price", true).
			ReturnFields("category", "price", "description").
			Limit(0, 10).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeNumerically(">=", 1))

		res2, err := client.NewSearchBuilder(ctx, "idx_complex", "@category:{$cat} @price:[$min $max]").
			ParamsMap(map[string]interface{}{
				"cat": "electronics",
				"min": 150,
				"max": 300,
			}).
			Dialect(2).
			WithScores().
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(1))
		Expect(res2.Docs[0].ID).To(Equal("product2"))
	})

	It("should test SearchBuilder error handling and edge cases", Label("search", "ftsearch", "builders", "edge-cases"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_edge").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "txt", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_edge")

		client.HSet(ctx, "doc1", "txt", "hello world")

		// Test empty query
		res1, err := client.NewSearchBuilder(ctx, "idx_edge", "*").NoContent().Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(Equal(1))

		// Test query with no results
		res2, err := client.NewSearchBuilder(ctx, "idx_edge", "nonexistent").NoContent().Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(Equal(0))

		// Test with multiple chained methods
		res3, err := client.NewSearchBuilder(ctx, "idx_edge", "hello").
			WithScores().
			NoContent().
			Verbatim().
			InOrder().
			Slop(0).
			Timeout(5000).
			Language("english").
			Dialect(2).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Total).To(Equal(1))
	})

	It("should test SearchBuilder method chaining", Label("search", "ftsearch", "builders", "fluent"), func() {
		createVal, err := client.NewCreateIndexBuilder(ctx, "idx_fluent").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText}).
			Schema(&redis.FieldSchema{FieldName: "tags", FieldType: redis.SearchFieldTypeTag}).
			Schema(&redis.FieldSchema{FieldName: "score", FieldType: redis.SearchFieldTypeNumeric, Sortable: true}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))
		WaitForIndexing(client, "idx_fluent")

		client.HSet(ctx, "doc1", "title", "Redis Search Tutorial", "tags", "redis,search,tutorial", "score", 95)
		client.HSet(ctx, "doc2", "title", "Advanced Redis", "tags", "redis,advanced", "score", 88)

		builder := client.NewSearchBuilder(ctx, "idx_fluent", "@title:(redis) @tags:{search}")
		result := builder.
			WithScores().
			Filter("score", 90, 100).
			SortBy("score", false).
			ReturnFields("title", "score").
			Limit(0, 5).
			Dialect(2).
			Timeout(1000).
			Language("english")

		res, err := result.Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(Equal(1))
		Expect(res.Docs[0].ID).To(Equal("doc1"))
		Expect(res.Docs[0].Fields["title"]).To(Equal("Redis Search Tutorial"))
		Expect(*res.Docs[0].Score).To(BeNumerically(">", 0))
	})
})
