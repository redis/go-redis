package redis_test

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Client", func() {
	var client *redis.Client
	var graph = "test-graph"

	BeforeEach(func() {
		// redisgraph / falkordb
		opt := redisOptions()
		opt.Addr = ":36379"
		client = redis.NewClient(opt)
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("graph no-result", func() {
		res, err := client.GraphQuery(ctx, graph, "CREATE ()").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsResult()).To(BeFalse())
	})

	It("graph query result-basic", func() {
		query := "CREATE (:per {id: 1024, name: 'foo', pr: 3.14, success: true})"
		_, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())

		query = "MATCH (p:per {id: 1024}) RETURN p.id as id, p.name as name, p.pr as pr, p.success as success, p.non as non"
		res, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsResult()).To(BeTrue())
		Expect(res.Len()).To(Equal(1))

		row, err := res.Row()
		Expect(err).NotTo(HaveOccurred())
		Expect(row).To(HaveLen(5))
		Expect(row["id"].Int()).To(Equal(1024))
		Expect(row["name"].Int()).To(Equal("foo"))
		Expect(row["pr"].Float64()).To(Equal(3.14))
		Expect(row["success"].Bool()).To(BeTrue())
		Expect(row["non"].IsNil()).To(BeTrue())
	})

	It("graph query result-node", func() {
		query := "CREATE (:per {id: 1024, name: 'foo', pr: 3.14, success: true})"
		_, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())

		query = "MATCH (p:per {id: 1024}) RETURN p"
		res, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsResult()).To(BeTrue())
		Expect(res.Len()).To(Equal(1))

		row, err := res.Row()
		Expect(err).NotTo(HaveOccurred())
		Expect(row).To(HaveLen(1))

		node, ok := row["p"].Node()
		Expect(ok).To(BeTrue())

		Expect(node.Labels).To(Equal([]string{"per"}))
		Expect(node.Properties["id"].Int()).To(Equal(1024))
		Expect(node.Properties["name"].String()).To(Equal("foo"))
		Expect(node.Properties["pr"].Float64()).To(Equal(3.14))
		Expect(node.Properties["success"].Bool()).To(BeTrue())
	})

	It("graph query result-edge", func() {
		query := "CREATE (:per {id: 1024}) - [:FRIENDS {ts: 100, msg: 'txt-msg'}] -> (:per {id: 2048})"
		_, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())

		query = "MATCH (:per {id: 1024}) - [r:FRIENDS] -> (:per {id: 2048}) RETURN r"
		res, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsResult()).To(BeTrue())
		Expect(res.Len()).To(Equal(1))

		row, err := res.Row()
		Expect(err).NotTo(HaveOccurred())
		Expect(row).To(HaveLen(1))

		edge, ok := row["r"].Edge()
		Expect(ok).To(BeTrue())

		Expect(edge.Typ).To(Equal("FRIENDS"))
		Expect(edge.Properties["ts"].Int()).To(Equal(100))
		Expect(edge.Properties["msg"].String()).To(Equal("txt-msg"))
	})

	It("graph query no-row", func() {
		query := "MATCH (p:per {id: 999}) RETURN p.name"
		res, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.IsResult()).To(BeTrue())
		Expect(res.Len()).To(Equal(0))

		row, err := res.Row()
		Expect(err).To(Equal(redis.Nil))
		Expect(row).To(HaveLen(0))

		rows, err := res.Rows()
		Expect(err).To(Equal(redis.Nil))
		Expect(rows).To(HaveLen(0))
	})

	It("graph query rows", func() {
		query := "CREATE (:per {id: 1024}),(:per {id: 2048}),(:per {id: 4096})"
		_, err := client.GraphQuery(ctx, graph, query).Result()
		Expect(err).NotTo(HaveOccurred())

		query = "MATCH (p:per) return p.id as id"
		res, err := client.GraphQuery(ctx, graph, query).Result()

		row, err := res.Row()
		Expect(err).NotTo(HaveOccurred())
		Expect(row).To(HaveLen(1))
		Expect(row["id"].Int()).To(Equal(1024))

		rows, err := res.Rows()
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(3))
		Expect(rows[0]["id"].Int()).To(Equal(1024))
		Expect(rows[1]["id"].Int()).To(Equal(2048))
		Expect(rows[2]["id"].Int()).To(Equal(4096))
	})
})
