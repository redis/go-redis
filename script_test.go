package redis_test

import (
	"bitbucket.org/infectious/qp/Godeps/_workspace/src/gopkg.in/redis.v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Scripting", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewTCPClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should Eval", func() {
		eval := client.Eval(
			"return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
			[]string{"key1", "key2"},
			[]string{"first", "second"},
		)
		Expect(eval.Err()).NotTo(HaveOccurred())
		Expect(eval.Val()).To(Equal([]interface{}{"key1", "key2", "first", "second"}))

		eval = client.Eval(
			"return redis.call('set',KEYS[1],'bar')",
			[]string{"foo"},
			[]string{},
		)
		Expect(eval.Err()).NotTo(HaveOccurred())
		Expect(eval.Val()).To(Equal("OK"))

		eval = client.Eval("return 10", []string{}, []string{})
		Expect(eval.Err()).NotTo(HaveOccurred())
		Expect(eval.Val()).To(Equal(int64(10)))

		eval = client.Eval("return {1,2,{3,'Hello World!'}}", []string{}, []string{})
		Expect(eval.Err()).NotTo(HaveOccurred())
		Expect(eval.Val()).To(Equal([]interface{}{int64(1), int64(2), []interface{}{int64(3), "Hello World!"}}))
	})

	It("should EvalSha", func() {
		set := client.Set("foo", "bar")
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		eval := client.Eval("return redis.call('get','foo')", nil, nil)
		Expect(eval.Err()).NotTo(HaveOccurred())
		Expect(eval.Val()).To(Equal("bar"))

		evalSha := client.EvalSha("6b1bf486c81ceb7edf3c093f4c48582e38c0e791", nil, nil)
		Expect(evalSha.Err()).NotTo(HaveOccurred())
		Expect(evalSha.Val()).To(Equal("bar"))

		evalSha = client.EvalSha("ffffffffffffffffffffffffffffffffffffffff", nil, nil)
		Expect(evalSha.Err()).To(MatchError("NOSCRIPT No matching script. Please use EVAL."))
		Expect(evalSha.Val()).To(BeNil())
	})

	It("should ScriptExists", func() {
		scriptLoad := client.ScriptLoad("return 1")
		Expect(scriptLoad.Err()).NotTo(HaveOccurred())
		Expect(scriptLoad.Val()).To(Equal("e0e1f9fabfc9d4800c877a703b823ac0578ff8db"))

		scriptExists := client.ScriptExists(
			"e0e1f9fabfc9d4800c877a703b823ac0578ff8db",
			"ffffffffffffffffffffffffffffffffffffffff",
		)
		Expect(scriptExists.Err()).NotTo(HaveOccurred())
		Expect(scriptExists.Val()).To(Equal([]bool{true, false}))
	})

	It("should ScriptFlush", func() {
		scriptFlush := client.ScriptFlush()
		Expect(scriptFlush.Err()).NotTo(HaveOccurred())
		Expect(scriptFlush.Val()).To(Equal("OK"))
	})

	It("should ScriptKill", func() {
		scriptKill := client.ScriptKill()
		Expect(scriptKill.Err()).To(HaveOccurred())
		Expect(scriptKill.Err().Error()).To(ContainSubstring("No scripts in execution right now."))
		Expect(scriptKill.Val()).To(Equal(""))
	})

	It("should ScriptLoad", func() {
		scriptLoad := client.ScriptLoad("return redis.call('get','foo')")
		Expect(scriptLoad.Err()).NotTo(HaveOccurred())
		Expect(scriptLoad.Val()).To(Equal("6b1bf486c81ceb7edf3c093f4c48582e38c0e791"))
	})

	It("should NewScript", func() {
		s := redis.NewScript("return 1")
		run := s.Run(client, nil, nil)
		Expect(run.Err()).NotTo(HaveOccurred())
		Expect(run.Val()).To(Equal(int64(1)))
	})

	It("should EvalAndPipeline", func() {
		pipeline := client.Pipeline()
		s := redis.NewScript("return 1")
		run := s.Eval(pipeline, nil, nil)
		_, err := pipeline.Exec()
		Expect(err).NotTo(HaveOccurred())
		Expect(run.Err()).NotTo(HaveOccurred())
		Expect(run.Val()).To(Equal(int64(1)))
	})

	It("should EvalSha in Pipeline", func() {
		s := redis.NewScript("return 1")
		Expect(s.Load(client).Err()).NotTo(HaveOccurred())

		pipeline := client.Pipeline()
		run := s.Eval(pipeline, nil, nil)
		_, err := pipeline.Exec()
		Expect(err).NotTo(HaveOccurred())
		Expect(run.Err()).NotTo(HaveOccurred())
		Expect(run.Val()).To(Equal(int64(1)))
	})

})
