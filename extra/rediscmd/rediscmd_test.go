package rediscmd

import (
	"context"
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "redisext")
}

var _ = Describe("AppendArg", func() {
	DescribeTable("...",
		func(src string, wanted string) {
			b := appendArg(nil, src)
			Expect(string(b)).To(Equal(wanted))
		},

		Entry("", "-inf", "-inf"),
		Entry("", "+inf", "+inf"),
		Entry("", "foo.bar", "foo.bar"),
		Entry("", "foo:bar", "foo:bar"),
		Entry("", "foo bar", "foo bar"),
		Entry("", "foo{bar}", "foo{bar}"),
		Entry("", "foo-123_BAR", "foo-123_BAR"),
		Entry("", "foo\nbar", "666f6f0a626172"),
		Entry("", "\000", "00"),
	)
})

var _ = Describe("CmdString", func() {
	DescribeTable("keeps credentials out of the rendered command",
		func(args []interface{}, wanted string) {
			Expect(CmdString(redis.NewCmd(context.Background(), args...))).To(Equal(wanted))
		},

		Entry("", []interface{}{"auth", "s3cret"}, "auth <redacted>"),
		Entry("", []interface{}{"auth", "alice", "s3cret"}, "auth alice <redacted>"),
		Entry("", []interface{}{"AUTH", "s3cret"}, "AUTH <redacted>"),
		Entry("", []interface{}{"hello", 3, "auth", "alice", "s3cret", "setname", "app"},
			"hello 3 auth alice <redacted> setname app"),
		Entry("", []interface{}{"hello", 3, "setname", "app"}, "hello 3 setname app"),
		Entry("", []interface{}{"config", "set", "requirepass", "s3cret"},
			"config set requirepass <redacted>"),
		Entry("", []interface{}{"config", "set", "maxmemory", "100mb", "masterauth", "s3cret"},
			"config set maxmemory 100mb masterauth <redacted>"),
		Entry("", []interface{}{"config", "get", "requirepass"}, "config get requirepass"),
		Entry("", []interface{}{"acl", "setuser", "bob", "on", ">s3cret", "~app:*", "+get"},
			"acl setuser bob on <redacted> ~app:* +get"),
		Entry("", []interface{}{"migrate", "h", "6379", "k", 0, 100, "auth", "s3cret"},
			"migrate h 6379 k 0 100 auth <redacted>"),
		Entry("", []interface{}{"migrate", "h", "6379", "", 0, 100, "keys", "auth", "k2"},
			"migrate h 6379  0 100 keys auth k2"),
		Entry("", []interface{}{"get", "key"}, "get key"),
		Entry("", []interface{}{"set", "auth", "value"}, "set auth value"),
	)
})
