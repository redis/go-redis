package rediscmd

import (
	"context"
	"strings"
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
		Entry("", []interface{}{"migrate", "h", "6379", "k", 0, 100, "auth2", "alice", "s3cret"},
			"migrate h 6379 k 0 100 auth2 alice <redacted>"),
		Entry("", []interface{}{"migrate", "h", "6379", "", 0, 100, "keys", "auth", "k2"},
			"migrate h 6379  0 100 keys auth k2"),
		// A password equal to a keyword must not shift the redacted position.
		Entry("", []interface{}{"migrate", "h", "6379", "", 0, 100, "auth", "auth", "keys", "k1", "k2"},
			"migrate h 6379  0 100 auth <redacted> keys k1 k2"),
		Entry("", []interface{}{"migrate", "h", "6379", "", 0, 100, "auth2", "alice", "auth", "keys", "k1"},
			"migrate h 6379  0 100 auth2 alice <redacted> keys k1"),
		Entry("", []interface{}{"sentinel", "set", "mymaster", "auth-pass", "s3cret"},
			"sentinel set mymaster auth-pass <redacted>"),
		Entry("", []interface{}{"sentinel", "set", "mymaster", "down-after-milliseconds", "5000", "auth-pass", "s3cret"},
			"sentinel set mymaster down-after-milliseconds 5000 auth-pass <redacted>"),
		Entry("", []interface{}{"sentinel", "set", "mymaster", "auth-user", "alice"},
			"sentinel set mymaster auth-user alice"),
		// rename-command takes two values, which shifts the option parity.
		Entry("", []interface{}{"sentinel", "set", "mymaster", "rename-command", "CONFIG", "cfg", "auth-pass", "s3cret"},
			"sentinel set mymaster rename-command CONFIG cfg auth-pass <redacted>"),
		Entry("", []interface{}{"get", "key"}, "get key"),
		Entry("", []interface{}{"set", "auth", "value"}, "set auth value"),
	)

	It("redacts custom Stringer types without leaking credential", func() {
		Expect(CmdString(redis.NewCmd(context.Background(), "SET", "k", testSecret("hunter2")))).To(Equal("SET k ***"))
		Expect(CmdStringLimit(redis.NewCmd(context.Background(), "SET", "k", testSecret("hunter2")), 64)).To(Equal("SET k ***"))
	})

	It("truncates long arguments with limit", func() {
		longVal := "hello" + strings.Repeat("x", 100)
		cmd := redis.NewCmd(context.Background(), "SET", "k", longVal)
		Expect(CmdStringLimit(cmd, 16)).To(Equal("SET k helloxxxxx…(+95 bytes)"))
	})

	It("truncates long pipeline commands with limit", func() {
		cmd1 := redis.NewCmd(context.Background(), "SET", "k1", "v1")
		cmd2 := redis.NewCmd(context.Background(), "SET", "k2", strings.Repeat("a", 100))
		summary, stmt := CmdsStringLimit([]redis.Cmder{cmd1, cmd2}, 24)
		Expect(summary).To(Equal("set"))
		Expect(stmt).To(ContainSubstring("…"))
	})

	It("safely recovers from panicking Stringer methods", func() {
		cmd := redis.NewCmd(context.Background(), "SET", "k", panicStringer{})
		Expect(CmdString(cmd)).To(ContainSubstring("PANIC=String method: test panic"))
		Expect(CmdStringLimit(cmd, 64)).To(ContainSubstring("PANIC=String method"))
	})

	It("returns command summary via CmdsSummary without formatting arguments", func() {
		cmd1 := redis.NewCmd(context.Background(), "SET", "k", "v")
		cmd2 := redis.NewCmd(context.Background(), "GET", "k")
		cmd3 := redis.NewCmd(context.Background(), "SET", "k2", "v2")
		Expect(CmdsSummary([]redis.Cmder{cmd1, cmd2, cmd3})).To(Equal("set get"))
	})

	It("uses bounded placeholder for non-struct custom types when truncated", func() {
		cmd := redis.NewCmd(context.Background(), "SET", "k", testBlob{})
		Expect(CmdStringLimit(cmd, 64)).To(Equal("SET k <rediscmd.testBlob>"))
	})
})

type testSecret string

func (s testSecret) String() string {
	return "***"
}

func (s testSecret) MarshalBinary() ([]byte, error) {
	return []byte(s), nil
}

type panicStringer struct{}

func (panicStringer) String() string {
	panic("test panic")
}

func (panicStringer) MarshalBinary() ([]byte, error) {
	return []byte("safe"), nil
}

type testBlob [1024]byte

func (b testBlob) MarshalBinary() ([]byte, error) {
	return b[:], nil
}

