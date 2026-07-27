package redis_test

import (
	"context"
	"fmt"
	"io"
	"net"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
)

type testTimeout struct {
	timeout bool
}

func (t testTimeout) Timeout() bool {
	return t.timeout
}

func (t testTimeout) Error() string {
	return "test timeout"
}

var _ = Describe("error", func() {
	BeforeEach(func() {
	})

	AfterEach(func() {
	})

	It("should retry", func() {
		data := map[error]bool{
			io.EOF:                   true,
			io.ErrUnexpectedEOF:      true,
			nil:                      false,
			context.Canceled:         false,
			context.DeadlineExceeded: false,
			redis.ErrPoolTimeout:     true,
			&net.OpError{Op: "dial"}: true,
			// Use typed errors instead of plain errors.New()
			proto.ParseErrorReply([]byte("-ERR max number of clients reached")):                                                                                   true,
			proto.ParseErrorReply([]byte("-LOADING Redis is loading the dataset in memory")):                                                                      true,
			proto.ParseErrorReply([]byte("-READONLY You can't write against a read only replica")):                                                                true,
			proto.ParseErrorReply([]byte("-ERR Error running script (call to f_abc123): @user_script:1: -READONLY You can't write against a read only replica.")): true,
			proto.ParseErrorReply([]byte("-CLUSTERDOWN The cluster is down")):                                                                                     true,
			proto.ParseErrorReply([]byte("-TRYAGAIN Command cannot be processed, please try again")):                                                              true,
			proto.ParseErrorReply([]byte("-NOREPLICAS Not enough good replicas to write")):                                                                        true,
			proto.ParseErrorReply([]byte("-ERR other")):                                                                                                           false,
			// Logical search timeout (search-on-timeout fail): never retried.
			proto.ParseErrorReply([]byte("-SEARCH_TIMEOUT Timeout limit was reached")): false,
		}

		for err, expected := range data {
			Expect(redis.ShouldRetry(err, false)).To(Equal(expected))
			Expect(redis.ShouldRetry(err, true)).To(Equal(expected))
		}
	})

	It("should retry timeout", func() {
		t1 := testTimeout{timeout: true}
		Expect(redis.ShouldRetry(t1, true)).To(Equal(true))
		Expect(redis.ShouldRetry(t1, false)).To(Equal(false))

		t2 := testTimeout{timeout: false}
		Expect(redis.ShouldRetry(t2, true)).To(Equal(true))
		Expect(redis.ShouldRetry(t2, false)).To(Equal(true))
	})

	It("should not retry a logical search timeout", func() {
		// -SEARCH_TIMEOUT (search-on-timeout fail) is never retried, plain or wrapped.
		err := proto.ParseErrorReply([]byte("-SEARCH_TIMEOUT Timeout limit was reached"))
		Expect(redis.ShouldRetry(err, false)).To(BeFalse())
		Expect(redis.ShouldRetry(err, true)).To(BeFalse())

		wrapped := fmt.Errorf("query failed: %w", err)
		Expect(redis.ShouldRetry(wrapped, false)).To(BeFalse())
		Expect(redis.ShouldRetry(wrapped, true)).To(BeFalse())
	})
})
