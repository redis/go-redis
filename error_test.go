package redis_test

import (
	"context"
	"errors"
	"io"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
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
			errors.New("ERR max number of clients reached"):                      true,
			errors.New("LOADING Redis is loading the dataset in memory"):         true,
			errors.New("READONLY You can't write against a read only replica"):   true,
			errors.New("CLUSTERDOWN The cluster is down"):                        true,
			errors.New("TRYAGAIN Command cannot be processed, please try again"): true,
			errors.New("other"): false,
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
})
