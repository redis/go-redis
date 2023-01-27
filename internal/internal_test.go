package internal

import (
	"testing"
	"time"

	. "github.com/bsm/gomega"
)

func TestRetryBackoff(t *testing.T) {
	RegisterTestingT(t)

	for i := 0; i <= 16; i++ {
		backoff := RetryBackoff(i, time.Millisecond, 512*time.Millisecond)
		Expect(backoff >= 0).To(BeTrue())
		Expect(backoff <= 512*time.Millisecond).To(BeTrue())
	}
}
