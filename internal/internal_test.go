package internal

import (
	"testing"
	. "github.com/onsi/gomega"
)

func TestRetryBackoff(t *testing.T) {
	RegisterTestingT(t)
	
	for i := -1; i<= 8; i++ {
		backoff := RetryBackoff(i)
		Expect(backoff >= 0).To(BeTrue())
		Expect(backoff <= maxRetryBackoff).To(BeTrue())
	}
}
