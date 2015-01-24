package redis

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CRC16", func() {

	// http://redis.io/topics/cluster-spec#keys-distribution-model
	It("should calculate CRC16", func() {
		tests := []struct {
			s string
			n uint16
		}{
			{"123456789", 0x31C3},
			{string([]byte{83, 153, 134, 118, 229, 214, 244, 75, 140, 37, 215, 215}), 21847},
		}

		for _, test := range tests {
			Expect(crc16sum(test.s)).To(Equal(test.n), "for %s", test.s)
		}
	})

})
