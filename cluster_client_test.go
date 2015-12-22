package redis

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func (c *ClusterClient) SlotAddrs(slot int) []string {
	return c.slotAddrs(slot)
}

// SwapSlot swaps a slot's master/slave address
// for testing MOVED redirects
func (c *ClusterClient) SwapSlot(pos int) []string {
	c.slotsMx.Lock()
	defer c.slotsMx.Unlock()
	c.slots[pos][0], c.slots[pos][1] = c.slots[pos][1], c.slots[pos][0]
	return c.slots[pos]
}

var _ = Describe("ClusterClient", func() {
	var subject *ClusterClient

	var populate = func() {
		subject.setSlots([]ClusterSlotInfo{
			{0, 4095, []string{"127.0.0.1:7000", "127.0.0.1:7004"}},
			{12288, 16383, []string{"127.0.0.1:7003", "127.0.0.1:7007"}},
			{4096, 8191, []string{"127.0.0.1:7001", "127.0.0.1:7005"}},
			{8192, 12287, []string{"127.0.0.1:7002", "127.0.0.1:7006"}},
		})
	}

	BeforeEach(func() {
		subject = NewClusterClient(&ClusterOptions{
			Addrs: []string{"127.0.0.1:6379", "127.0.0.1:7003", "127.0.0.1:7006"},
		})
	})

	AfterEach(func() {
		_ = subject.Close()
	})

	It("should initialize", func() {
		Expect(subject.addrs).To(HaveLen(3))
		Expect(subject.slots).To(HaveLen(16384))
	})

	It("should update slots cache", func() {
		populate()
		Expect(subject.slots[0]).To(Equal([]string{"127.0.0.1:7000", "127.0.0.1:7004"}))
		Expect(subject.slots[4095]).To(Equal([]string{"127.0.0.1:7000", "127.0.0.1:7004"}))
		Expect(subject.slots[4096]).To(Equal([]string{"127.0.0.1:7001", "127.0.0.1:7005"}))
		Expect(subject.slots[8191]).To(Equal([]string{"127.0.0.1:7001", "127.0.0.1:7005"}))
		Expect(subject.slots[8192]).To(Equal([]string{"127.0.0.1:7002", "127.0.0.1:7006"}))
		Expect(subject.slots[12287]).To(Equal([]string{"127.0.0.1:7002", "127.0.0.1:7006"}))
		Expect(subject.slots[12288]).To(Equal([]string{"127.0.0.1:7003", "127.0.0.1:7007"}))
		Expect(subject.slots[16383]).To(Equal([]string{"127.0.0.1:7003", "127.0.0.1:7007"}))
		Expect(subject.addrs).To(Equal([]string{
			"127.0.0.1:6379",
			"127.0.0.1:7003",
			"127.0.0.1:7006",
			"127.0.0.1:7000",
			"127.0.0.1:7004",
			"127.0.0.1:7007",
			"127.0.0.1:7001",
			"127.0.0.1:7005",
			"127.0.0.1:7002",
		}))
	})

	It("should close", func() {
		populate()
		Expect(subject.Close()).NotTo(HaveOccurred())
		Expect(subject.clients).To(BeEmpty())
		Expect(subject.slots[0]).To(BeEmpty())
		Expect(subject.slots[8191]).To(BeEmpty())
		Expect(subject.slots[8192]).To(BeEmpty())
		Expect(subject.slots[16383]).To(BeEmpty())
		Expect(subject.Ping().Err().Error()).To(Equal("redis: client is closed"))
	})
})
