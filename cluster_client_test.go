package redis

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func (c *ClusterClient) SlotAddrs(slot int) []string {
	var addrs []string
	for _, n := range c.slotNodes(slot) {
		addrs = append(addrs, n.Client.getAddr())
	}
	return addrs
}

// SwapSlot swaps a slot's master/slave address
// for testing MOVED redirects
func (c *ClusterClient) SwapSlotNodes(slot int) []string {
	c.mu.Lock()
	nodes := c.slots[slot]
	nodes[0], nodes[1] = nodes[1], nodes[0]
	c.mu.Unlock()
	return c.SlotAddrs(slot)
}

var _ = Describe("ClusterClient", func() {
	var subject *ClusterClient

	var populate = func() {
		subject.setSlots([]ClusterSlot{
			{0, 4095, []ClusterNode{{"", "127.0.0.1:7000"}, {"", "127.0.0.1:7004"}}},
			{12288, 16383, []ClusterNode{{"", "127.0.0.1:7003"}, {"", "127.0.0.1:7007"}}},
			{4096, 8191, []ClusterNode{{"", "127.0.0.1:7001"}, {"", "127.0.0.1:7005"}}},
			{8192, 12287, []ClusterNode{{"", "127.0.0.1:7002"}, {"", "127.0.0.1:7006"}}},
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
	})

	It("should update slots cache", func() {
		populate()
		Expect(subject.slots[0][0].Client.getAddr()).To(Equal("127.0.0.1:7000"))
		Expect(subject.slots[0][1].Client.getAddr()).To(Equal("127.0.0.1:7004"))
		Expect(subject.slots[4095][0].Client.getAddr()).To(Equal("127.0.0.1:7000"))
		Expect(subject.slots[4095][1].Client.getAddr()).To(Equal("127.0.0.1:7004"))
		Expect(subject.slots[4096][0].Client.getAddr()).To(Equal("127.0.0.1:7001"))
		Expect(subject.slots[4096][1].Client.getAddr()).To(Equal("127.0.0.1:7005"))
		Expect(subject.slots[8191][0].Client.getAddr()).To(Equal("127.0.0.1:7001"))
		Expect(subject.slots[8191][1].Client.getAddr()).To(Equal("127.0.0.1:7005"))
		Expect(subject.slots[8192][0].Client.getAddr()).To(Equal("127.0.0.1:7002"))
		Expect(subject.slots[8192][1].Client.getAddr()).To(Equal("127.0.0.1:7006"))
		Expect(subject.slots[12287][0].Client.getAddr()).To(Equal("127.0.0.1:7002"))
		Expect(subject.slots[12287][1].Client.getAddr()).To(Equal("127.0.0.1:7006"))
		Expect(subject.slots[12288][0].Client.getAddr()).To(Equal("127.0.0.1:7003"))
		Expect(subject.slots[12288][1].Client.getAddr()).To(Equal("127.0.0.1:7007"))
		Expect(subject.slots[16383][0].Client.getAddr()).To(Equal("127.0.0.1:7003"))
		Expect(subject.slots[16383][1].Client.getAddr()).To(Equal("127.0.0.1:7007"))
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
		Expect(subject.addrs).To(BeEmpty())
		Expect(subject.nodes).To(BeEmpty())
		Expect(subject.slots).To(BeEmpty())
		Expect(subject.Ping().Err().Error()).To(Equal("redis: client is closed"))
	})
})
