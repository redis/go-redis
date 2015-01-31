package redis

import (
	"sort"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("clusterPool", func() {

	var subject *clusterPool
	var populate = func() {
		subject.reset()
		subject.update([]ClusterSlotInfo{
			{0, 4095, []string{"127.0.0.1:7000", "127.0.0.1:7004"}},
			{12288, 16383, []string{"127.0.0.1:7003", "127.0.0.1:7007"}},
			{4096, 8191, []string{"127.0.0.1:7001", "127.0.0.1:7005"}},
			{8192, 12287, []string{"127.0.0.1:7002", "127.0.0.1:7006"}},
		})
	}

	BeforeEach(func() {
		var err error
		subject, err = newClusterPool(&ClusterOptions{
			Addrs: []string{"127.0.0.1:6379", "127.0.0.1:7003", "127.0.0.1:7006"},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should initialize", func() {
		Expect(subject.addrs).To(HaveLen(3))
		Expect(subject.slots).To(HaveLen(HashSlots))
		Expect(subject.conns).To(BeEmpty())
		Expect(subject._forceReload).To(Equal(uint32(1)))
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
		Expect(subject.addrs).To(ConsistOf([]string{
			"127.0.0.1:6379",
			"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003",
			"127.0.0.1:7004", "127.0.0.1:7005", "127.0.0.1:7006", "127.0.0.1:7007",
		}))
	})

	It("should find the current master address of a slot", func() {
		Expect(subject.master(1000)).To(Equal(""))
		populate()
		Expect(subject.master(1000)).To(Equal("127.0.0.1:7000"))
	})

	It("should find next addresses", func() {
		populate()
		seen := map[string]struct{}{
			"127.0.0.1:7000": struct{}{},
			"127.0.0.1:7001": struct{}{},
			"127.0.0.1:7003": struct{}{},
		}
		sort.Strings(subject.addrs)

		Expect(subject.next(seen)).To(Equal("127.0.0.1:6379"))
		seen["127.0.0.1:6379"] = struct{}{}
		Expect(subject.next(seen)).To(Equal("127.0.0.1:7002"))
		seen["127.0.0.1:7002"] = struct{}{}
		Expect(subject.next(seen)).To(Equal("127.0.0.1:7004"))
		seen["127.0.0.1:7004"] = struct{}{}
		Expect(subject.next(seen)).To(Equal("127.0.0.1:7005"))
		seen["127.0.0.1:7005"] = struct{}{}
		Expect(subject.next(seen)).To(Equal("127.0.0.1:7006"))
		seen["127.0.0.1:7006"] = struct{}{}
		Expect(subject.next(seen)).To(Equal("127.0.0.1:7007"))
		seen["127.0.0.1:7007"] = struct{}{}
		Expect(subject.next(seen)).To(Equal(""))
	})

	It("should check if reload is due", func() {
		Expect(subject.reloadDue()).To(BeTrue())
		Expect(subject.reloadDue()).To(BeFalse())
		subject.forceReload()
		Expect(subject.reloadDue()).To(BeTrue())
		Expect(subject.reloadDue()).To(BeFalse())
	})
})
