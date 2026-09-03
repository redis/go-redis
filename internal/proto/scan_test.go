package proto_test

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9/internal/proto"
)

type testScanSliceStruct struct {
	ID   int
	Name string
}

func (s *testScanSliceStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

func (s *testScanSliceStruct) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, s)
}

var _ = Describe("ScanSlice", func() {
	data := []string{
		`{"ID":-1,"Name":"Back Yu"}`,
		`{"ID":1,"Name":"szyhf"}`,
	}

	It("[]testScanSliceStruct", func() {
		var slice []testScanSliceStruct
		err := proto.ScanSlice(data, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]testScanSliceStruct{
			{-1, "Back Yu"},
			{1, "szyhf"},
		}))
	})

	It("var testContainer []*testScanSliceStruct", func() {
		var slice []*testScanSliceStruct
		err := proto.ScanSlice(data, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]*testScanSliceStruct{
			{-1, "Back Yu"},
			{1, "szyhf"},
		}))
	})
})

// ScanSlice hands its input to Scan as a []byte aliasing the string's backing
// array, so the branches where b escapes (*[]byte, *net.IP,
// encoding.BinaryUnmarshaler) have to copy. These specs pin that down: they
// fail if those branches ever go back to passing b along directly, and they
// distinguish make+copy from append(T(nil), b...), which yields nil rather
// than an empty slice.
var _ = Describe("ScanSlice copy semantics", func() {
	It("does not alias the source strings for [][]byte", func() {
		src := []string{strings.Repeat("a", 8), strings.Repeat("b", 8)}

		var slice [][]byte
		err := proto.ScanSlice(src, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([][]byte{[]byte("aaaaaaaa"), []byte("bbbbbbbb")}))

		slice[0][0] = 'Z'
		slice[1][0] = 'Z'
		Expect(src).To(Equal([]string{"aaaaaaaa", "bbbbbbbb"}))
	})

	It("does not alias the source strings for []net.IP", func() {
		src := []string{string(net.IPv4(10, 0, 0, 1).To4())}

		var slice []net.IP
		err := proto.ScanSlice(src, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]net.IP{net.IPv4(10, 0, 0, 1).To4()}))

		slice[0][3] = 9
		Expect(src).To(Equal([]string{string(net.IPv4(10, 0, 0, 1).To4())}))
	})

	It("scans an empty element into an empty, non-nil []byte", func() {
		var slice [][]byte
		err := proto.ScanSlice([]string{""}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(HaveLen(1))
		Expect(slice[0]).NotTo(BeNil())
		Expect(slice[0]).To(HaveLen(0))
	})

	It("scans an empty element into an empty, non-nil net.IP", func() {
		var slice []net.IP
		err := proto.ScanSlice([]string{""}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(HaveLen(1))
		Expect(slice[0]).NotTo(BeNil())
		Expect(slice[0]).To(HaveLen(0))
	})
})

// makeSliceNextElemFunc extends the destination in place when it already has
// spare capacity, and reuses non-nil pointer elements found in that capacity
// rather than allocating new ones. These specs cover that branch: scanning
// twice into the same backing array, which the benchmarks exercise but never
// assert on.
var _ = Describe("ScanSlice capacity reuse", func() {
	It("reuses the backing array of a []string", func() {
		var slice []string
		err := proto.ScanSlice([]string{"a", "b", "c"}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]string{"a", "b", "c"}))

		capBefore, firstElem := cap(slice), &slice[0]

		slice = slice[:0]
		err = proto.ScanSlice([]string{"x", "y", "z"}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]string{"x", "y", "z"}))
		// Same array, so the in-place path really was taken.
		Expect(cap(slice)).To(Equal(capBefore))
		Expect(&slice[0]).To(BeIdenticalTo(firstElem))
	})

	It("does not leave stale elements when the second scan is shorter", func() {
		var slice []string
		err := proto.ScanSlice([]string{"a", "b", "c"}, &slice)
		Expect(err).NotTo(HaveOccurred())

		slice = slice[:0]
		err = proto.ScanSlice([]string{"z"}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]string{"z"}))
	})

	It("scans into a preallocated slice without growing it", func() {
		slice := make([]int64, 0, 4)
		err := proto.ScanSlice([]string{"1", "2", "3"}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]int64{1, 2, 3}))
		Expect(cap(slice)).To(Equal(4))
	})

	It("reuses existing pointers for []*testScanSliceStruct", func() {
		var slice []*testScanSliceStruct
		err := proto.ScanSlice([]string{
			`{"ID":1,"Name":"one"}`,
			`{"ID":2,"Name":"two"}`,
		}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]*testScanSliceStruct{{1, "one"}, {2, "two"}}))

		capBefore := cap(slice)
		reused := []*testScanSliceStruct{slice[0], slice[1]}

		slice = slice[:0]
		err = proto.ScanSlice([]string{
			`{"ID":3,"Name":"three"}`,
			`{"ID":4,"Name":"four"}`,
		}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]*testScanSliceStruct{{3, "three"}, {4, "four"}}))
		Expect(cap(slice)).To(Equal(capBefore))
		// The pointers in the recycled capacity are scanned into, not replaced.
		Expect(slice[0]).To(BeIdenticalTo(reused[0]))
		Expect(slice[1]).To(BeIdenticalTo(reused[1]))
	})

	It("allocates pointers for nil elements in recycled capacity", func() {
		slice := make([]*testScanSliceStruct, 0, 2)
		err := proto.ScanSlice([]string{`{"ID":1,"Name":"one"}`}, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]*testScanSliceStruct{{1, "one"}}))
		Expect(slice[0]).NotTo(BeNil())
	})
})

// benchScanSliceElems is the element count per benchmark iteration. Divide
// allocs/op and ns/op by it to get the per-element cost.
const benchScanSliceElems = 1024

// benchStrings builds n deterministic strings of the given byte length.
func benchStrings(n, size int) []string {
	data := make([]string, n)
	for i := range data {
		// Vary the prefix so the strings are distinct but equally sized.
		prefix := strconv.Itoa(i)
		if len(prefix) > size {
			prefix = prefix[:size]
		}
		data[i] = prefix + strings.Repeat("x", size-len(prefix))
	}
	return data
}

// benchNumbers builds n deterministic base-10 integer strings.
func benchNumbers(n int) []string {
	data := make([]string, n)
	for i := range data {
		data[i] = strconv.Itoa(1234567890 + i)
	}
	return data
}

var benchElemSizes = []int{8, 64, 512}

// The destination slices below are allocated once and reset to zero length
// between iterations, so makeSliceNextElemFunc reuses the existing capacity.
// That keeps slice growth out of the measurement and leaves the per-element
// string -> []byte conversion inside Scan as the dominant cost.

func BenchmarkScanSliceString(b *testing.B) {
	for _, size := range benchElemSizes {
		b.Run("size="+strconv.Itoa(size), func(b *testing.B) {
			data := benchStrings(benchScanSliceElems, size)
			dst := make([]string, 0, benchScanSliceElems)

			b.ReportAllocs()
			for b.Loop() {
				dst = dst[:0]
				if err := proto.ScanSlice(data, &dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScanSliceBytes covers Scan's *[]byte branch, which hands b to the
// caller and therefore has to copy regardless of how b was produced.
func BenchmarkScanSliceBytes(b *testing.B) {
	for _, size := range benchElemSizes {
		b.Run("size="+strconv.Itoa(size), func(b *testing.B) {
			data := benchStrings(benchScanSliceElems, size)
			dst := make([][]byte, 0, benchScanSliceElems)

			b.ReportAllocs()
			for b.Loop() {
				dst = dst[:0]
				if err := proto.ScanSlice(data, &dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkScanSliceInt64(b *testing.B) {
	data := benchNumbers(benchScanSliceElems)
	dst := make([]int64, 0, benchScanSliceElems)

	b.ReportAllocs()

	for b.Loop() {
		dst = dst[:0]
		if err := proto.ScanSlice(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScanSliceFloat64(b *testing.B) {
	data := benchNumbers(benchScanSliceElems)
	dst := make([]float64, 0, benchScanSliceElems)

	b.ReportAllocs()

	for b.Loop() {
		dst = dst[:0]
		if err := proto.ScanSlice(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanSliceIP covers Scan's *net.IP branch, the other place b escapes.
func BenchmarkScanSliceIP(b *testing.B) {
	data := make([]string, benchScanSliceElems)
	for i := range data {
		data[i] = string(net.IPv4(10, 0, byte(i>>8), byte(i)).To4())
	}
	dst := make([]net.IP, 0, benchScanSliceElems)

	b.ReportAllocs()

	for b.Loop() {
		dst = dst[:0]
		if err := proto.ScanSlice(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

// benchJSON builds n deterministic JSON objects for testScanSliceStruct.
func benchJSON(n int) []string {
	data := make([]string, n)
	for i := range data {
		id := strconv.Itoa(i)
		data[i] = `{"ID":` + id + `,"Name":"name-` + id + `"}`
	}
	return data
}

// BenchmarkScanSliceBinaryUnmarshaler is the realistic mixed case: Scan copies
// b before handing it to UnmarshalBinary (so implementations may mutate or
// retain it), and json.Unmarshal dominates on top of that copy.
func BenchmarkScanSliceBinaryUnmarshaler(b *testing.B) {
	data := benchJSON(benchScanSliceElems)
	dst := make([]testScanSliceStruct, 0, benchScanSliceElems)

	b.ReportAllocs()

	for b.Loop() {
		dst = dst[:0]
		if err := proto.ScanSlice(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanSlicePtr covers pointer element types where the capacity already
// holds non-nil pointers: makeSliceNextElemFunc reuses them, so reflect.New
// never runs and the refill should allocate nothing beyond json.Unmarshal.
func BenchmarkScanSlicePtr(b *testing.B) {
	data := benchJSON(benchScanSliceElems)
	dst := make([]*testScanSliceStruct, 0, benchScanSliceElems)

	// Populate the capacity first so the measured loop takes the reuse path.
	if err := proto.ScanSlice(data, &dst); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		dst = dst[:0]
		if err := proto.ScanSlice(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanSlicePtrGrow is the same pointer element type starting from a nil
// slice, so every element pays a reflect.New on top of the slice growth.
func BenchmarkScanSlicePtrGrow(b *testing.B) {
	data := benchJSON(benchScanSliceElems)

	b.ReportAllocs()

	for b.Loop() {
		var dst []*testScanSliceStruct
		if err := proto.ScanSlice(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanSliceGrow starts from a nil slice, so it includes the slice
// growth cost a first-time caller actually pays.
func BenchmarkScanSliceGrow(b *testing.B) {
	data := benchStrings(benchScanSliceElems, 64)

	b.ReportAllocs()

	for b.Loop() {
		var dst []string
		if err := proto.ScanSlice(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}
