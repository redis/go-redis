package proto

import (
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type testScanSliceStruct struct {
	ID   int
	Name string
}

func (this *testScanSliceStruct) MarshalBinary() (data []byte, err error) {
	return json.Marshal(data)
}

func (this *testScanSliceStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, this)
}

var _ = Describe("ScanSlice", func() {

	// Base string array for test.
	strAry := []string{`{"ID":-1,"Name":"Back Yu"}`, `{"ID":1,"Name":"szyhf"}`}
	// Validate json bytes of container if ScanSlice success
	equalJson := Equal([]byte(`[{"ID":-1,"Name":"Back Yu"},{"ID":1,"Name":"szyhf"}]`))

	It("var testContainer []testScanSliceStruct", func() {
		var testContainer []testScanSliceStruct
		err := ScanSlice(strAry, &testContainer)
		Expect(err).NotTo(HaveOccurred())

		jsonBytes, err := json.Marshal(testContainer)
		Expect(err).NotTo(HaveOccurred())
		Expect(jsonBytes).Should(equalJson)
	})

	It("testContainer := new([]testScanSliceStruct)", func() {
		testContainer := new([]testScanSliceStruct)
		err := ScanSlice(strAry, testContainer)
		Expect(err).NotTo(HaveOccurred())

		jsonBytes, err := json.Marshal(testContainer)
		Expect(err).NotTo(HaveOccurred())
		Expect(jsonBytes).Should(equalJson)
	})

	It("var testContainer []*testScanSliceStruct", func() {
		var testContainer []*testScanSliceStruct
		err := ScanSlice(strAry, &testContainer)
		Expect(err).NotTo(HaveOccurred())

		jsonBytes, err := json.Marshal(testContainer)
		Expect(err).NotTo(HaveOccurred())
		Expect(jsonBytes).Should(equalJson)
	})

	It("testContainer := new([]*testScanSliceStruct)", func() {
		testContainer := new([]*testScanSliceStruct)
		err := ScanSlice(strAry, testContainer)
		Expect(err).NotTo(HaveOccurred())

		jsonBytes, err := json.Marshal(testContainer)
		Expect(err).NotTo(HaveOccurred())
		Expect(jsonBytes).Should(equalJson)
	})

})
