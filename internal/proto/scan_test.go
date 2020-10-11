package proto

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
		err := ScanSlice(data, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]testScanSliceStruct{
			{-1, "Back Yu"},
			{1, "szyhf"},
		}))
	})

	It("var testContainer []*testScanSliceStruct", func() {
		var slice []*testScanSliceStruct
		err := ScanSlice(data, &slice)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]*testScanSliceStruct{
			{-1, "Back Yu"},
			{1, "szyhf"},
		}))
	})
})

func TestScan(t *testing.T) {
	t.Parallel()

	t.Run("time", func(t *testing.T) {
		t.Parallel()

		toWrite := time.Now()
		var toScan time.Time

		buf := new(bytes.Buffer)
		wr := NewWriter(buf)
		err := wr.WriteArg(toWrite)
		if err != nil {
			t.Fatal(err)
		}

		r := NewReader(buf)
		b, err := r.readTmpBytesReply()
		if err != nil {
			t.Fatal(err)
		}

		err = Scan(b, &toScan)
		if err != nil {
			t.Fatal(err)
		}

		if !toWrite.Equal(toScan) {
			t.Fatal(errors.New("toWrite and toScan is not equal"))
		}
	})

}
