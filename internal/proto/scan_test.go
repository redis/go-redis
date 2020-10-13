package proto_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/internal/proto"
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

func TestScan(t *testing.T) {
	t.Parallel()

	t.Run("time", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		rdb := redis.NewClient(&redis.Options{
			Addr: ":6379",
		})

		tm := time.Now()
		rdb.Set(ctx, "now", tm, 0)

		var tm2 time.Time
		rdb.Get(ctx, "now").Scan(&tm2)

		if !tm2.Equal(tm) {
			t.Fatal(errors.New("tm2 and tm are not equal"))
		}
	})

}
