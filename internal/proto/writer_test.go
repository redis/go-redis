package proto_test

import (
	"bytes"
	"encoding"
	"fmt"
	"net"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/util"
)

type MyType struct{}

var _ encoding.BinaryMarshaler = (*MyType)(nil)

func (t *MyType) MarshalBinary() ([]byte, error) {
	return []byte("hello"), nil
}

var _ = Describe("WriteBuffer", func() {
	var buf *bytes.Buffer
	var wr *proto.Writer

	BeforeEach(func() {
		buf = new(bytes.Buffer)
		wr = proto.NewWriter(buf)
	})

	It("should write args", func() {
		err := wr.WriteArgs([]interface{}{
			"string",
			12,
			34.56,
			[]byte{'b', 'y', 't', 'e', 's'},
			true,
			nil,
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(buf.Bytes()).To(Equal([]byte("*6\r\n" +
			"$6\r\nstring\r\n" +
			"$2\r\n12\r\n" +
			"$5\r\n34.56\r\n" +
			"$5\r\nbytes\r\n" +
			"$1\r\n1\r\n" +
			"$0\r\n" +
			"\r\n")))
	})

	It("should append time", func() {
		tm := time.Date(2019, 1, 1, 9, 45, 10, 222125, time.UTC)
		err := wr.WriteArgs([]interface{}{tm})
		Expect(err).NotTo(HaveOccurred())

		Expect(buf.Len()).To(Equal(41))
	})

	It("should append marshalable args", func() {
		err := wr.WriteArgs([]interface{}{&MyType{}})
		Expect(err).NotTo(HaveOccurred())

		Expect(buf.Len()).To(Equal(15))
	})

	It("should append net.IP", func() {
		ip := net.ParseIP("192.168.1.1")
		err := wr.WriteArgs([]interface{}{ip})
		Expect(err).NotTo(HaveOccurred())
		Expect(buf.String()).To(Equal(fmt.Sprintf("*1\r\n$16\r\n%s\r\n", bytes.NewBuffer(ip))))
	})
})

type discard struct{}

func (discard) Write(b []byte) (int, error) {
	return len(b), nil
}

func (discard) WriteString(s string) (int, error) {
	return len(s), nil
}

func (discard) WriteByte(c byte) error {
	return nil
}

func BenchmarkWriteBuffer_Append(b *testing.B) {
	buf := proto.NewWriter(discard{})
	args := []interface{}{"hello", "world", "foo", "bar"}

	for i := 0; i < b.N; i++ {
		err := buf.WriteArgs(args)
		if err != nil {
			b.Fatal(err)
		}
	}
}

var _ = Describe("WriteArg", func() {
	var buf *bytes.Buffer
	var wr *proto.Writer

	BeforeEach(func() {
		buf = new(bytes.Buffer)
		wr = proto.NewWriter(buf)
	})

	args := map[any]string{
		"hello":                   "$5\r\nhello\r\n",
		int(10):                   "$2\r\n10\r\n",
		util.ToPtr(int(10)):       "$2\r\n10\r\n",
		int8(10):                  "$2\r\n10\r\n",
		util.ToPtr(int8(10)):      "$2\r\n10\r\n",
		int16(10):                 "$2\r\n10\r\n",
		util.ToPtr(int16(10)):     "$2\r\n10\r\n",
		int32(10):                 "$2\r\n10\r\n",
		util.ToPtr(int32(10)):     "$2\r\n10\r\n",
		int64(10):                 "$2\r\n10\r\n",
		util.ToPtr(int64(10)):     "$2\r\n10\r\n",
		uint(10):                  "$2\r\n10\r\n",
		util.ToPtr(uint(10)):      "$2\r\n10\r\n",
		uint8(10):                 "$2\r\n10\r\n",
		util.ToPtr(uint8(10)):     "$2\r\n10\r\n",
		uint16(10):                "$2\r\n10\r\n",
		util.ToPtr(uint16(10)):    "$2\r\n10\r\n",
		uint32(10):                "$2\r\n10\r\n",
		util.ToPtr(uint32(10)):    "$2\r\n10\r\n",
		uint64(10):                "$2\r\n10\r\n",
		util.ToPtr(uint64(10)):    "$2\r\n10\r\n",
		float32(10.3):             "$18\r\n10.300000190734863\r\n",
		util.ToPtr(float32(10.3)): "$18\r\n10.300000190734863\r\n",
		float64(10.3):             "$4\r\n10.3\r\n",
		util.ToPtr(float64(10.3)): "$4\r\n10.3\r\n",
		bool(true):                "$1\r\n1\r\n",
		bool(false):               "$1\r\n0\r\n",
		util.ToPtr(bool(true)):    "$1\r\n1\r\n",
		util.ToPtr(bool(false)):   "$1\r\n0\r\n",
	}

	for arg, expect := range args {
		arg, expect := arg, expect
		It(fmt.Sprintf("should write arg of type %T", arg), func() {
			err := wr.WriteArg(arg)
			Expect(err).NotTo(HaveOccurred())
			Expect(buf.String()).To(Equal(expect))
		})
	}
})
