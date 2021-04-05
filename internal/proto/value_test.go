package proto

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/big"
)

var _ = Describe("resp value", func() {
	var v *Value

	BeforeEach(func() {
		v = &Value{}
	})
	It("int64", func() {
		v.Typ = redisInteger
		v.Integer = 1024
		Expect(v.Int64()).To(Equal(int64(1024)))

		v.Typ = redisString
		v.Str = "2048"
		Expect(v.Int64()).To(Equal(int64(2048)))

		v.Typ = redisBigInt
		v.BigInt = &big.Int{}
		v.BigInt.SetString("4096", 10)
		Expect(v.Int64()).To(Equal(int64(4096)))
	})

	It("float64", func() {
		v.Typ = redisFloat
		v.Float = 1024.1024
		Expect(v.Float64()).To(Equal(1024.1024))

		v.Typ = redisString
		v.Str = "2048.2048"
		Expect(v.Float64()).To(Equal(2048.2048))
	})

	It("bool", func() {
		v.Typ = redisBool
		v.Boolean = true
		Expect(v.Bool()).To(BeTrue())

		v.Typ = redisInteger
		v.Integer = 1
		Expect(v.Bool()).To(BeTrue())

		v.Typ = redisString
		v.Str = "1"
		Expect(v.Bool()).To(BeTrue())
	})

	It("status", func() {
		v.Typ = redisBool
		v.Boolean = true
		Expect(v.Status()).To(BeTrue())

		v.Typ = redisInteger
		v.Integer = 1
		Expect(v.Status()).To(BeTrue())

		v.Typ = redisString
		v.Str = "OK"
		Expect(v.Status()).To(BeTrue())
	})

	It("string", func() {
		v.Typ = redisString
		v.Str = "string"
		Expect(v.String()).To(Equal("string"))

		v.Typ = redisStatus
		v.Str = "status"
		Expect(v.String()).To(Equal("status"))

		v.Typ = redisInteger
		v.Integer = 1024
		Expect(v.String()).To(Equal("1024"))

		v.Typ = redisFloat
		v.Float = 2048.2048
		Expect(v.String()).To(Equal("2048.2048"))

		v.Typ = redisBool
		v.Boolean = true
		Expect(v.String()).To(Equal("true"))

		v.Typ = redisVerb
		v.StrFmt, v.Str = "txt", "hello"
		Expect(v.String()).To(Equal("txt:hello"))

		v.Typ = redisBigInt
		v.BigInt = &big.Int{}
		v.BigInt.SetString("4096", 10)
		Expect(v.String()).To(Equal("4096"))
	})

	It("fmt string", func() {
		v.Typ = redisVerb
		v.StrFmt, v.Str = "txt", "hello"
		format, str, _ := v.FmtString()
		Expect(format).To(Equal("txt"))
		Expect(str).To(Equal("hello"))
	})

	It("map string string", func() {
		v.Typ = redisMap
		v.Map = map[*Value]*Value{
			{
				Typ:     redisInteger,
				Integer: 1024,
			}: {
				Typ: redisStatus,
				Str: "OK",
			},
		}
		Expect(v.MapStringString()).To(Equal(map[string]string{
			"1024": "OK",
		}))

		v.Typ = redisArray
		v.Slice = []*Value{
			{Typ: redisInteger, Integer: 2048},
			{Typ: redisString, Str: "hello"},
			{Typ: redisFloat, Float: 4096.4096},
		}
		_, err := v.MapStringString()
		Expect(err).To(HaveOccurred())

		v.Slice = append(v.Slice, &Value{
			Typ:     redisBool,
			Boolean: true,
		})
		Expect(v.MapStringString()).To(Equal(map[string]string{
			"2048":      "hello",
			"4096.4096": "true",
		}))
	})

	It("map string int64", func() {
		v.Typ = redisMap
		v.Map = map[*Value]*Value{
			{
				Typ: redisStatus,
				Str: "OK",
			}: {
				Typ:     redisInteger,
				Integer: 1024,
			},
		}
		Expect(v.MapStringInt64()).To(Equal(map[string]int64{
			"OK": 1024,
		}))

		v.Typ = redisArray
		v.Slice = []*Value{
			{Typ: redisString, Str: "hello"},
			{Typ: redisInteger, Integer: 2048},
			{Typ: redisFloat, Float: 4096.4096},
		}
		_, err := v.MapStringString()
		Expect(err).To(HaveOccurred())

		v.Slice = append(v.Slice, &Value{
			Typ:     redisInteger,
			Integer: 8192,
		})
		Expect(v.MapStringInt64()).To(Equal(map[string]int64{
			"hello":     2048,
			"4096.4096": 8192,
		}))
	})

	It("slice string", func() {
		v.Typ = redisArray
		v.Slice = []*Value{
			{Typ: redisInteger, Integer: 1024},
			{Typ: redisFloat, Float: 2048.2048},
			{Typ: redisStatus, Str: "OK"},
			{Typ: redisString, Str: "hello"},
			{Typ: redisBool, Boolean: true},
		}
		Expect(v.SliceString()).To(Equal([]string{
			"1024", "2048.2048", "OK", "hello", "true",
		}))
	})

	It("slice int64", func() {
		v.Typ = redisArray
		v.Slice = []*Value{
			{Typ: redisInteger, Integer: 1024},
			{Typ: redisString, Str: "2048"},
		}
		Expect(v.SliceInt64()).To(Equal([]int64{
			1024, 2048,
		}))
	})

	It("slice float64", func() {
		v.Typ = redisArray
		v.Slice = []*Value{
			{Typ: redisFloat, Float: 1024.1024},
			{Typ: redisString, Str: "2048.2048"},
		}
		Expect(v.SliceFloat64()).To(Equal([]float64{
			1024.1024, 2048.2048,
		}))
	})

	It("slice scan", func() {
		v.Typ = redisArray
		v.Slice = []*Value{
			{Typ: redisInteger, Integer: 1024},
			{Typ: redisString, Str: "hello"},
			{Typ: redisBool, Boolean: true},
		}

		var strs []string
		err := v.SliceScan(func(item *Value) error {
			temp, err := item.String()
			Expect(err).NotTo(HaveOccurred())
			strs = append(strs, temp)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(strs).To(Equal([]string{
			"1024", "hello", "true",
		}))
	})

	It("slice len", func() {
		v.Typ = redisArray
		v.Slice = []*Value{
			{Typ: redisInteger, Integer: 1024},
			{Typ: redisString, Str: "hello"},
			{Typ: redisBool, Boolean: true},
		}

		n, err := v.SliceLen()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(3))
	})
})
