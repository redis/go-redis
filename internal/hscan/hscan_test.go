package hscan

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type data struct {
	Omit  string `redis:"-"`
	Empty string

	String  string  `redis:"string"`
	Bytes   []byte  `redis:"byte"`
	Int     int     `redis:"int"`
	Int64   int64   `redis:"int64"`
	Uint    uint    `redis:"uint"`
	Uint64  uint64  `redis:"uint64"`
	Float   float32 `redis:"float"`
	Float64 float64 `redis:"float64"`
	Bool    bool    `redis:"bool"`
}

type i []interface{}

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "hscan")
}

var _ = Describe("Scan", func() {
	It("catches bad args", func() {
		var d data

		Expect(Scan(&d, i{}, i{})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		Expect(Scan(&d, i{"key"}, i{})).To(HaveOccurred())
		Expect(Scan(&d, i{"key"}, i{"1", "2"})).To(HaveOccurred())
		Expect(Scan(nil, i{"key", "1"}, i{})).To(HaveOccurred())

		var m map[string]interface{}
		Expect(Scan(&m, i{"key"}, i{"1"})).To(HaveOccurred())
		Expect(Scan(data{}, i{"key"}, i{"1"})).To(HaveOccurred())
		Expect(Scan(data{}, i{"key", "string"}, i{nil, nil})).To(HaveOccurred())
	})

	It("scans good values", func() {
		var d data

		// non-tagged fields.
		Expect(Scan(&d, i{"key"}, i{"value"})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		keys := i{"string", "byte", "int", "int64", "uint", "uint64", "float", "float64", "bool"}
		vals := i{
			"str!", "bytes!", "123", "123456789123456789", "456", "987654321987654321",
			"123.456", "123456789123456789.987654321987654321", "1",
		}
		Expect(Scan(&d, keys, vals)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String:  "str!",
			Bytes:   []byte("bytes!"),
			Int:     123,
			Int64:   123456789123456789,
			Uint:    456,
			Uint64:  987654321987654321,
			Float:   123.456,
			Float64: 1.2345678912345678e+17,
			Bool:    true,
		}))

		// Scan a different type with the same values to test that
		// the struct spec maps don't conflict.
		type data2 struct {
			String string  `redis:"string"`
			Bytes  []byte  `redis:"byte"`
			Int    int     `redis:"int"`
			Uint   uint    `redis:"uint"`
			Float  float32 `redis:"float"`
			Bool   bool    `redis:"bool"`
		}
		var d2 data2
		Expect(Scan(&d2, keys, vals)).NotTo(HaveOccurred())
		Expect(d2).To(Equal(data2{
			String: "str!",
			Bytes:  []byte("bytes!"),
			Int:    123,
			Uint:   456,
			Float:  123.456,
			Bool:   true,
		}))

		Expect(Scan(&d, i{"string", "float", "bool"}, i{"", "1", "t"})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String:  "",
			Bytes:   []byte("bytes!"),
			Int:     123,
			Int64:   123456789123456789,
			Uint:    456,
			Uint64:  987654321987654321,
			Float:   1.0,
			Float64: 1.2345678912345678e+17,
			Bool:    true,
		}))
	})

	It("omits untagged fields", func() {
		var d data

		Expect(Scan(&d, i{"empty", "omit", "string"}, i{"value", "value", "str!"})).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String: "str!",
		}))
	})

	It("catches bad values", func() {
		var d data

		Expect(Scan(&d, i{"int"}, i{"a"})).To(HaveOccurred())
		Expect(Scan(&d, i{"uint"}, i{"a"})).To(HaveOccurred())
		Expect(Scan(&d, i{"uint"}, i{""})).To(HaveOccurred())
		Expect(Scan(&d, i{"float"}, i{"b"})).To(HaveOccurred())
		Expect(Scan(&d, i{"bool"}, i{"-1"})).To(HaveOccurred())
		Expect(Scan(&d, i{"bool"}, i{""})).To(HaveOccurred())
		Expect(Scan(&d, i{"bool"}, i{"123"})).To(HaveOccurred())
	})
})
