package hscan

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type data struct {
	Omit  string `redis:"-"`
	Empty string

	String string  `redis:"string"`
	Bytes  []byte  `redis:"byte"`
	Int    int     `redis:"int"`
	Uint   uint    `redis:"uint"`
	Float  float32 `redis:"float"`
	Bool   bool    `redis:"bool"`
}

type i []interface{}

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "hscan")
}

var _ = Describe("Scan", func() {
	It("catches bad args", func() {
		var (
			d data
		)

		Expect(Scan(i{}, i{}, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		Expect(Scan(i{"key"}, i{}, &d)).To(HaveOccurred())
		Expect(Scan(i{"key"}, i{"1", "2"}, &d)).To(HaveOccurred())
		Expect(Scan(i{"key", "1"}, i{}, nil)).To(HaveOccurred())

		var m map[string]interface{}
		Expect(Scan(i{"key"}, i{"1"}, &m)).To(HaveOccurred())
		Expect(Scan(i{"key"}, i{"1"}, data{})).To(HaveOccurred())
		Expect(Scan(i{"key", "string"}, i{nil, nil}, data{})).To(HaveOccurred())
	})

	It("scans good values", func() {
		var d data

		// non-tagged fields.
		Expect(Scan(i{"key"}, i{"value"}, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		keys := i{"string", "byte", "int", "uint", "float", "bool"}
		vals := i{"str!", "bytes!", "123", "456", "123.456", "1"}
		Expect(Scan(keys, vals, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String: "str!",
			Bytes:  []byte("bytes!"),
			Int:    123,
			Uint:   456,
			Float:  123.456,
			Bool:   true,
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
		Expect(Scan(keys, vals, &d2)).NotTo(HaveOccurred())
		Expect(d2).To(Equal(data2{
			String: "str!",
			Bytes:  []byte("bytes!"),
			Int:    123,
			Uint:   456,
			Float:  123.456,
			Bool:   true,
		}))

		Expect(Scan(i{"string", "float", "bool"}, i{"", "1", "t"}, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String: "",
			Bytes:  []byte("bytes!"),
			Int:    123,
			Uint:   456,
			Float:  1.0,
			Bool:   true,
		}))
	})

	It("omits untagged fields", func() {
		var d data

		Expect(Scan(i{"empty", "omit", "string"}, i{"value", "value", "str!"}, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String: "str!",
		}))
	})

	It("catches bad values", func() {
		var d data

		Expect(Scan(i{"int"}, i{"a"}, &d)).To(HaveOccurred())
		Expect(Scan(i{"uint"}, i{"a"}, &d)).To(HaveOccurred())
		Expect(Scan(i{"uint"}, i{""}, &d)).To(HaveOccurred())
		Expect(Scan(i{"float"}, i{"b"}, &d)).To(HaveOccurred())
		Expect(Scan(i{"bool"}, i{"-1"}, &d)).To(HaveOccurred())
		Expect(Scan(i{"bool"}, i{""}, &d)).To(HaveOccurred())
		Expect(Scan(i{"bool"}, i{"123"}, &d)).To(HaveOccurred())
	})
})
