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

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "hscan")
}

var _ = Describe("Scan", func() {
	It("catches bad args", func() {
		var d data

		Expect(Scan([]interface{}{}, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		Expect(Scan([]interface{}{"key"}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"key", "1", "2"}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"key", "1"}, nil)).To(HaveOccurred())

		var i map[string]interface{}
		Expect(Scan([]interface{}{"key", "1"}, &i)).To(HaveOccurred())
		Expect(Scan([]interface{}{"key", "1"}, data{})).To(HaveOccurred())
		Expect(Scan([]interface{}{"key", nil, "string", nil}, data{})).To(HaveOccurred())
	})

	It("scans good values", func() {
		var d data

		// non-tagged fields.
		Expect(Scan([]interface{}{"key", "value"}, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{}))

		res := []interface{}{"string", "str!",
			"byte", "bytes!",
			"int", "123",
			"uint", "456",
			"float", "123.456",
			"bool", "1"}
		Expect(Scan(res, &d)).NotTo(HaveOccurred())
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
		Expect(Scan(res, &d2)).NotTo(HaveOccurred())
		Expect(d2).To(Equal(data2{
			String: "str!",
			Bytes:  []byte("bytes!"),
			Int:    123,
			Uint:   456,
			Float:  123.456,
			Bool:   true,
		}))

		Expect(Scan([]interface{}{
			"string", "",
			"float", "1",
			"bool", "t"}, &d)).NotTo(HaveOccurred())
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

		Expect(Scan([]interface{}{
			"empty", "value",
			"omit", "value",
			"string", "str!"}, &d)).NotTo(HaveOccurred())
		Expect(d).To(Equal(data{
			String: "str!",
		}))
	})

	It("catches bad values", func() {
		var d data

		Expect(Scan([]interface{}{"int", "a"}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"uint", "a"}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"uint", ""}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"float", "b"}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"bool", "-1"}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"bool", ""}, &d)).To(HaveOccurred())
		Expect(Scan([]interface{}{"bool", "123"}, &d)).To(HaveOccurred())
	})
})
