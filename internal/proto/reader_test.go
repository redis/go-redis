package proto

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func BenchmarkReader_ParseReplyResp3_Status(b *testing.B) {
	benchmarkParseReply(b, "+OK\r\n", false)
}

func BenchmarkReader_ParseReply_Int(b *testing.B) {
	benchmarkParseReply(b, ":1\r\n", false)
}

func BenchmarkReader_ParseReply_Error(b *testing.B) {
	benchmarkParseReply(b, "-Error message\r\n", true)
}

func BenchmarkReader_ParseReply_String(b *testing.B) {
	benchmarkParseReply(b, "$5\r\nhello\r\n", false)
}

func BenchmarkReader_ParseReply_Nil(b *testing.B) {
	benchmarkParseReply(b, "_\r\n", true)
}

func BenchmarkReader_ParseReply_Float(b *testing.B) {
	benchmarkParseReply(b, ",100.1234\r\n", false)
}

func BenchmarkReader_ParseReply_Bool(b *testing.B) {
	benchmarkParseReply(b, "#t\r\n", false)
}

func BenchmarkReader_ParseReply_BlobError(b *testing.B) {
	benchmarkParseReply(b, "!10\r\nblob error\r\n", true)
}

func BenchmarkReader_ParseReply_Verb(b *testing.B) {
	benchmarkParseReply(b, "=9\r\ntxt:hello\r\n", false)
}

func BenchmarkReader_ParseReply_BigInt(b *testing.B) {
	benchmarkParseReply(b, "(3492890328409238509324850943850943825024385\r\n", false)
}

func BenchmarkReader_ParseReply_Slice(b *testing.B) {
	benchmarkParseReply(b, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Slice2(b *testing.B) {
	r := "*3\r\n" +

		"%2\r\n" +
		"+key\r\n" +
		"$2\r\nhi\r\n" +
		"$5\r\nindex\r\n" +
		":1024\r\n" +

		"$5\r\nhello\r\n" +

		"*2\r\n" +
		"_\r\n" +
		"#t\r\n"

	benchmarkParseReply(b, r, false)
}

func BenchmarkReader_ParseReply_Map(b *testing.B) {
	r := "%2\r\n" +

		"*2\r\n" +
		"$2\r\nhi\r\n" +
		"*1\r\n" +
		"_\r\n" +

		"#f\r\n" +

		"$1\r\nk\r\n" +

		"*1\r\n" +
		"$4\r\ntext\r\n"

	benchmarkParseReply(b, r, false)
}

func BenchmarkReader_ParseReply_Set(b *testing.B) {
	benchmarkParseReply(b, "~2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Attr(b *testing.B) {
	r := "|1\r\n" +
		"+key\r\n" +
		"%2\r\n" +
		"$1\r\na\r\n" +
		",0.1923\r\n" +
		"$1\r\nb\r\n" +
		",0.0012\r\n" +

		"*2\r\n" +
		":1024\r\n" +
		":2048\r\n"
	benchmarkParseReply(b, r, false)
}

func BenchmarkReader_ParseReply_Push(b *testing.B) {
	benchmarkParseReply(b, ">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func benchmarkParseReply(b *testing.B, reply string, redisErr bool) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		buf.WriteString(reply)
	}
	p := NewReader(buf)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v, err := p.ReadReply()
		if err != nil {
			b.Fatal(err)
		}
		if redisErr {
			if v.RedisError == nil {
				b.Fatal("expect redis error, but it is nil")
			}
		} else {
			if v.RedisError != nil {
				b.Fatal("expect nil, but it is error")
			}
		}
	}
}

var notNil = errors.New("not nil")

type respList struct {
	resp   string
	expect *Value
	err    error
}

var list = []respList{
	//SimpleString
	{
		resp:   "+\r\n",
		expect: &Value{Typ: redisStatus},
	},
	{
		resp:   "+hello world\r\n",
		expect: &Value{Typ: redisStatus, Str: "hello world"},
	},

	// SimpleError
	{
		resp:   "-\r\n",
		expect: &Value{Typ: redisError, RedisError: RedisError("")},
	},
	{
		resp:   "-hello world\r\n",
		expect: &Value{Typ: redisError, RedisError: RedisError("hello world")},
	},

	// BlobString
	{
		resp:   "$0\r\n\r\n",
		expect: &Value{Typ: redisString},
	},
	{
		resp: "$0\r\n",
		err:  io.EOF,
	},
	{
		resp: "$-10\r\n",
		err:  notNil,
	},
	{
		resp: "$\r\n",
		err:  notNil,
	},
	{
		resp:   "$6\r\nhello\n\r\n",
		expect: &Value{Typ: redisString, Str: "hello\n"},
	},

	// Number
	{
		resp: ":\r\n",
		err:  notNil,
	},
	{
		resp:   ":0\r\n",
		expect: &Value{Typ: redisInteger, Integer: int64(0)},
	},
	{
		resp:   ":-10\r\n",
		expect: &Value{Typ: redisInteger, Integer: int64(-10)},
	},
	{
		resp:   ":1024\r\n",
		expect: &Value{Typ: redisInteger, Integer: int64(1024)},
	},

	// Null
	{
		resp:   "_\r\n",
		expect: &Value{Typ: redisNil, RedisError: Nil},
	},
	{
		resp:   "_hello\r\n",
		expect: &Value{Typ: redisNil, RedisError: Nil},
	},

	// Float
	{
		resp: ",\r\n",
		err:  notNil,
	},
	{
		resp:   ",0\r\n",
		expect: &Value{Typ: redisFloat, Float: 0},
	},
	{
		resp:   ",100.1234\r\n",
		expect: &Value{Typ: redisFloat, Float: 100.1234},
	},
	{
		resp:   ",-100.4321\r\n",
		expect: &Value{Typ: redisFloat, Float: -100.4321},
	},
	{
		resp:   ",inf\r\n",
		expect: &Value{Typ: redisFloat, Float: math.Inf(1)},
	},
	{
		resp:   ",-inf\r\n",
		expect: &Value{Typ: redisFloat, Float: math.Inf(-1)},
	},

	// Bool
	{
		resp: "#\r\n",
		err:  notNil,
	},
	{
		resp:   "#t\r\n",
		expect: &Value{Typ: redisBool, Boolean: true},
	},
	{
		resp:   "#f\r\n",
		expect: &Value{Typ: redisBool, Boolean: false},
	},

	// BlobError
	{
		resp:   "!0\r\n\r\n",
		expect: &Value{Typ: redisBlobError, RedisError: RedisError("")},
	},
	{
		resp: "!0\r\n",
		err:  io.EOF,
	},
	{
		resp: "!-10\r\n",
		err:  errors.New("redis: invalid reply: \"!-10\""),
	},
	{
		resp: "!\r\n",
		err:  notNil,
	},
	{
		resp:   "!6\r\nhello\n\r\n",
		expect: &Value{Typ: redisBlobError, RedisError: RedisError("hello\n")},
	},

	// VerbatimString
	{
		resp: "=0\r\n",
		err:  io.EOF,
	},
	{
		resp: "=-10\r\n",
		err:  errors.New("redis: invalid reply: \"=-10\""),
	},
	{
		resp: "=\r\n",
		err:  notNil,
	},
	{
		resp: "=2\r\nhi\r\n",
		err:  notNil,
	},
	{
		resp: "=5\r\nhello\r\n",
		err:  notNil,
	},
	{
		resp:   "=9\r\ntxt:hello\r\n",
		expect: &Value{Typ: redisVerb, Str: "hello", StrFmt: "txt"},
	},

	// BigNumber
	{
		resp: "(\r\n",
		err:  errors.New("redis: can't parse bigInt reply: \"(\""),
	},
	{
		resp: "(hello1024\r\n",
		err:  notNil,
	},
	{
		resp:   "(3492890328409238509324850943850943825024385\r\n",
		expect: &Value{Typ: redisBigInt, BigInt: bigInt("3492890328409238509324850943850943825024385")},
	},
	{
		resp:   "(-3492890328409238509324850943850943825024385\r\n",
		expect: &Value{Typ: redisBigInt, BigInt: bigInt("-3492890328409238509324850943850943825024385")},
	},

	// Array
	{
		resp:   "*0\r\n",
		expect: &Value{Typ: redisArray, Slice: make([]*Value, 0)},
	},
	{
		resp: "*\r\n",
		err:  notNil,
	},
	{
		resp: "*2\r\n+hello world\r\n:1024\r\n",
		expect: &Value{Typ: redisArray, Slice: []*Value{
			{Typ: redisStatus, Str: "hello world"},
			{Typ: redisInteger, Integer: 1024},
		}},
	},
	{
		resp: "*2\r\n*2\r\n$2\r\nok\r\n,100.1234\r\n$5\r\nhello\r\n",
		expect: &Value{Typ: redisArray, Slice: []*Value{
			{
				Typ: redisArray,
				Slice: []*Value{
					{Typ: redisString, Str: "ok"},
					{Typ: redisFloat, Float: 100.1234},
				},
			},
			{
				Typ: redisString,
				Str: "hello",
			},
		}},
	},

	// Set
	{
		resp:   "~0\r\n",
		expect: &Value{Typ: redisSet, Slice: make([]*Value, 0)},
	},
	{
		resp: "~\r\n",
		err:  notNil,
	},
	{
		resp: "~2\r\n+hello world\r\n:1024\r\n",
		expect: &Value{Typ: redisSet, Slice: []*Value{
			{Typ: redisStatus, Str: "hello world"},
			{Typ: redisInteger, Integer: 1024},
		}},
	},
	{
		resp: "~2\r\n*2\r\n$2\r\nok\r\n,100.1234\r\n$5\r\nhello\r\n",
		expect: &Value{Typ: redisSet, Slice: []*Value{
			{
				Typ: redisArray,
				Slice: []*Value{
					{Typ: redisString, Str: "ok"},
					{Typ: redisFloat, Float: 100.1234},
				},
			},
			{
				Typ: redisString,
				Str: "hello",
			},
		}},
	},

	// Push
	{
		resp:   ">0\r\n",
		expect: &Value{Typ: redisPush, Slice: make([]*Value, 0)},
	},
	{
		resp: ">\r\n",
		err:  notNil,
	},
	{
		resp: ">2\r\n+hello world\r\n:1024\r\n",
		expect: &Value{Typ: redisPush, Slice: []*Value{
			{Typ: redisStatus, Str: "hello world"},
			{Typ: redisInteger, Integer: 1024},
		}},
	},
	{
		resp: ">2\r\n*2\r\n$2\r\nok\r\n,100.1234\r\n$5\r\nhello\r\n",
		expect: &Value{Typ: redisPush, Slice: []*Value{
			{
				Typ: redisArray,
				Slice: []*Value{
					{Typ: redisString, Str: "ok"},
					{Typ: redisFloat, Float: 100.1234},
				},
			},
			{
				Typ: redisString,
				Str: "hello",
			},
		}},
	},
}

func bigInt(s string) *big.Int {
	i := new(big.Int)
	i, _ = i.SetString(s, 10)
	return i
}

func TestRespProto(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "resp")

}

var _ = Describe("resp proto", func() {
	It("should format", func() {
		pp := []byte{
			redisStatus,
			redisError,
			redisString,
			redisInteger,
			redisNil,
			redisFloat,
			redisBool,
			redisBlobError,
			redisVerb,
			redisBigInt,
			redisArray,
			redisMap,
			redisSet,
			redisAttr,
			redisPush,
		}
		for _, p := range pp {
			wr := new(bytes.Buffer)
			r := NewReader(wr)

			wr.WriteString(fmt.Sprintf("%s1024\n\r\n", string(p)))
			_, err := r.ReadReply()
			Expect(err).To(Equal(
				fmt.Errorf("redis: invalid reply: %q", fmt.Sprintf("%s1024\n", string(p))),
			))
		}
	})

	It("read map", func() {
		p := "%2\r\n*2\r\n$2\r\nhi\r\n*1\r\n_\r\n#f\r\n$1\r\nk\r\n*1\r\n$4\r\ntext\r\n"
		wr := new(bytes.Buffer)
		r := NewReader(wr)

		wr.WriteString(p)
		v, err := r.ReadReply()
		Expect(err).NotTo(HaveOccurred())

		expectVal := &Value{
			Typ: redisMap,
			Map: map[*Value]*Value{
				{Typ: redisArray, Slice: []*Value{
					{Typ: redisString, Str: "hi"},
					{Typ: redisArray, Slice: []*Value{
						{Typ: redisNil, RedisError: Nil},
					}},
				}}: {Typ: redisBool, Boolean: false},

				{Typ: redisString, Str: "k"}: {
					Typ: redisArray,
					Slice: []*Value{
						{Typ: redisString, Str: "text"},
					},
				},
			},
		}
		Expect(v.Interface()).To(Equal(expectVal.Interface()))
	})

	It("read", func() {
		for _, l := range list {
			wr := new(bytes.Buffer)
			r := NewReader(wr)

			wr.WriteString(l.resp)

			v, err := r.ReadReply()
			if l.err != nil {
				if l.err == notNil {
					Expect(err).To(HaveOccurred())
				} else {
					Expect(err).To(Equal(l.err))
				}
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(v).To(Equal(l.expect))
			}
		}
	})
})
