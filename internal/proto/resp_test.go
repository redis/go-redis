package proto

//
//import (
//	"bytes"
//	"errors"
//	"fmt"
//	"io"
//	"math"
//	"math/big"
//	"testing"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//)
//
//type respList struct {
//	resp   string
//	expect *Value
//}
//
//var list = []respList{
//	//SimpleString
//	{
//		resp:   "+\r\n",
//		expect: &Value{},
//	},
//	{
//		resp:   "+hello world\r\n",
//		expect: &Value{Str: "hello world"},
//	},
//
//	// SimpleError
//	{
//		resp:   "-\r\n",
//		expect: &Value{err: errors.New("")},
//	},
//	{
//		resp:   "-hello world\r\n",
//		expect: &Value{err: errors.New("hello world")},
//	},
//
//	// BlobString
//	{
//		resp:   "$0\r\n\r\n",
//		expect: &Value{},
//	},
//	{
//		resp:   "$0\r\n",
//		expect: &Value{err: io.EOF},
//	},
//	{
//		resp:   "$-10\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "$\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "$6\r\nhello\n\r\n",
//		expect: &Value{Str: "hello\n"},
//	},
//
//	// Number
//	{
//		resp:   ":\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   ":0\r\n",
//		expect: &Value{Number: int64(0)},
//	},
//	{
//		resp:   ":-10\r\n",
//		expect: &Value{Number: int64(-10)},
//	},
//	{
//		resp:   ":1024\r\n",
//		expect: &Value{Number: int64(1024)},
//	},
//
//	// Null
//	{
//		resp:   "_\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "_hello\r\n",
//		expect: &Value{err: Nil},
//	},
//
//	// Float
//	{
//		resp:   ",\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   ",0\r\n",
//		expect: &Value{Float: 0},
//	},
//	{
//		resp:   ",100.1234\r\n",
//		expect: &Value{Float: 100.1234},
//	},
//	{
//		resp:   ",-100.4321\r\n",
//		expect: &Value{Float: -100.4321},
//	},
//	{
//		resp:   ",inf\r\n",
//		expect: &Value{Float: math.Inf(1)},
//	},
//	{
//		resp:   ",-inf\r\n",
//		expect: &Value{Float: math.Inf(-1)},
//	},
//
//	// Bool
//	{
//		resp:   "#\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "#t\r\n",
//		expect: &Value{Boolean: true},
//	},
//	{
//		resp:   "#f\r\n",
//		expect: &Value{Boolean: false},
//	},
//
//	// BlobError
//	{
//		resp:   "!0\r\n\r\n",
//		expect: &Value{err: errors.New("")},
//	},
//	{
//		resp:   "!0\r\n",
//		expect: &Value{err: io.EOF},
//	},
//	{
//		resp:   "!-10\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "!\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "!6\r\nhello\n\r\n",
//		expect: &Value{err: errors.New("hello\n")},
//	},
//
//	// VerbatimString
//	{
//		resp:   "=0\r\n",
//		expect: &Value{err: io.EOF},
//	},
//	{
//		resp:   "=-10\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "=\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "=2\r\nhi\r\n",
//		expect: &Value{err: fmt.Errorf("redis: can't parse verbatim string reply: %q", "=2")},
//	},
//	{
//		resp:   "=5\r\nhello\r\n",
//		expect: &Value{err: fmt.Errorf("redis: can't parse verbatim string reply: %q", "=5")},
//	},
//	{
//		resp:   "=9\r\ntxt:hello\r\n",
//		expect: &Value{Str: "hello", StrFmt: "txt"},
//	},
//
//	// BigNumber
//	{
//		resp:   "(\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp:   "(hello1024\r\n",
//		expect: &Value{err: fmt.Errorf("redis: can't parse bigInt reply: %q", "(hello1024")},
//	},
//	{
//		resp:   "(3492890328409238509324850943850943825024385\r\n",
//		expect: &Value{BigInt: bigInt("3492890328409238509324850943850943825024385")},
//	},
//	{
//		resp:   "(-3492890328409238509324850943850943825024385\r\n",
//		expect: &Value{BigInt: bigInt("-3492890328409238509324850943850943825024385")},
//	},
//
//	// Array
//	{
//		resp:   "*0\r\n",
//		expect: &Value{Slice: make([]*Value, 0)},
//	},
//	{
//		resp:   "*\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp: "*2\r\n+hello world\r\n:1024\r\n",
//		expect: &Value{Slice: []*Value{
//			{Str: "hello world"},
//			{Number: 1024},
//		}},
//	},
//	{
//		resp: "*2\r\n*2\r\n$2\r\nok\r\n,100.1234\r\n$5\r\nhello\r\n",
//		expect: &Value{Slice: []*Value{
//			{
//				Slice: []*Value{
//					{Str: "ok"},
//					{Float: 100.1234},
//				},
//			},
//			{
//				Str: "hello",
//			},
//		}},
//	},
//
//	// Map
//	{
//		resp:   "%0\r\n",
//		expect: &Value{Map: make(map[*Value]*Value)},
//	},
//	{
//		resp:   "%\r\n",
//		expect: &Value{err: Nil},
//	},
//
//	// Set
//	{
//		resp:   "~0\r\n",
//		expect: &Value{Slice: make([]*Value, 0)},
//	},
//	{
//		resp:   "~\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp: "~2\r\n+hello world\r\n:1024\r\n",
//		expect: &Value{Slice: []*Value{
//			{Str: "hello world"},
//			{Number: 1024},
//		}},
//	},
//	{
//		resp: "~2\r\n*2\r\n$2\r\nok\r\n,100.1234\r\n$5\r\nhello\r\n",
//		expect: &Value{Slice: []*Value{
//			{
//				Slice: []*Value{
//					{Str: "ok"},
//					{Float: 100.1234},
//				},
//			},
//			{
//				Str: "hello",
//			},
//		}},
//	},
//
//	// Push
//	{
//		resp:   ">0\r\n",
//		expect: &Value{Slice: make([]*Value, 0)},
//	},
//	{
//		resp:   ">\r\n",
//		expect: &Value{err: Nil},
//	},
//	{
//		resp: ">2\r\n+hello world\r\n:1024\r\n",
//		expect: &Value{Slice: []*Value{
//			{Str: "hello world"},
//			{Number: 1024},
//		}},
//	},
//	{
//		resp: ">2\r\n*2\r\n$2\r\nok\r\n,100.1234\r\n$5\r\nhello\r\n",
//		expect: &Value{Slice: []*Value{
//			{
//				Slice: []*Value{
//					{Str: "ok"},
//					{Float: 100.1234},
//				},
//			},
//			{
//				Str: "hello",
//			},
//		}},
//	},
//
//	// Attribute
//	{
//		resp: "|0\r\n*2\r\n+hello\r\n:1024\r\n",
//		expect: &Value{
//			Attr: make(map[*Value]*Value),
//			Slice: []*Value{
//				{Str: "hello"},
//				{Number: 1024},
//			},
//		},
//	},
//}
//
//func bigInt(s string) *big.Int {
//	i := new(big.Int)
//	i, _ = i.SetString(s, 10)
//	return i
//}
//
//func TestRespProto(t *testing.T) {
//	RegisterFailHandler(Fail)
//	RunSpecs(t, "resp")
//
//}
//
//func TestRespMap(t *testing.T) {
//	wr := new(bytes.Buffer)
//	r := NewRespReader(wr)
//
//	wr.WriteString("%2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n:1024\r\n+hello\r\n")
//	v := r.ReadReply()
//
//	expectKey := map[interface{}]interface{}{"key": true, 1024: true}
//	expectVal := map[interface{}]interface{}{"hello": true, "value": true}
//	for key, val := range v.Map {
//		if key.Str != "" {
//			delete(expectKey, key.Str)
//		}
//		if key.Number != 0 {
//			delete(expectKey, key.Number)
//		}
//		delete(expectVal, val.Str)
//	}
//
//	if len(expectKey) > 0 || len(expectVal) > 0 {
//		t.Errorf("expected %v-%v but got %v", expectKey, expectVal, v.Map)
//	}
//}
//
//func TestRespAttr(t *testing.T) {
//	//{
//	//resp: "|1\r\n$5\r\nhello\r\n%2\r\n+key\r\n,100.1234\r\n$3\r\nmod\r\n:1024\r\n*2\r\n:100\r\n:200\r\n",
//	//	expect: &Value{
//	//	Attr: map[*Value]*Value{
//	//		&Value{Str: "hello"}: {
//	//			Map: map[*Value]*Value{
//	//				&Value{Str: "key"}: {Float: 100.1234},
//	//				&Value{Str: "mod"}: {Number: 1024},
//	//			},
//	//		},
//	//	},
//	//	Slice: []*Value{
//	//		{Number: 100},
//	//		{Number: 200},
//	//	},
//	//},
//	//},
//	//wr := new(bytes.Buffer)
//	//r := NewRespReader(wr)
//	//
//	//wr.WriteString("|1\r\n$5\r\nhello\r\n%2\r\n+key\r\n,100.1234\r\n$3\r\nmod\r\n:1024\r\n*2\r\n:100\r\n:200\r\n")
//	//v := r.ReadReply()
//	//for key, val := range v.Attr {
//	//	if key.Str != "hello" {
//	//		t.Errorf("expected hello but got %s", key.Str)
//	//	}
//	//	//for sk, sv := range val.Map {
//	//	//	if sk.Str != "key" && sk.Str != "mod" {
//	//	//
//	//	//	}
//	//	//}
//	//}
//}
//
//func getMapItem(v *Value, typ string) map[int]*Value {
//	m := make(map[int]*Value)
//	var i int
//
//	switch typ {
//	case "mapKey":
//		for item, _ := range v.Map {
//			m[i] = item
//			i++
//		}
//	case "mapVal":
//		for _, item := range v.Map {
//			m[i] = item
//			i++
//		}
//	case "attrKey":
//		for item, _ := range v.Attr {
//			m[i] = item
//			i++
//		}
//	case "attrVal":
//		for _, item := range v.Attr {
//			m[i] = item
//			i++
//		}
//	}
//	for _, item := range m {
//		childItems := getMapItem(item, typ)
//		for _, childItem := range childItems {
//			m[i] = childItem
//			i++
//		}
//	}
//	return m
//}
//
//func valueMapToIntMap(v *Value) (mapKey, mapVal, attrKey, attrVal map[int]*Value) {
//	mapKey = getMapItem(v, "mapKey")
//	mapVal = getMapItem(v, "mapVal")
//	attrKey = getMapItem(v, "attrKey")
//	attrVal = getMapItem(v, "attrVal")
//
//	for _, m := range []map[int]*Value{mapKey, mapVal, attrKey, attrVal} {
//		for _, item := range m {
//			item.Map = nil
//			item.Attr = nil
//		}
//	}
//
//	return
//}
//
//var _ = Describe("resp proto", func() {
//	It("should format", func() {
//		pp := []byte{
//			SimpleString,
//			SimpleError,
//			BlobString,
//			Number,
//			Null,
//			Float,
//			Bool,
//			BlobError,
//			VerbatimString,
//			BigNumber,
//			Array,
//			Map,
//			Set,
//			Attribute,
//			Push,
//		}
//		for _, p := range pp {
//			wr := new(bytes.Buffer)
//			r := NewRespReader(wr)
//
//			wr.WriteString(fmt.Sprintf("%s1024\n\r\n", string(p)))
//			Expect(*(r.ReadReply())).To(Equal(Value{
//				err: fmt.Errorf("redis: invalid reply: %q", fmt.Sprintf("%s1024\n", string(p))),
//			}))
//		}
//	})
//	It("read", func() {
//		for _, l := range list {
//			wr := new(bytes.Buffer)
//			r := NewRespReader(wr)
//
//			wr.WriteString(l.resp)
//
//			v := r.ReadReply()
//
//			vMapKey, vMapVal, vAttrKey, vAttrVal := valueMapToIntMap(v)
//			eMapKey, eMapVal, eAttrKey, eAttrVal := valueMapToIntMap(l.expect)
//
//			Expect(vMapKey).To(Equal(eMapKey))
//			Expect(vMapVal).To(Equal(eMapVal))
//			Expect(vAttrKey).To(Equal(eAttrKey))
//			Expect(vAttrVal).To(Equal(eAttrVal))
//
//			v.Map, v.Attr = nil, nil
//			l.expect.Map, l.expect.Attr = nil, nil
//			Expect(v).To(Equal(l.expect))
//		}
//	})
//})
