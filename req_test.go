package redis_test

import (
	"github.com/vmihailenco/bufio"
	. "launchpad.net/gocheck"

	"github.com/vmihailenco/redis"
)

//------------------------------------------------------------------------------

type LineReader struct {
	line []byte
}

func NewLineReader(line []byte) *LineReader {
	return &LineReader{line: line}
}

func (r *LineReader) Read(buf []byte) (int, error) {
	return copy(buf, r.line), nil
}

//------------------------------------------------------------------------------

type RequestTest struct{}

var _ = Suite(&RequestTest{})

//------------------------------------------------------------------------------

func (t *RequestTest) SetUpTest(c *C) {}

func (t *RequestTest) TearDownTest(c *C) {}

//------------------------------------------------------------------------------

func (t *RequestTest) benchmarkReq(c *C, reqString string, req redis.Req, checker Checker, expected interface{}) {
	c.StopTimer()

	lineReader := NewLineReader([]byte(reqString))
	rd := bufio.NewReaderSize(lineReader, 1024)

	for i := 0; i < 10; i++ {
		vIface, err := req.ParseReply(rd)
		c.Check(err, IsNil)
		c.Check(vIface, checker, expected)
		req.SetVal(vIface)
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		v, _ := req.ParseReply(rd)
		req.SetVal(v)
	}
}

func (t *RequestTest) BenchmarkStatusReq(c *C) {
	t.benchmarkReq(c, "+OK\r\n", redis.NewStatusReq(), Equals, "OK")
}

func (t *RequestTest) BenchmarkIntReq(c *C) {
	t.benchmarkReq(c, ":1\r\n", redis.NewIntReq(), Equals, int64(1))
}

func (t *RequestTest) BenchmarkStringReq(c *C) {
	t.benchmarkReq(c, "$5\r\nhello\r\n", redis.NewStringReq(), Equals, "hello")
}

func (t *RequestTest) BenchmarkFloatReq(c *C) {
	t.benchmarkReq(c, "$5\r\n1.111\r\n", redis.NewFloatReq(), Equals, 1.111)
}

func (t *RequestTest) BenchmarkStringSliceReq(c *C) {
	t.benchmarkReq(
		c,
		"*2\r\n$5\r\nhello\r\n$5\r\nhello\r\n",
		redis.NewStringSliceReq(),
		DeepEquals,
		[]string{"hello", "hello"},
	)
}

func (t *RequestTest) BenchmarkIfaceSliceReq(c *C) {
	t.benchmarkReq(
		c,
		"*2\r\n$5\r\nhello\r\n$5\r\nhello\r\n",
		redis.NewIfaceSliceReq(),
		DeepEquals,
		[]interface{}{"hello", "hello"},
	)
}
