package redis_test

import (
	"bufio"

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

func (t *RequestTest) BenchmarkStatusReq(c *C) {
	c.StopTimer()

	lineReader := NewLineReader([]byte("+OK\r\n"))
	rd := bufio.NewReaderSize(lineReader, 1024)
	req := redis.NewStatusReq()

	for i := 0; i < 10; i++ {
		vI, err := req.ParseReply(rd)
		c.Check(err, IsNil)
		c.Check(vI, Equals, "OK")

		req.SetVal(vI)
		c.Check(req.Err(), IsNil)
		c.Check(req.Val(), Equals, "OK")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		v, _ := req.ParseReply(rd)
		req.SetVal(v)
		req.Err()
		req.Val()
	}
}
