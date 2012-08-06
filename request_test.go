package redis_test

import (
	"github.com/vmihailenco/bufreader"
	. "launchpad.net/gocheck"

	"github.com/vmihailenco/redis"
)

//------------------------------------------------------------------------------

type RequestTest struct{}

var _ = Suite(&RequestTest{})

//------------------------------------------------------------------------------

func (t *RequestTest) SetUpTest(c *C) {}

func (t *RequestTest) TearDownTest(c *C) {}

//------------------------------------------------------------------------------

func (t *RequestTest) BenchmarkStatusReq(c *C) {
	c.StopTimer()

	rd := bufreader.NewSizedReader(1024)
	rd.Set([]byte("+OK\r\n"))
	req := redis.NewStatusReq()

	for i := 0; i < 10; i++ {
		rd.ResetPos()
		vI, err := req.ParseReply(rd)
		c.Check(err, IsNil)
		c.Check(vI, Equals, "OK")

		req.SetVal(vI)
		c.Check(req.Err(), IsNil)
		c.Check(req.Val(), Equals, "OK")
	}

	c.StartTimer()

	for i := 0; i < c.N; i++ {
		rd.ResetPos()
		v, _ := req.ParseReply(rd)
		req.SetVal(v)
		req.Err()
		req.Val()
	}
}
