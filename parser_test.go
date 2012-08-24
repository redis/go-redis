package redis_test

import (
	"bytes"

	"github.com/vmihailenco/bufio"
	. "launchpad.net/gocheck"

	"github.com/vmihailenco/redis"
)

type ParserTest struct{}

var _ = Suite(&ParserTest{})

func (t *ParserTest) TestParseReq(c *C) {
	buf := bytes.NewBufferString("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n")
	rd := bufio.NewReaderSize(buf, 1024)

	args, err := redis.ParseReq(rd)
	c.Check(err, IsNil)
	c.Check(args, DeepEquals, []string{"SET", "key", "hello"})
}
