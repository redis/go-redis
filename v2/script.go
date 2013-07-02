package redis

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"
)

type Script struct {
	src, hash string
}

func NewScript(src string) *Script {
	h := sha1.New()
	io.WriteString(h, src)
	return &Script{
		src:  src,
		hash: hex.EncodeToString(h.Sum(nil)),
	}
}

func (s *Script) Load(c *Client) *StringReq {
	return c.ScriptLoad(s.src)
}

func (s *Script) Exists(c *Client) *BoolSliceReq {
	return c.ScriptExists(s.src)
}

func (s *Script) Eval(c *Client, keys []string, args []string) *IfaceReq {
	return c.Eval(s.src, keys, args)
}

func (s *Script) EvalSha(c *Client, keys []string, args []string) *IfaceReq {
	return c.EvalSha(s.hash, keys, args)
}

func (s *Script) Run(c *Client, keys []string, args []string) *IfaceReq {
	r := s.EvalSha(c, keys, args)
	if err := r.Err(); err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
		return s.Eval(c, keys, args)
	}
	return r
}
