package redis

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"sync"
)

type Scripter interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Cmd
	EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *Cmd
	EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Cmd
	ScriptExists(ctx context.Context, hashes ...string) *BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *StringCmd
}

var (
	_ Scripter = (*Client)(nil)
	_ Scripter = (*Ring)(nil)
	_ Scripter = (*ClusterClient)(nil)
)

var disableClientSHA1 bool

func init() {
	gd := os.Getenv("GODEBUG")
	if strings.Contains(gd, "fips140=only") || strings.Contains(gd, "fips140=on") {
		disableClientSHA1 = true
	}
	if v := os.Getenv("GO_REDIS_DISABLE_CLIENT_SHA1"); v == "1" || strings.EqualFold(v, "true") {
		disableClientSHA1 = true
	}
}

type Script struct {
	src  string
	mu   sync.RWMutex
	hash string
}

func NewScript(src string) *Script {
	s := &Script{src: src}
	if !disableClientSHA1 {
		h := sha1.New()
		_, _ = io.WriteString(h, src)
		s.hash = hex.EncodeToString(h.Sum(nil))
	}
	return s
}

func (s *Script) Hash() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hash
}

func (s *Script) Load(ctx context.Context, c Scripter) *StringCmd {
	cmd := c.ScriptLoad(ctx, s.src)
	if err := cmd.Err(); err == nil {
		s.mu.Lock()
		s.hash = cmd.Val()
		s.mu.Unlock()
	}
	return cmd
}

func (s *Script) Exists(ctx context.Context, c Scripter) *BoolSliceCmd {
	s.mu.RLock()
	hash := s.hash
	s.mu.RUnlock()
	if hash == "" {
		// If we don't have a hash yet, this checks an empty hash.
		// We don't force a load here to keep API behavior simple.
		return c.ScriptExists(ctx, "")
	}
	return c.ScriptExists(ctx, hash)
}

func (s *Script) Eval(ctx context.Context, c Scripter, keys []string, args ...interface{}) *Cmd {
	return c.Eval(ctx, s.src, keys, args...)
}

func (s *Script) EvalRO(ctx context.Context, c Scripter, keys []string, args ...interface{}) *Cmd {
	return c.EvalRO(ctx, s.src, keys, args...)
}

// ensureHash ensures that s.hash is populated by using SCRIPT LOAD.
// It never calls SHA-1 in Go; Redis computes and returns the digest.
func (s *Script) ensureHash(ctx context.Context, c Scripter) error {
	// Fast path: read lock, return if hash is already set.
	s.mu.RLock()
	if s.hash != "" {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	// Slow path: acquire write lock and load.
	s.mu.Lock()
	if s.hash != "" {
		s.mu.Unlock()
		return nil
	}
	cmd := c.ScriptLoad(ctx, s.src)
	if err := cmd.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.hash = cmd.Val()
	s.mu.Unlock()
	return nil
}

func (s *Script) EvalSha(ctx context.Context, c Scripter, keys []string, args ...interface{}) *Cmd {
	// Non-FIPS: behave like upstream go-redis (client-side SHA-1, direct EVALSHA).
	if !disableClientSHA1 {
		s.mu.RLock()
		hash := s.hash
		s.mu.RUnlock()
		return c.EvalSha(ctx, hash, keys, args...)
	}

	// FIPS: server-side SHA via SCRIPT LOAD + EVALSHA.
	if err := s.ensureHash(ctx, c); err != nil {
		// If we cannot load the script, fall back to EVAL so we don't fail.
		return s.Eval(ctx, c, keys, args...)
	}

	s.mu.RLock()
	hash := s.hash
	s.mu.RUnlock()

	r := c.EvalSha(ctx, hash, keys, args...)
	if HasErrorPrefix(r.Err(), "NOSCRIPT") {
		// Script cache was flushed; reload and retry once.
		if err := s.ensureHash(ctx, c); err != nil {
			return s.Eval(ctx, c, keys, args...)
		}
		s.mu.RLock()
		hash = s.hash
		s.mu.RUnlock()
		return c.EvalSha(ctx, hash, keys, args...)
	}

	return r
}

func (s *Script) EvalShaRO(ctx context.Context, c Scripter, keys []string, args ...interface{}) *Cmd {
	if !disableClientSHA1 {
		s.mu.RLock()
		hash := s.hash
		s.mu.RUnlock()
		return c.EvalShaRO(ctx, hash, keys, args...)
	}

	if err := s.ensureHash(ctx, c); err != nil {
		return s.EvalRO(ctx, c, keys, args...)
	}

	s.mu.RLock()
	hash := s.hash
	s.mu.RUnlock()

	r := c.EvalShaRO(ctx, hash, keys, args...)
	if HasErrorPrefix(r.Err(), "NOSCRIPT") {
		if err := s.ensureHash(ctx, c); err != nil {
			return s.EvalRO(ctx, c, keys, args...)
		}
		s.mu.RLock()
		hash = s.hash
		s.mu.RUnlock()
		return c.EvalShaRO(ctx, hash, keys, args...)
	}

	return r
}

// Run optimistically uses EVALSHA to run the script. If script does not exist
// it is retried using EVAL.
func (s *Script) Run(ctx context.Context, c Scripter, keys []string, args ...interface{}) *Cmd {
	r := s.EvalSha(ctx, c, keys, args...)
	if HasErrorPrefix(r.Err(), "NOSCRIPT") {
		return s.Eval(ctx, c, keys, args...)
	}
	return r
}

// RunRO optimistically uses EVALSHA_RO to run the script. If script does not exist
// it is retried using EVAL_RO.
func (s *Script) RunRO(ctx context.Context, c Scripter, keys []string, args ...interface{}) *Cmd {
	r := s.EvalShaRO(ctx, c, keys, args...)
	if HasErrorPrefix(r.Err(), "NOSCRIPT") {
		return s.EvalRO(ctx, c, keys, args...)
	}
	return r
}
