package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// fakeScripter simulates enough of Scripter for unit testing Script behavior.
// It records which redis commands were invoked and can simulate NOSCRIPT.
type fakeScripter struct {
	mu sync.Mutex

	// call counters
	scriptLoadCalls int
	evalCalls       int
	evalShaCalls    int
	evalROCalls     int
	evalShaROCalls  int

	// behavior controls
	hashToReturn string

	// digests the script helper actually sent with EVALSHA/EVALSHA_RO
	lastEvalShaSHA   string
	lastEvalShaROSHA string

	// If set, the first EvalSha/EvalShaRO returns a NOSCRIPT error.
	failFirstEvalShaWithNoScr   bool
	failFirstEvalShaROWithNoScr bool
}

func (f *fakeScripter) ScriptLoad(ctx context.Context, script string) *StringCmd {
	f.mu.Lock()
	f.scriptLoadCalls++
	f.mu.Unlock()

	cmd := NewStringCmd(ctx)
	cmd.SetVal(f.hashToReturn)
	return cmd
}

func (f *fakeScripter) ScriptExists(ctx context.Context, hashes ...string) *BoolSliceCmd {
	cmd := NewBoolSliceCmd(ctx)
	vals := make([]bool, len(hashes))
	for i := range vals {
		vals[i] = true
	}
	cmd.SetVal(vals)
	return cmd
}

func (f *fakeScripter) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *Cmd {
	f.mu.Lock()
	f.evalCalls++
	f.mu.Unlock()

	cmd := NewCmd(ctx)
	cmd.SetErr(nil)
	return cmd
}

func (f *fakeScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Cmd {
	f.mu.Lock()
	f.evalShaCalls++
	f.lastEvalShaSHA = sha1
	callNum := f.evalShaCalls
	fail := f.failFirstEvalShaWithNoScr && callNum == 1
	f.mu.Unlock()

	cmd := NewCmd(ctx)
	if fail {
		// Use RedisError so go-redis NOSCRIPT detection triggers.
		cmd.SetErr(proto.RedisError("NOSCRIPT No matching script. Please use EVAL."))
		return cmd
	}
	cmd.SetErr(nil)
	return cmd
}

func (f *fakeScripter) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *Cmd {
	f.mu.Lock()
	f.evalROCalls++
	f.mu.Unlock()

	cmd := NewCmd(ctx)
	cmd.SetErr(nil)
	return cmd
}

func (f *fakeScripter) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Cmd {
	f.mu.Lock()
	f.evalShaROCalls++
	f.lastEvalShaROSHA = sha1
	callNum := f.evalShaROCalls
	fail := f.failFirstEvalShaROWithNoScr && callNum == 1
	f.mu.Unlock()

	cmd := NewCmd(ctx)
	if fail {
		cmd.SetErr(proto.RedisError("NOSCRIPT No matching script. Please use EVAL."))
		return cmd
	}
	cmd.SetErr(nil)
	return cmd
}

func TestNewScriptServerSHA_Run_UsesScriptLoadAndEvalSha_NoEval(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: strings.Repeat("a", 40)}

	s := NewScriptServerSHA("return 1")
	if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("Run() err: %v", err)
	}

	if c.scriptLoadCalls != 1 {
		t.Fatalf("expected ScriptLoad 1 call, got %d", c.scriptLoadCalls)
	}
	if c.evalShaCalls != 1 {
		t.Fatalf("expected EvalSha 1 call, got %d", c.evalShaCalls)
	}
	if c.evalCalls != 0 {
		t.Fatalf("expected Eval 0 calls, got %d", c.evalCalls)
	}
}

func TestNewScriptServerSHA_Run_RetriesOnNoScript_NoEvalFallback(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{
		hashToReturn:              strings.Repeat("b", 40),
		failFirstEvalShaWithNoScr: true,
	}

	s := NewScriptServerSHA("return 1")
	if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("Run() err: %v", err)
	}

	// Expected:
	// - ScriptLoad once (hash cached in Script)
	// - EvalSha twice (first NOSCRIPT, second retry)
	if c.scriptLoadCalls != 1 {
		t.Fatalf("expected ScriptLoad 1 call (hash cached), got %d", c.scriptLoadCalls)
	}
	if c.evalShaCalls != 2 {
		t.Fatalf("expected EvalSha 2 calls (retry), got %d", c.evalShaCalls)
	}
	// Critical: serverSHA path should NOT fallback to Eval
	if c.evalCalls != 0 {
		t.Fatalf("expected Eval 0 calls, got %d", c.evalCalls)
	}
}

func TestNewScriptServerSHA_RunRO_UsesScriptLoadAndEvalShaRO_NoEvalRO(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: strings.Repeat("c", 40)}

	s := NewScriptServerSHA("return 1")
	if err := s.RunRO(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("RunRO() err: %v", err)
	}

	if c.scriptLoadCalls != 1 {
		t.Fatalf("expected ScriptLoad 1 call, got %d", c.scriptLoadCalls)
	}
	if c.evalShaROCalls != 1 {
		t.Fatalf("expected EvalShaRO 1 call, got %d", c.evalShaROCalls)
	}
	if c.evalROCalls != 0 {
		t.Fatalf("expected EvalRO 0 calls, got %d", c.evalROCalls)
	}
}

func TestNewScriptServerSHA_RunRO_RetriesOnNoScript_NoEvalROFallback(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{
		hashToReturn:                strings.Repeat("d", 40),
		failFirstEvalShaROWithNoScr: true,
	}

	s := NewScriptServerSHA("return 1")
	if err := s.RunRO(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("RunRO() err: %v", err)
	}

	if c.scriptLoadCalls != 1 {
		t.Fatalf("expected ScriptLoad 1 call (hash cached), got %d", c.scriptLoadCalls)
	}
	if c.evalShaROCalls != 2 {
		t.Fatalf("expected EvalShaRO 2 calls (retry), got %d", c.evalShaROCalls)
	}
	if c.evalROCalls != 0 {
		t.Fatalf("expected EvalRO 0 calls, got %d", c.evalROCalls)
	}
}

// forgedSHA is a well-formed digest that is not the SHA-1 of any script used here.
const forgedSHA = "0123456789abcdef0123456789abcdef01234567"

func TestScriptLoad_KeepsClientComputedDigest(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: forgedSHA}

	s := NewScript("return 1")
	want := s.Hash()

	err := s.Load(ctx, c).Err()
	if !errors.Is(err, ErrScriptDigestMismatch) {
		t.Fatalf("Load() err = %v, want ErrScriptDigestMismatch", err)
	}
	if got := s.Hash(); got != want {
		t.Fatalf("Hash() = %q after Load, want the locally computed %q", got, want)
	}

	if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("Run() err: %v", err)
	}
	if c.lastEvalShaSHA != want {
		t.Fatalf("EVALSHA sent %q, want %q", c.lastEvalShaSHA, want)
	}
}

func TestScriptLoad_KeepsClientComputedDigest_RO(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: forgedSHA}

	s := NewScript("return 2")
	want := s.Hash()
	_ = s.Load(ctx, c)

	if err := s.RunRO(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("RunRO() err: %v", err)
	}
	if c.lastEvalShaROSHA != want {
		t.Fatalf("EVALSHA_RO sent %q, want %q", c.lastEvalShaROSHA, want)
	}
}

func TestScriptLoad_MatchingDigestIsNotAnError(t *testing.T) {
	ctx := context.Background()
	s := NewScript("return 3")
	c := &fakeScripter{hashToReturn: s.Hash()}

	cmd := s.Load(ctx, c)
	if err := cmd.Err(); err != nil {
		t.Fatalf("Load() err: %v", err)
	}
	if cmd.Val() != s.Hash() {
		t.Fatalf("Load() val = %q, want %q", cmd.Val(), s.Hash())
	}
}

func TestScriptLoad_EmptyDigestIsNotAnError(t *testing.T) {
	// A pipelined SCRIPT LOAD has no value until Exec; Load must not flag it.
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: ""}

	s := NewScript("return 4")
	want := s.Hash()

	if err := s.Load(ctx, c).Err(); err != nil {
		t.Fatalf("Load() err: %v", err)
	}
	if got := s.Hash(); got != want {
		t.Fatalf("Hash() = %q after Load, want %q", got, want)
	}
}

func TestScriptLoad_ServerSHAAdoptsServerDigest(t *testing.T) {
	ctx := context.Background()
	hash := strings.Repeat("e", 40)
	c := &fakeScripter{hashToReturn: hash}

	s := NewScriptServerSHA("return 5")
	if err := s.Load(ctx, c).Err(); err != nil {
		t.Fatalf("Load() err: %v", err)
	}
	if s.Hash() != hash {
		t.Fatalf("Hash() = %q, want %q", s.Hash(), hash)
	}
}
