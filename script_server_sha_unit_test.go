package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// fakeScripter simulates enough of Scripter for unit testing Script behavior.
// It records which redis commands were invoked and can simulate a flushed
// script cache.
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

	// If set, the script cache is flushed after the first SCRIPT LOAD:
	// EvalSha/EvalShaRO return NOSCRIPT until the script is loaded again,
	// which is what a real server does.
	flushedAfterFirstLoad bool

	// If set, SCRIPT LOAD fails with this error.
	scriptLoadErr error
	// If set, EvalSha/EvalShaRO return NOSCRIPT whatever was loaded.
	noScript bool
	// If set, decides the answer of EvalSha for the digest it was sent.
	evalShaHook func(sha1 string) error
}

var errFakeNoScript = proto.RedisError("NOSCRIPT No matching script. Please use EVAL.")

func (f *fakeScripter) ScriptLoad(ctx context.Context, script string) *StringCmd {
	f.mu.Lock()
	f.scriptLoadCalls++
	err := f.scriptLoadErr
	f.mu.Unlock()

	cmd := NewStringCmd(ctx)
	if err != nil {
		cmd.SetErr(err)
		return cmd
	}
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
	fail := f.noScript || (f.flushedAfterFirstLoad && f.scriptLoadCalls < 2)
	hook := f.evalShaHook
	f.mu.Unlock()

	cmd := NewCmd(ctx)
	if hook != nil {
		cmd.SetErr(hook(sha1))
		return cmd
	}
	if fail {
		// Use RedisError so go-redis NOSCRIPT detection triggers.
		cmd.SetErr(errFakeNoScript)
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
	fail := f.noScript || (f.flushedAfterFirstLoad && f.scriptLoadCalls < 2)
	f.mu.Unlock()

	cmd := NewCmd(ctx)
	if fail {
		cmd.SetErr(errFakeNoScript)
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
		hashToReturn:          strings.Repeat("b", 40),
		flushedAfterFirstLoad: true,
	}

	s := NewScriptServerSHA("return 1")
	if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("Run() err: %v", err)
	}

	// Expected:
	// - ScriptLoad twice (first load, reload after NOSCRIPT)
	// - EvalSha twice (first NOSCRIPT, second retry)
	if c.scriptLoadCalls != 2 {
		t.Fatalf("expected ScriptLoad 2 calls (reload after NOSCRIPT), got %d", c.scriptLoadCalls)
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
		hashToReturn:          strings.Repeat("d", 40),
		flushedAfterFirstLoad: true,
	}

	s := NewScriptServerSHA("return 1")
	if err := s.RunRO(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("RunRO() err: %v", err)
	}

	if c.scriptLoadCalls != 2 {
		t.Fatalf("expected ScriptLoad 2 calls (reload after NOSCRIPT), got %d", c.scriptLoadCalls)
	}
	if c.evalShaROCalls != 2 {
		t.Fatalf("expected EvalShaRO 2 calls (retry), got %d", c.evalShaROCalls)
	}
	if c.evalROCalls != 0 {
		t.Fatalf("expected EvalRO 0 calls, got %d", c.evalROCalls)
	}
}

// EvalSha and EvalShaRO have no EVAL fallback of their own (that is Run's),
// so without the reload they keep answering NOSCRIPT after a flush.
func TestNewScriptServerSHA_EvalSha_ReloadsOnNoScript(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{
		hashToReturn:          strings.Repeat("f", 40),
		flushedAfterFirstLoad: true,
	}

	s := NewScriptServerSHA("return 1")
	if err := s.EvalSha(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("EvalSha() err: %v", err)
	}
	if c.scriptLoadCalls != 2 {
		t.Fatalf("expected ScriptLoad 2 calls (reload after NOSCRIPT), got %d", c.scriptLoadCalls)
	}
	if c.evalShaCalls != 2 {
		t.Fatalf("expected EvalSha 2 calls (retry), got %d", c.evalShaCalls)
	}

	if err := s.EvalShaRO(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("EvalShaRO() err: %v", err)
	}
}

func TestNewScriptServerSHA_EvalShaRO_ReloadsOnNoScript(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{
		hashToReturn:          strings.Repeat("f", 40),
		flushedAfterFirstLoad: true,
	}

	s := NewScriptServerSHA("return 1")
	if err := s.EvalShaRO(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("EvalShaRO() err: %v", err)
	}
	if c.scriptLoadCalls != 2 {
		t.Fatalf("expected ScriptLoad 2 calls (reload after NOSCRIPT), got %d", c.scriptLoadCalls)
	}
	if c.evalShaROCalls != 2 {
		t.Fatalf("expected EvalShaRO 2 calls (retry), got %d", c.evalShaROCalls)
	}
}

// A reload that fails (SCRIPT LOAD denied by ACL, network error, load only
// queued) must not drop the cached digest: the EVAL fallback puts the script
// back into the server's cache, so the next call can use EVALSHA again.
func TestNewScriptServerSHA_FailedReloadKeepsDigest(t *testing.T) {
	hash := strings.Repeat("8", 40)
	tests := map[string]func(c *fakeScripter){
		"load error": func(c *fakeScripter) {
			c.scriptLoadErr = proto.RedisError("NOPERM this user has no permissions to run the 'script|load' command")
		},
		"queued load": func(c *fakeScripter) { c.hashToReturn = "" },
	}
	for name, breakLoad := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c := &fakeScripter{hashToReturn: hash}

			s := NewScriptServerSHA("return 1")
			if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
				t.Fatalf("Run() err: %v", err)
			}

			// The script cache is flushed and SCRIPT LOAD stops working.
			c.noScript = true
			breakLoad(c)
			if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
				t.Fatalf("Run() after flush err: %v", err)
			}
			if c.scriptLoadCalls != 2 || c.evalCalls != 1 {
				t.Fatalf("expected a failed reload and the Eval fallback, got ScriptLoad %d calls, Eval %d calls", c.scriptLoadCalls, c.evalCalls)
			}
			if got := s.Hash(); got != hash {
				t.Errorf("Hash() = %q after a failed reload, want %q", got, hash)
			}

			// The server has the script again; SCRIPT LOAD still does not work.
			c.noScript = false
			if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
				t.Fatalf("Run() after recovery err: %v", err)
			}
			if err := s.RunRO(ctx, c, []string{"k"}).Err(); err != nil {
				t.Fatalf("RunRO() after recovery err: %v", err)
			}
			// EvalSha: first run, the miss, the run after recovery.
			if c.evalShaCalls != 3 || c.lastEvalShaSHA != hash || c.evalShaROCalls != 1 || c.lastEvalShaROSHA != hash {
				t.Errorf("expected EVALSHA and EVALSHA_RO with the cached digest after recovery, got EvalSha %d calls (last %q), EvalShaRO %d calls (last %q)",
					c.evalShaCalls, c.lastEvalShaSHA, c.evalShaROCalls, c.lastEvalShaROSHA)
			}
			if c.scriptLoadCalls != 2 || c.evalCalls != 1 || c.evalROCalls != 0 {
				t.Errorf("expected no reload and no fallback after recovery, got ScriptLoad %d calls, Eval %d calls, EvalRO %d calls",
					c.scriptLoadCalls, c.evalCalls, c.evalROCalls)
			}
		})
	}
}

// Callers that get NOSCRIPT at the same time all reload. SCRIPT LOAD answers
// with the digest they already hold, so none of them may treat the digest
// another one has just stored as stale and none may retry with an empty one.
func TestNewScriptServerSHA_ConcurrentReloadNeverSendsEmptyDigest(t *testing.T) {
	const callers = 8

	ctx := context.Background()
	hash := strings.Repeat("9", 40)

	// The interleaving that breaks this cannot be forced from a Scripter, so
	// the collision is repeated.
	for round := 0; round < 200; round++ {
		var (
			evalShas atomic.Int32
			empty    atomic.Int32
			missed   sync.WaitGroup
		)
		missed.Add(callers)
		c := &fakeScripter{hashToReturn: hash}
		c.evalShaHook = func(sha1 string) error {
			if sha1 == "" {
				empty.Add(1)
				return errFakeNoScript
			}
			if evalShas.Add(1) <= callers {
				// Every caller misses once, and all of them have missed
				// before the first one starts to reload.
				missed.Done()
				missed.Wait()
				return errFakeNoScript
			}
			return nil
		}

		s := NewScriptServerSHA("return 1")
		if err := s.Load(ctx, c).Err(); err != nil {
			t.Fatalf("Load() err: %v", err)
		}

		errs := make([]error, callers)
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = s.EvalSha(ctx, c, []string{"k"}).Err()
			}(i)
		}
		wg.Wait()

		if n := empty.Load(); n != 0 {
			t.Fatalf("round %d: EVALSHA was sent with an empty digest %d times", round, n)
		}
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d: EvalSha() of caller %d err: %v", round, i, err)
			}
		}
		if got := s.Hash(); got != hash {
			t.Fatalf("round %d: Hash() = %q, want %q", round, got, hash)
		}
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

func TestScriptLoad_UppercaseDigestIsNotAnError(t *testing.T) {
	ctx := context.Background()
	s := NewScript("return 5")
	want := s.Hash()
	c := &fakeScripter{hashToReturn: strings.ToUpper(want)}

	if err := s.Load(ctx, c).Err(); err != nil {
		t.Fatalf("Load() err: %v", err)
	}
	if got := s.Hash(); got != want {
		t.Fatalf("Hash() = %q after Load, want %q", got, want)
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

func TestScriptServerSHA_Run_QueuedLoadFallsBackToEval(t *testing.T) {
	// A SCRIPT LOAD buffered in a pipeline answers with an empty digest until
	// the pipeline executes, so Run must not send EVALSHA "".
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: ""}

	s := NewScriptServerSHA("return 1")
	if err := s.Run(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("Run() err: %v", err)
	}
	if c.evalShaCalls != 0 {
		t.Fatalf("expected no EVALSHA, got %d calls with digest %q", c.evalShaCalls, c.lastEvalShaSHA)
	}
	if c.evalCalls != 1 {
		t.Fatalf("expected Eval 1 call, got %d", c.evalCalls)
	}
}

func TestScriptServerSHA_RunRO_QueuedLoadFallsBackToEvalRO(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: ""}

	s := NewScriptServerSHA("return 1")
	if err := s.RunRO(ctx, c, []string{"k"}).Err(); err != nil {
		t.Fatalf("RunRO() err: %v", err)
	}
	if c.evalShaROCalls != 0 {
		t.Fatalf("expected no EVALSHA_RO, got %d calls with digest %q", c.evalShaROCalls, c.lastEvalShaROSHA)
	}
	if c.evalROCalls != 1 {
		t.Fatalf("expected EvalRO 1 call, got %d", c.evalROCalls)
	}
}
