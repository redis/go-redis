package redis

import (
	"context"
	"strings"
	"testing"
)

// forgedSHA is a well-formed digest that is not the SHA-1 of any script used here.
const forgedSHA = "0123456789abcdef0123456789abcdef01234567"

func TestScriptLoad_KeepsClientComputedDigest(t *testing.T) {
	ctx := context.Background()
	c := &fakeScripter{hashToReturn: forgedSHA}

	s := NewScript("return 1")
	want := s.Hash()

	if err := s.Load(ctx, c).Err(); err == nil {
		t.Fatal("Load() err = nil, want a digest-mismatch error")
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

func TestScriptLoad_ServerSHAAdoptsServerDigest(t *testing.T) {
	ctx := context.Background()
	hash := strings.Repeat("e", 40)
	c := &fakeScripter{hashToReturn: hash}

	s := NewScriptServerSHA("return 4")
	if err := s.Load(ctx, c).Err(); err != nil {
		t.Fatalf("Load() err: %v", err)
	}
	if s.Hash() != hash {
		t.Fatalf("Hash() = %q, want %q", s.Hash(), hash)
	}
}
