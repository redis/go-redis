//go:build guard

package guard

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The scenarios follow the prototype runs in the design notes (sections 11.5
// and 13.2 of go-redis-guard-goldens-vs-base-compare.md). Each delta is
// computed by scripts/guard-delta.sh on a scratch repository made from HEAD.
// Both scenarios assert the same gated lines for the same change, so their
// digests match too (A1 = step 1, C1 = C3 = step 7, C2 = step 4).
// Step 2b of the prototype (head against the tip of master) is left out: the
// script always uses the merge base.

const (
	ttlMinus1     = "- 01_constants | . | KeepTTL | -1"
	ttlMinus2     = "+ 01_constants | . | KeepTTL | -2"
	retries3      = "- 02_defaults | . | *Options.init | assign:opt.MaxRetries = 3"
	retries4      = "+ 02_defaults | . | *Options.init | assign:opt.MaxRetries = 4"
	closedOld     = `- 03_errors | internal/pool | errors | New | "redis: client is closed"`
	closedNew     = `+ 03_errors | internal/pool | errors | New | "redis: client closed"`
	keepTTLLine   = "const KeepTTL = -1"
	keepTTLLine2  = "const KeepTTL = -2"
	retriesLine   = "\t\topt.MaxRetries = 3\n"
	retriesLine4  = "\t\topt.MaxRetries = 4\n"
	errClosedText = `errors.New("redis: client is closed")`
	errClosedNew  = `errors.New("redis: client closed")`
)

type deltaResult struct {
	rc     int
	digest string // "" when nothing needs approval
	gated  []string
	info   []string
	out    string
}

type scratch struct {
	t    *testing.T
	repo string
}

func newScratch(t *testing.T) *scratch {
	t.Helper()
	s := &scratch{t: t, repo: filepath.Join(t.TempDir(), "repo")}
	exportHead(t, s.repo)
	s.git("init", "-q", "-b", "master")
	s.git("add", "-A")
	s.git("commit", "-q", "-m", "M0 base")
	return s
}

func (s *scratch) git(args ...string) string { s.t.Helper(); return git(s.t, s.repo, args...) }

func (s *scratch) path(rel string) string { return filepath.Join(s.repo, filepath.FromSlash(rel)) }

func (s *scratch) edit(rel, old, new, msg string) {
	s.t.Helper()
	replaceOnce(s.t, s.path(rel), old, new)
	s.git("commit", "-q", "-am", msg)
}

// delta runs scripts/guard-delta.sh <base> <head> on the scratch repo.
func (s *scratch) delta(base, head string) deltaResult {
	s.t.Helper()
	script := filepath.Join(repoTop(s.t), "scripts", "guard-delta.sh")
	cmd := exec.Command("bash", script, base, head)
	cmd.Env = append(os.Environ(), "GUARD_REPO="+s.repo, "GUARD_BIN="+guardBin(s.t))
	out, err := cmd.CombinedOutput()
	r := deltaResult{out: string(out)}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		r.rc = exit.ExitCode()
	default:
		s.t.Fatalf("run guard-delta: %v", err)
	}
	if r.rc != 0 && r.rc != 1 {
		s.t.Fatalf("guard-delta exit %d:\n%s", r.rc, out)
	}
	section := ""
	for _, line := range strings.Split(r.out, "\n") {
		switch {
		case strings.HasPrefix(line, "needs approval:"):
			section = "gated"
		case strings.HasPrefix(line, "for information:"):
			section = "info"
		case strings.HasPrefix(line, "digest: "):
			if d := strings.TrimPrefix(line, "digest: "); !strings.HasPrefix(d, "none") {
				r.digest = d
			}
			section = ""
		case strings.HasPrefix(line, "  "):
			if section == "gated" {
				r.gated = append(r.gated, strings.TrimSpace(line))
			} else if section == "info" {
				r.info = append(r.info, strings.TrimSpace(line))
			}
		}
	}
	if (r.rc == 1) != (r.digest != "") || (r.digest != "") != (len(r.gated) > 0) {
		s.t.Fatalf("exit %d, digest %q and %d gated lines disagree:\n%s", r.rc, r.digest, len(r.gated), out)
	}
	return r
}

func expectGated(t *testing.T, step string, r deltaResult, want ...string) {
	t.Helper()
	if want == nil {
		want = []string(nil)
	}
	if !reflect.DeepEqual(r.gated, want) {
		t.Fatalf("%s: gated\n got %q\nwant %q\n%s", step, r.gated, want, r.out)
	}
}

func expectSame(t *testing.T, step string, a, b deltaResult) {
	t.Helper()
	if a.digest != b.digest {
		t.Fatalf("%s: digest %q, want %q", step, b.digest, a.digest)
	}
}

func expectDiffer(t *testing.T, step string, a, b deltaResult) {
	t.Helper()
	if a.digest == b.digest {
		t.Fatalf("%s: digest %q did not change", step, b.digest)
	}
}

// TestScenarioPushesAndMasterMoves: how the delta reacts to pushes on the PR
// and to master moving (design notes, section 11.5).
func TestScenarioPushesAndMasterMoves(t *testing.T) {
	s := newScratch(t)

	s.git("checkout", "-q", "-b", "pr")
	s.edit("commands.go", keepTTLLine, keepTTLLine2, "P1 change KeepTTL")
	d1 := s.delta("master", "pr")
	expectGated(t, "1 PR opened", d1, ttlMinus2, ttlMinus1)

	s.git("checkout", "-q", "master")
	s.edit("internal/pool/pool.go", errClosedText, errClosedNew, "M1 change ErrClosed text")
	s.git("checkout", "-q", "pr")
	d2 := s.delta("master", "pr")
	expectGated(t, "2 master moved", d2, ttlMinus2, ttlMinus1)
	expectSame(t, "2 master moved", d1, d2)

	appendTo(t, s.path("version.go"), "\n// unrelated comment\n")
	if err := os.WriteFile(s.path("guard_scenario_test.go"), []byte("package redis\n\nimport \"testing\"\n\nfunc TestGuardScenario(t *testing.T) {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.git("add", "-A")
	s.git("commit", "-q", "-m", "P2 unrelated change and a new test")
	d3 := s.delta("master", "pr")
	expectSame(t, "3 unrelated push", d1, d3)
	if !contains(d3.info, "+ 09_tests | . | TestGuardScenario | func") {
		t.Fatalf("3 unrelated push: new test not reported:\n%s", d3.out)
	}

	s.edit("options.go", retriesLine, retriesLine4, "P3 change MaxRetries default")
	d4 := s.delta("master", "pr")
	expectGated(t, "4 second guarded change", d4, ttlMinus2, retries4, ttlMinus1, retries3)
	expectDiffer(t, "4 second guarded change", d1, d4)

	s.git("merge", "-q", "--no-edit", "master")
	d5 := s.delta("master", "pr")
	expectSame(t, "5 merge master, no interaction", d4, d5)

	s.git("checkout", "-q", "master")
	s.edit("commands.go", keepTTLLine, keepTTLLine2, "M2 same KeepTTL change")
	s.git("checkout", "-q", "pr")
	d6 := s.delta("master", "pr")
	expectSame(t, "6 master made the same change, PR untouched", d4, d6)

	s.git("merge", "-q", "--no-edit", "master")
	d7 := s.delta("master", "pr")
	expectGated(t, "7 merge master again", d7, retries4, retries3)
	expectDiffer(t, "7 merge master again", d4, d7)
	expectDiffer(t, "7 merge master again", d1, d7)

	s.edit("options.go", retriesLine4, retriesLine, "P4 revert MaxRetries")
	d8 := s.delta("master", "pr")
	expectGated(t, "8 revert", d8)
	if d8.rc != 0 {
		t.Fatalf("8 revert: exit %d, want 0", d8.rc)
	}
}

// TestScenarioTargetBranch: the PR targets another branch than the one it
// was created from (design notes, section 13.2).
func TestScenarioTargetBranch(t *testing.T) {
	s := newScratch(t)
	s.git("branch", "v9.23")
	s.edit("internal/pool/pool.go", errClosedText, errClosedNew, "M1 change ErrClosed text (master only)")

	s.git("checkout", "-q", "-b", "pr")
	s.edit("commands.go", keepTTLLine, keepTTLLine2, "P1 change KeepTTL")
	a1 := s.delta("master", "pr")
	expectGated(t, "A1 target master", a1, ttlMinus2, ttlMinus1)
	a2 := s.delta("v9.23", "pr")
	expectGated(t, "A2 target v9.23", a2, ttlMinus2, closedNew, ttlMinus1, closedOld)
	expectDiffer(t, "A2 target v9.23", a1, a2)

	s.git("checkout", "-q", "-b", "backport", "v9.23")
	s.git("cherry-pick", "pr")
	b := s.delta("v9.23", "backport")
	expectSame(t, "B backport", a1, b)

	s.git("checkout", "-q", "-b", "feature", "master")
	s.edit("commands.go", keepTTLLine, keepTTLLine2, "F1 change KeepTTL")
	s.git("checkout", "-q", "-b", "pr2")
	s.edit("options.go", retriesLine, retriesLine4, "Q1 change MaxRetries")
	c1 := s.delta("feature", "pr2")
	expectGated(t, "C1 stacked PR", c1, retries4, retries3)

	s.git("checkout", "-q", "master")
	s.git("merge", "-q", "--squash", "feature")
	s.git("commit", "-q", "-m", "feature squashed (#1)")
	c2 := s.delta("master", "pr2")
	expectGated(t, "C2 parent squash-merged", c2, ttlMinus2, retries4, ttlMinus1, retries3)

	s.git("checkout", "-q", "pr2")
	s.git("merge", "-q", "--no-edit", "master")
	c3 := s.delta("master", "pr2")
	expectSame(t, "C3 merge master into pr2", c1, c3)
}

// TestScenarioWorkingTree: make guard compares the working tree, including
// untracked files, and skips files deleted from it.
func TestScenarioWorkingTree(t *testing.T) {
	s := newScratch(t)
	s.git("checkout", "-q", "-b", "pr")
	replaceOnce(t, s.path("commands.go"), keepTTLLine, keepTTLLine2)
	if err := os.WriteFile(s.path("guard_untracked.go"), []byte("package redis\n\nfunc GuardUntracked() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(s.path("version.go")); err != nil {
		t.Fatal(err)
	}

	script := filepath.Join(repoTop(t), "scripts", "guard-delta.sh")
	cmd := exec.Command("bash", script, "master")
	cmd.Env = append(os.Environ(), "GUARD_REPO="+s.repo, "GUARD_BIN="+guardBin(t))
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("want exit 1, got %v:\n%s", err, out)
	}
	for _, want := range []string{
		ttlMinus2, ttlMinus1,
		"+ 11_api | . | func GuardUntracked | func()",
		"- 11_api | . | func Version | func() string",
	} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
