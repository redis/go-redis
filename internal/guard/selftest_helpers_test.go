//go:build guard

package guard

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Self tests for the guard: scenarios run scripts/guard-delta.sh on scratch
// repositories, mutations change one file of a copy of HEAD. Both need git
// and a checkout; run with make guard.selftest.

func repoTop(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	return strings.TrimSpace(string(out))
}

var (
	binOnce sync.Once
	binPath string
	binErr  error
	binDir  string
)

// guardBin builds cmd/guard once per test binary.
func guardBin(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		binDir, binErr = os.MkdirTemp("", "guard-bin-")
		if binErr != nil {
			return
		}
		binPath = filepath.Join(binDir, "guard")
		out, err := exec.Command("go", "build", "-o", binPath, "./cmd/guard").CombinedOutput()
		if err != nil {
			binErr = &buildError{out: string(out), err: err}
		}
	})
	if binErr != nil {
		t.Fatalf("build guard: %v", binErr)
	}
	return binPath
}

type buildError struct {
	out string
	err error
}

func (e *buildError) Error() string { return e.err.Error() + "\n" + e.out }

func TestMain(m *testing.M) {
	code := m.Run()
	if binDir != "" {
		_ = os.RemoveAll(binDir)
	}
	os.Exit(code)
}

// exportHead extracts HEAD of the enclosing repository into dir.
func exportHead(t *testing.T, dir string) {
	t.Helper()
	tarball := filepath.Join(t.TempDir(), "head.tar")
	run(t, "", nil, "git", "-C", repoTop(t), "archive", "-o", tarball, "HEAD")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, "", nil, "tar", "-x", "-f", tarball, "-C", dir)
}

// gitEnv isolates scratch repositories from the user's git config: no
// signing, no hooks, fixed identity.
var gitEnv = []string{
	"GIT_CONFIG_GLOBAL=/dev/null",
	"GIT_CONFIG_NOSYSTEM=1",
	"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
	"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	full := append([]string{"-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)
	return run(t, dir, gitEnv, "git", full...)
}

func run(t *testing.T, dir string, env []string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

// replaceOnce replaces old with new in path; old must occur exactly once.
func replaceOnce(t *testing.T, path, old, new string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(data, []byte(old)); n != 1 {
		t.Fatalf("%s: %q occurs %d times, want 1", path, old, n)
	}
	if err := os.WriteFile(path, bytes.Replace(data, []byte(old), []byte(new), 1), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendTo(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(text); err != nil {
		t.Fatal(err)
	}
}
