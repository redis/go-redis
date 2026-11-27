//go:build guard

package guard

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// mutation changes one file of a copy of HEAD. want maps each layer that
// must change to whether that layer's change needs approval. No other layer
// may change.
type mutation struct {
	name   string
	file   string
	mutate func(t *testing.T, path string)
	want   map[string]bool
}

func replace(old, new string) func(*testing.T, string) {
	return func(t *testing.T, path string) { replaceOnce(t, path, old, new) }
}

func add(text string) func(*testing.T, string) {
	return func(t *testing.T, path string) { appendTo(t, path, text) }
}

var mutations = []mutation{
	{
		"L1 constant value", "commands.go",
		replace("const KeepTTL = -1", "const KeepTTL = -2"),
		map[string]bool{"01_constants": true},
	},
	{
		"L1 new constant", "commands.go",
		add("\nconst guardMutantConst = 1\n"),
		map[string]bool{"01_constants": false},
	},
	{
		"L2 default value", "options.go",
		replace("\t\topt.MaxRetries = 3\n", "\t\topt.MaxRetries = 4\n"),
		map[string]bool{"02_defaults": true},
	},
	{
		"L3 error string", "internal/pool/pool.go",
		replace(`errors.New("redis: client is closed")`, `errors.New("redis: client closed")`),
		map[string]bool{"03_errors": true},
	},
	{
		"L3 new error", "internal/pool/pool.go",
		add("\nfunc guardMutantErr() error { return errors.New(\"guard mutant\") }\n"),
		map[string]bool{"03_errors": false},
	},
	{
		"L4 log message", "redis.go",
		replace(`"redis: re-authentication failed: %v"`, `"redis: reauth failed: %v"`),
		map[string]bool{"04_logs": true},
	},
	{
		"L5 predicate result", "error.go",
		replace("if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {\n\t\treturn true",
			"if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {\n\t\treturn false"),
		map[string]bool{"05_error_policy": true},
	},
	{
		"L5 reordered checks", "error.go",
		replace("\tif err == nil {\n\t\treturn false\n\t}\n\n\t// Check for EOF errors (works with wrapped errors)\n\tif errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {\n\t\treturn true\n\t}\n",
			"\t// Check for EOF errors (works with wrapped errors)\n\tif errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {\n\t\treturn true\n\t}\n\n\tif err == nil {\n\t\treturn false\n\t}\n"),
		map[string]bool{"05_error_policy": true},
	},
	{
		"L6 panic outside constructor", "internal/pool/pool.go",
		add("\nfunc guardMutantCrash() { panic(\"guard mutant\") }\n"),
		// The panic call is also a new layer 3 entry.
		map[string]bool{"06_panics": true, "03_errors": false},
	},
	{
		"L7 go directive", "go.mod",
		replace("\ngo 1.26.0\n", "\ngo 1.26.1\n"),
		map[string]bool{"07_deps": true},
	},
	{
		"L8 new import", "internal/util.go",
		replace("import (\n\t\"context\"\n", "import (\n\t\"context\"\n\t_ \"os/exec\"\n"),
		map[string]bool{"08_imports": true},
	},
	{
		"L9 new skip", "internal/util_test.go",
		replace("func TestToLower(t *testing.T) {\n", "func TestToLower(t *testing.T) {\n\tt.Skip(\"guard mutant\")\n"),
		map[string]bool{"09_tests": false},
	},
	{
		"L9 nolint directive", "internal/util.go",
		add("\n//nolint:unused\nvar guardMutantUnused = 1\n"),
		map[string]bool{"09_tests": false},
	},
	{
		"L10 command token", "string_commands.go",
		replace(`NewBoolCmd(ctx, "set", key, value, "keepttl", "nx")`, `NewBoolCmd(ctx, "set", key, value, "keepttl", "xx")`),
		map[string]bool{"10_protocol": true},
	},
	{
		"L10 added option token", "string_commands.go",
		replace(`NewBoolCmd(ctx, "set", key, value, "keepttl", "nx")`, `NewBoolCmd(ctx, "set", key, value, "keepttl", "nx", "get")`),
		map[string]bool{"10_protocol": false},
	},
	{
		"L10 dropped token", "string_commands.go",
		replace(`NewBoolCmd(ctx, "set", key, value, "keepttl", "nx")`, `NewBoolCmd(ctx, "set", key, value, "nx")`),
		map[string]bool{"10_protocol": true},
	},
	{
		"L11 promoted method signature", "commands.go",
		replace("func (c cmdable) WaitAOF(ctx context.Context, numLocal, numSlaves int, timeout time.Duration) *IntSliceCmd {",
			"func (c cmdable) WaitAOF(ctx context.Context, numLocal int, timeout time.Duration) *IntSliceCmd {"),
		map[string]bool{"11_api": true},
	},
	{
		"L11 changed signature", "commands.go",
		replace("\tEcho(ctx context.Context, message interface{}) *StringCmd\n", "\tEcho(ctx context.Context, message string) *StringCmd\n"),
		map[string]bool{"11_api": true},
	},
	{
		"L11 new exported func", "commands.go",
		add("\nfunc GuardMutantAPI() {}\n"),
		map[string]bool{"11_api": false},
	},
	{
		"L11 new interface method", "commands.go",
		replace("\tEcho(ctx context.Context, message interface{}) *StringCmd\n",
			"\tEcho(ctx context.Context, message interface{}) *StringCmd\n\tGuardMutant(ctx context.Context) *StringCmd\n"),
		map[string]bool{"11_api": false},
	},

	// No change expected.
	{
		"P renamed parameter", "redis.go",
		replace("func NewClient(opt *Options) *Client {", "func NewClient(o *Options) *Client {\n\topt := o"),
		// The body gains "opt := o", which no layer records.
		map[string]bool{},
	},
	{
		"P comment only", "commands.go",
		add("\n// guard mutant: comment only\n"),
		map[string]bool{},
	},
	{
		"P prose mentions nolint", "internal/util.go",
		add("\n// This comment says nolint only in prose.\n"),
		map[string]bool{},
	},
	{
		"P new test func", "internal/util_test.go",
		add("\nfunc TestGuardMutant(t *testing.T) {}\n"),
		map[string]bool{"09_tests": false},
	},
}

// TestMutations checks that each layer catches one representative change
// and classifies it as intended. It works on a copy of HEAD and restores
// each file from a backup copy after use.
func TestMutations(t *testing.T) {
	root := filepath.Join(t.TempDir(), "tree")
	exportHead(t, root)
	base, err := Extract(root)
	if err != nil {
		t.Fatal(err)
	}
	backup := t.TempDir()

	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			path := filepath.Join(root, filepath.FromSlash(m.file))
			orig, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			bk := filepath.Join(backup, filepath.Base(m.file)+".orig")
			if err := os.WriteFile(bk, orig, 0o644); err != nil {
				t.Fatal(err)
			}
			defer restore(t, bk, path, orig)

			m.mutate(t, path)
			if now, _ := os.ReadFile(path); bytes.Equal(now, orig) {
				t.Fatal("mutation changed nothing")
			}
			head, err := Extract(root)
			if err != nil {
				t.Fatal(err)
			}
			changes := Delta(base, head)
			got := map[string]bool{}
			for _, c := range changes {
				got[c.Layer] = got[c.Layer] || c.Gated
			}
			if !equalBoolMaps(got, m.want) {
				var ls []string
				for _, c := range changes {
					ls = append(ls, c.Line())
				}
				sort.Strings(ls)
				t.Fatalf("layers (layer -> gated) = %v, want %v\n%q", got, m.want, ls)
			}
			wantDigest := false
			for _, g := range m.want {
				wantDigest = wantDigest || g
			}
			if (Digest(changes) != "") != wantDigest {
				t.Fatalf("digest %q, want one: %v", Digest(changes), wantDigest)
			}
		})
	}
}

func restore(t *testing.T, backup, path string, orig []byte) {
	t.Helper()
	data, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if now, err := os.ReadFile(path); err != nil || !bytes.Equal(now, orig) {
		t.Fatalf("restore %s failed", path)
	}
}

func equalBoolMaps(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}
