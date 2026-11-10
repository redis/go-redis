//go:build guard

package guard

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Layer 9: anti-cheat. Records what keeps tests and linters honest, so
// weakening them to make CI pass shows up as a diff:
//
//   - test functions (Test*, Fuzz*, Benchmark*, Example*) per package;
//   - Ginkgo nodes (Describe, It, Entry, ...) including focused (F*) and
//     pending (P*, X*) variants and Pending/Focus decorators;
//   - skips: t.Skip*, Ginkgo Skip, SkipBeforeRedisVersion, SkipAfterRedisVersion;
//   - //nolint comments in any Go file;
//   - "go test" lines in the Makefile, workflows and actions, and the
//     golangci-lint config.
//
// Test files are scanned in every directory, including e2e, doctests and fuzz.
func TestGuard_9_AntiCheat(t *testing.T) {
	var entries []GuardEntry
	for _, f := range allGoSources(t) {
		pkg := f.file.Name.Name
		if f.test {
			for _, decl := range f.file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && isTestFuncName(fn.Name.Name) {
					entries = append(entries, GuardEntry{Package: pkg, Entity: fn.Name.Name, Detail: "func"})
				}
			}
		}
		for _, decl := range f.file.Decls {
			owner := declName(decl)
			ast.Inspect(decl, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := calleeName(call)
				switch {
				case f.test && isGinkgoNode(name):
					entries = append(entries, GuardEntry{Package: pkg, Entity: owner, Detail: ginkgoDetail(name, call)})
				case isSkipCall(name):
					entries = append(entries, GuardEntry{Package: pkg, Entity: owner, Detail: "skip: " + exprStr(call)})
				}
				return true
			})
		}
		for _, cg := range f.file.Comments {
			for _, c := range cg.List {
				if isNolintDirective(c.Text) {
					entries = append(entries, GuardEntry{
						Package: pkg,
						Entity:  ownerAt(f.file, c.Pos()),
						Detail:  "nolint: " + strings.TrimSpace(strings.TrimPrefix(c.Text, "//")),
					})
				}
			}
		}
	}
	entries = append(entries, ciTestLines(t)...)
	sortEntries(entries)
	compareGoldenAdditive(t, "guard_9_anticheat.json", entries, isAllowedTestAddition)
}

// isAllowedTestAddition reports whether a new Layer 9 entry may appear
// without a golden update: a new test function, a plain (not focused or
// pending) Ginkgo node, or a Redis version gate. New skips, //nolint,
// focus/pending specs and CI/lint config changes still fail.
func isAllowedTestAddition(e GuardEntry) bool {
	if e.Package == "ci" {
		return false
	}
	d := e.Detail
	if d == "func" {
		return true
	}
	if strings.HasPrefix(d, "skip: SkipBeforeRedisVersion(") || strings.HasPrefix(d, "skip: SkipAfterRedisVersion(") {
		return true
	}
	node, _, ok := strings.Cut(d, ":")
	if !ok || !ginkgoNodes[node] || node[0] == 'F' || node[0] == 'P' || node[0] == 'X' {
		return false
	}
	return !strings.Contains(d, " [Pending]") && !strings.Contains(d, " [Focus]")
}

// isNolintDirective matches "//nolint..." and "// nolint..." comments, not
// prose that mentions the word.
func isNolintDirective(text string) bool {
	s, ok := strings.CutPrefix(text, "//")
	if !ok {
		return false
	}
	return strings.HasPrefix(strings.TrimLeft(s, " "), "nolint")
}

type goSource struct {
	file *ast.File
	test bool
}

var (
	allSourcesOnce sync.Once
	allSources     []goSource
	allSourcesErr  error
)

// allGoSources parses every .go file, test files included, skipping only
// hidden dirs, vendor, testdata and tmp.
func allGoSources(t *testing.T) []goSource {
	t.Helper()
	allSourcesOnce.Do(func() {
		fset := token.NewFileSet()
		allSourcesErr = filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := d.Name()
			if d.IsDir() {
				if path != repoRoot && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "tmp") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(name, ".go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution|parser.ParseComments)
			if err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			allSources = append(allSources, goSource{file: file, test: strings.HasSuffix(name, "_test.go")})
			return nil
		})
	})
	if allSourcesErr != nil {
		t.Fatalf("scan: %v", allSourcesErr)
	}
	return allSources
}

func isTestFuncName(name string) bool {
	for _, p := range []string{"Test", "Fuzz", "Benchmark", "Example"} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

var ginkgoNodes = map[string]bool{}

func init() {
	for _, n := range []string{"Describe", "Context", "When", "It", "Specify", "DescribeTable", "Entry"} {
		ginkgoNodes[n] = true
		ginkgoNodes["F"+n] = true
		ginkgoNodes["P"+n] = true
		ginkgoNodes["X"+n] = true
	}
}

func isGinkgoNode(name string) bool { return ginkgoNodes[name] }

// ginkgoDetail is "<node>: <first arg>", plus Pending/Focus decorators.
func ginkgoDetail(name string, call *ast.CallExpr) string {
	detail := name + ":"
	if len(call.Args) == 0 {
		return detail
	}
	detail += " " + exprStr(call.Args[0])
	for _, a := range call.Args[1:] {
		if id, ok := a.(*ast.Ident); ok && (id.Name == "Pending" || id.Name == "Focus") {
			detail += " [" + id.Name + "]"
		}
	}
	return detail
}

func isSkipCall(name string) bool {
	switch name {
	case "Skip", "Skipf", "SkipNow", "SkipBeforeRedisVersion", "SkipAfterRedisVersion":
		return true
	}
	return false
}

func declName(decl ast.Decl) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		return funcEntity(d)
	case *ast.GenDecl:
		for _, s := range d.Specs {
			if vs, ok := s.(*ast.ValueSpec); ok && len(vs.Names) > 0 {
				return "var " + vs.Names[0].Name
			}
			if ts, ok := s.(*ast.TypeSpec); ok {
				return "type " + ts.Name.Name
			}
		}
		return d.Tok.String()
	}
	return "?"
}

// ownerAt returns the name of the declaration containing pos, or "file".
func ownerAt(file *ast.File, pos token.Pos) string {
	for _, decl := range file.Decls {
		if decl.Pos() <= pos && pos <= decl.End() {
			return declName(decl)
		}
	}
	if pos < file.Package {
		return "file header"
	}
	return "file"
}

// ciTestLines records "go test" lines from the Makefile and GitHub config,
// and every non-comment line of the golangci-lint config.
func ciTestLines(t *testing.T) []GuardEntry {
	t.Helper()
	var out []GuardEntry
	add := func(rel string, all bool) {
		data, err := os.ReadFile(filepath.Join(repoRoot, rel))
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			t.Fatalf("read %s: %v", rel, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if all || strings.Contains(line, "go test") {
				out = append(out, GuardEntry{Package: "ci", Entity: rel, Detail: line})
			}
		}
	}
	add("Makefile", false)
	for _, pattern := range []string{".github/workflows/*.yml", ".github/workflows/*.yaml", ".github/actions/*/action.yml"} {
		matches, err := filepath.Glob(filepath.Join(repoRoot, pattern))
		if err != nil {
			t.Fatalf("glob %s: %v", pattern, err)
		}
		for _, m := range matches {
			rel, _ := filepath.Rel(repoRoot, m)
			add(filepath.ToSlash(rel), false)
		}
	}
	for _, cfg := range []string{".golangci.yml", ".golangci.yaml"} {
		add(cfg, true)
	}
	return out
}
