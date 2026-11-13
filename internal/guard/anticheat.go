package guard

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Layer 9: tests and linters. Records what keeps tests and linters honest,
// so weakening them shows up in the report:
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
func antiCheat(t *tree) ([]Entry, error) {
	var entries []Entry
	for _, f := range t.all {
		pkg := f.file.Name.Name
		add := func(e Entry) {
			e.Dir, e.Package = f.dir, pkg
			entries = append(entries, e)
		}
		if f.test {
			for _, decl := range f.file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && isTestFuncName(fn.Name.Name) {
					add(Entry{Entity: fn.Name.Name, Detail: "func"})
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
					add(Entry{Entity: owner, Detail: ginkgoDetail(name, call)})
				case isSkipCall(name):
					add(Entry{Entity: owner, Detail: "skip: " + exprStr(call)})
				}
				return true
			})
		}
		for _, cg := range f.file.Comments {
			for _, c := range cg.List {
				if isNolintDirective(c.Text) {
					add(Entry{
						Entity: ownerAt(f.file, c.Pos()),
						Detail: "nolint: " + strings.TrimSpace(strings.TrimPrefix(c.Text, "//")),
					})
				}
			}
		}
	}
	ci, err := ciTestLines(t.root)
	if err != nil {
		return nil, err
	}
	return append(entries, ci...), nil
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
func ciTestLines(root string) ([]Entry, error) {
	var out []Entry
	add := func(rel string, all bool) error {
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("read %s: %w", rel, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if all || strings.Contains(line, "go test") {
				out = append(out, Entry{Dir: ".", Package: "ci", Entity: rel, Detail: line})
			}
		}
		return sc.Err()
	}
	if err := add("Makefile", false); err != nil {
		return nil, err
	}
	for _, pattern := range []string{".github/workflows/*.yml", ".github/workflows/*.yaml", ".github/actions/*/action.yml"} {
		matches, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			return nil, fmt.Errorf("glob %s: %w", pattern, err)
		}
		for _, m := range matches {
			rel, err := filepath.Rel(root, m)
			if err != nil {
				return nil, err
			}
			if err := add(filepath.ToSlash(rel), false); err != nil {
				return nil, err
			}
		}
	}
	for _, cfg := range []string{".golangci.yml", ".golangci.yaml"} {
		if err := add(cfg, true); err != nil {
			return nil, err
		}
	}
	return out, nil
}
