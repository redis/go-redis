//go:build guard

package guard

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

var guardUpdate = flag.Bool("guard.update", false, "regenerate behavioral guard golden files")

// repoRoot is the scan root, relative to this package directory. CI sets
// GUARD_ROOT to scan a PR checkout with the base branch's guard code.
var repoRoot = guardRoot()

func guardRoot() string {
	if r := os.Getenv("GUARD_ROOT"); r != "" {
		return filepath.Clean(r)
	}
	return "../.."
}

// GuardEntry is one signal. It holds no file name or line number, so moving
// code between files does not change the snapshot.
type GuardEntry struct {
	Package string   `json:"package"`
	Entity  string   `json:"entity"`
	Detail  string   `json:"detail"`
	RawArgs []string `json:"raw_args,omitempty"`
}

// guardSkipDirs are directory names that are never scanned.
var guardSkipDirs = map[string]bool{
	"vendor":    true,
	"testdata":  true,
	"example":   true,
	"doctests":  true,
	"e2e":       true,
	"fuzz":      true,
	"tmp":       true,
	"dockers":   true,
	"scripts":   true,
	"customvet": true,
}

type guardInspector func(file *ast.File, add func(GuardEntry))

// Layer 1: constants, including unexported ones.
func TestGuard_1_Constants(t *testing.T) {
	runGuard(t, "guard_1_constants.json", func(file *ast.File, add func(GuardEntry)) {
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			// A spec without values repeats the previous spec's type and values.
			var lastType ast.Expr
			var lastValues []ast.Expr
			for idx, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				implicit := len(vs.Values) == 0
				if !implicit {
					lastType, lastValues = vs.Type, vs.Values
				}
				for i, name := range vs.Names {
					if name.Name == "_" {
						continue
					}
					val := "?"
					if i < len(lastValues) {
						val = exprStr(lastValues[i])
					}
					detail := val
					if lastType != nil {
						detail = exprStr(lastType) + " = " + val
					}
					// iota values depend on position in the block.
					if implicit || strings.Contains(val, "iota") {
						detail += fmt.Sprintf(" [iota %d]", idx)
					}
					add(GuardEntry{Entity: name.Name, Detail: detail})
				}
			}
		}
	})
}

// Layer 2: default values. Scans New* functions, functions with "default" in
// the name, and init methods on *Options types. Captures composite literal
// fields and assignments to selectors (opt.MaxRetries = 3).
func TestGuard_2_Defaults(t *testing.T) {
	runGuard(t, "guard_2_defaults.json", func(file *ast.File, add func(GuardEntry)) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isDefaultsFunc(fn) {
				continue
			}
			entity := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) > 0 {
				entity = recvTypeName(fn.Recv.List[0].Type) + "." + entity
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CompositeLit:
					for _, elt := range x.Elts {
						kv, ok := elt.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						if key, ok := kv.Key.(*ast.Ident); ok {
							add(GuardEntry{
								Entity: entity,
								Detail: fmt.Sprintf("field:%s = %s", key.Name, exprStr(kv.Value)),
							})
						}
					}
				case *ast.AssignStmt:
					if len(x.Lhs) != len(x.Rhs) {
						return true
					}
					for i, lhs := range x.Lhs {
						if _, ok := lhs.(*ast.SelectorExpr); ok {
							add(GuardEntry{
								Entity: entity,
								Detail: fmt.Sprintf("assign:%s = %s", exprStr(lhs), exprStr(x.Rhs[i])),
							})
						}
					}
				}
				return true
			})
		}
	})
}

// Layer 3: error construction and panics.
func TestGuard_3_Errors(t *testing.T) {
	runGuard(t, "guard_3_errors.json", func(file *ast.File, add func(GuardEntry)) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				id, ok := fun.X.(*ast.Ident)
				if !ok {
					return true
				}
				method := fun.Sel.Name
				if (id.Name == "errors" && method == "New") ||
					(id.Name == "fmt" && method == "Errorf") ||
					(id.Name == "internal" && isErrorFactory(method)) {
					add(GuardEntry{Entity: id.Name, Detail: method, RawArgs: argStrs(call.Args)})
				}
			case *ast.Ident:
				if fun.Name == "panic" {
					add(GuardEntry{Entity: "builtin", Detail: "panic", RawArgs: argStrs(call.Args)})
				} else if isErrorFactory(fun.Name) {
					add(GuardEntry{Entity: "func", Detail: fun.Name, RawArgs: argStrs(call.Args)})
				}
			}
			return true
		})
	})
}

// Layer 4: log calls and metric definitions.
func TestGuard_4_Logs(t *testing.T) {
	runGuard(t, "guard_4_logs.json", func(file *ast.File, add func(GuardEntry)) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv := exprStr(sel.X)
			if isLogCall(recv, sel.Sel.Name) || isMetricCall(recv) {
				add(GuardEntry{Entity: recv, Detail: sel.Sel.Name, RawArgs: argStrs(call.Args)})
			}
			return true
		})
	})
}

var (
	sourcesOnce sync.Once
	sources     []*ast.File
	sourcesFset = token.NewFileSet()
	sourcesErr  error
)

// guardSources parses the scanned files once per test binary, in walk order.
func guardSources(t *testing.T) []*ast.File {
	t.Helper()
	sourcesOnce.Do(func() {
		fset := sourcesFset
		sourcesErr = filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := d.Name()
			if d.IsDir() {
				if path != repoRoot && (strings.HasPrefix(name, ".") || guardSkipDirs[name]) {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution|parser.ParseComments)
			if err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			sources = append(sources, file)
			return nil
		})
	})
	if sourcesErr != nil {
		t.Fatalf("scan: %v", sourcesErr)
	}
	return sources
}

func collect(t *testing.T, inspect guardInspector) []GuardEntry {
	t.Helper()
	var entries []GuardEntry
	for _, file := range guardSources(t) {
		pkg := file.Name.Name
		inspect(file, func(e GuardEntry) {
			e.Package = pkg
			entries = append(entries, e)
		})
	}
	return entries
}

// runGuard sorts all entries, so the snapshot does not depend on order.
func runGuard(t *testing.T, snapshot string, inspect guardInspector) {
	t.Helper()
	entries := collect(t, inspect)
	sortEntries(entries)
	compareGolden(t, snapshot, entries)
}

func sortEntries(entries []GuardEntry) {
	sort.Slice(entries, func(i, j int) bool { return entryKey(entries[i]) < entryKey(entries[j]) })
}

// runGuardOrdered sorts by package and entity only and keeps source order
// inside an entity. Use it where order has meaning, such as the order of
// checks in a function.
func runGuardOrdered(t *testing.T, snapshot string, inspect guardInspector) {
	t.Helper()
	entries := collect(t, inspect)
	sort.SliceStable(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.Package != b.Package {
			return a.Package < b.Package
		}
		return a.Entity < b.Entity
	})
	compareGolden(t, snapshot, entries)
}

// compareGoldenAdditive is compareGolden, except that entries missing from
// the golden pass when allowAdd accepts them. Removed entries and other
// additions fail. With -guard.update the golden is rewritten as usual.
func compareGoldenAdditive(t *testing.T, snapshot string, entries []GuardEntry, allowAdd func(GuardEntry) bool) {
	t.Helper()
	golden := filepath.Join("testdata", snapshot)
	if *guardUpdate {
		compareGolden(t, snapshot, entries)
		return
	}
	data, err := os.ReadFile(golden)
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("%s is missing; create it with: go test ./internal/guard -guard.update", golden)
	}
	if err != nil {
		t.Fatalf("read %s: %v", golden, err)
	}
	var base []GuardEntry
	if err := json.Unmarshal(data, &base); err != nil {
		t.Fatalf("parse %s: %v", golden, err)
	}

	// Multiset difference: duplicates count.
	count := map[string]int{}
	for _, e := range base {
		count[entryKey(e)]++
	}
	var added []GuardEntry
	for _, e := range entries {
		k := entryKey(e)
		if count[k] > 0 {
			count[k]--
			continue
		}
		added = append(added, e)
	}
	var removed, blocked []string
	for _, e := range base {
		if k := entryKey(e); count[k] > 0 {
			count[k]--
			removed = append(removed, fmtEntry(e))
		}
	}
	for _, e := range added {
		if !allowAdd(e) {
			blocked = append(blocked, fmtEntry(e))
		}
	}
	if len(removed) == 0 && len(blocked) == 0 {
		if len(added) > 0 {
			t.Logf("%s: %d allowed addition(s) not yet in the golden", snapshot, len(added))
		}
		return
	}
	var b strings.Builder
	for _, s := range removed {
		b.WriteString("\033[31m- " + s + "\033[0m\n")
	}
	for _, s := range blocked {
		b.WriteString("\033[32m+ " + s + "\033[0m\n")
	}
	t.Errorf("\033[31m[BEHAVIORAL REGRESSION DETECTED]: %s changed\033[0m\n%s", snapshot, b.String())
	t.Fatalf("%s", updateHint)
}

func fmtEntry(e GuardEntry) string {
	s := e.Package + " | " + e.Entity + " | " + e.Detail
	if len(e.RawArgs) > 0 {
		s += " | " + strings.Join(e.RawArgs, ", ")
	}
	return s
}

// updateHint is printed when a golden does not match.
const updateHint = "if the change is intended, a maintainer adds the guard-update label to the PR; " +
	"CI regenerates the goldens. Golden commits not made by the guard bot are rejected by guard-provenance."

func compareGolden(t *testing.T, snapshot string, entries []GuardEntry) {
	t.Helper()
	// No HTML escaping, so "->" and "&&" stay readable in diffs.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(entries); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	current := buf.Bytes()

	golden := filepath.Join("testdata", snapshot)
	if *guardUpdate {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(golden, current, 0o644); err != nil {
			t.Fatalf("write %s: %v", golden, err)
		}
		t.Logf("updated %s (%d entries)", golden, len(entries))
		return
	}

	base, err := os.ReadFile(golden)
	if errors.Is(err, fs.ErrNotExist) {
		// Never create goldens implicitly: a deleted golden must not pass.
		t.Fatalf("%s is missing; create it with: go test ./internal/guard -guard.update", golden)
	}
	if err != nil {
		t.Fatalf("read %s: %v", golden, err)
	}
	if bytes.Equal(base, current) {
		return
	}

	tmp := filepath.Join(t.TempDir(), snapshot)
	if err := os.WriteFile(tmp, current, 0o644); err != nil {
		t.Fatalf("write %s: %v", tmp, err)
	}
	out, _ := exec.Command("diff", "-u", golden, tmp).CombinedOutput()
	t.Errorf("\033[31m[BEHAVIORAL REGRESSION DETECTED]: %s changed\033[0m\n%s", snapshot, colorizeDiff(string(out)))
	t.Fatalf("%s", updateHint)
}

func entryKey(e GuardEntry) string {
	return strings.Join([]string{e.Package, e.Entity, e.Detail, strings.Join(e.RawArgs, "\x00")}, "\x01")
}

func exprStr(e ast.Expr) string {
	return types.ExprString(e)
}

func argStrs(args []ast.Expr) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, exprStr(a))
	}
	return out
}

func recvTypeName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.StarExpr:
		return "*" + recvTypeName(x.X)
	case *ast.IndexExpr:
		return recvTypeName(x.X)
	case *ast.IndexListExpr:
		return recvTypeName(x.X)
	case *ast.Ident:
		return x.Name
	}
	return exprStr(e)
}

func isDefaultsFunc(fn *ast.FuncDecl) bool {
	name := fn.Name.Name
	if strings.HasPrefix(name, "New") || strings.Contains(strings.ToLower(name), "default") {
		return true
	}
	if name == "init" && fn.Recv != nil && len(fn.Recv.List) > 0 {
		return strings.HasSuffix(recvTypeName(fn.Recv.List[0].Type), "Options")
	}
	return false
}

// isErrorFactory matches constructor-style error helpers such as
// newRedisError, NewError or newErr.
func isErrorFactory(name string) bool {
	n := strings.ToLower(name)
	return strings.HasPrefix(n, "new") && strings.Contains(n, "err") ||
		n == "wrap" || n == "wrapf"
}

func isLogCall(recv, method string) bool {
	r := strings.ToLower(recv)
	root := r
	if i := strings.IndexByte(root, '.'); i >= 0 {
		root = root[:i]
	}
	if root != "log" && root != "slog" && root != "zap" && !strings.Contains(r, "logger") {
		return false
	}
	m := strings.ToLower(method)
	for _, s := range []string{"print", "log", "info", "warn", "err", "debug", "fatal", "panic"} {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}

func isMetricCall(recv string) bool {
	r := strings.ToLower(recv)
	return r == "prometheus" || r == "metrics" ||
		strings.HasSuffix(r, "meter") || strings.HasSuffix(r, ".meter")
}

func colorizeDiff(diff string) string {
	lines := strings.Split(diff, "\n")
	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "+++"), strings.HasPrefix(line, "---"):
		case strings.HasPrefix(line, "+"):
			lines[i] = "\033[32m" + line + "\033[0m"
		case strings.HasPrefix(line, "-"):
			lines[i] = "\033[31m" + line + "\033[0m"
		case strings.HasPrefix(line, "@@"):
			lines[i] = "\033[36m" + line + "\033[0m"
		}
	}
	return strings.Join(lines, "\n")
}
