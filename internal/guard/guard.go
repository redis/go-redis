package guard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// Entry is one signal. It holds no file name or line number, so moving code
// between files of a package does not change it.
type Entry struct {
	Dir     string   `json:"dir"` // package directory, relative to the root
	Package string   `json:"package"`
	Entity  string   `json:"entity"`
	Detail  string   `json:"detail"`
	RawArgs []string `json:"raw_args,omitempty"`
}

// Line is the entry as one line of the report and the digest input.
func (e Entry) Line() string {
	parts := append([]string{e.Dir, e.Entity, e.Detail}, e.RawArgs...)
	return strings.Join(parts, " | ")
}

// Tier says how a layer's changes are classified.
type Tier int

const (
	// TierGated: every added or removed entry needs approval.
	TierGated Tier = iota
	// TierNamed: removals need approval; an addition needs approval when
	// an entry with the same identity exists in the base (a changed value).
	// A new identity is report only.
	TierNamed
	// TierPaired: removals need approval; an addition needs approval when
	// a removed entry has the same identity. For layers whose entries have
	// no name, such as error strings.
	TierPaired
	// TierInfo: reported, never gated.
	TierInfo
)

// Layer is one kind of signal.
type Layer struct {
	Name     string
	Tier     Tier
	Identity func(Entry) string // nil for TierGated and TierInfo
	// Extends, if set, reports whether new only adds to old. When one
	// entry of an identity is replaced by one that extends it, both halves
	// are for information.
	Extends func(old, new Entry) bool
	extract func(*tree) ([]Entry, error)
}

// Layers lists every layer in report order.
var Layers = []Layer{
	{Name: "01_constants", Tier: TierNamed, Identity: dirEntity, extract: constants},
	{Name: "02_defaults", Tier: TierNamed, Identity: defaultsIdentity, extract: defaults},
	{Name: "03_errors", Tier: TierPaired, Identity: dirEntityDetail, extract: errorCalls},
	{Name: "04_logs", Tier: TierPaired, Identity: dirEntityDetail, extract: logCalls},
	{Name: "05_error_policy", Tier: TierGated, extract: errorPolicy},
	{Name: "06_panics", Tier: TierGated, extract: panics},
	{Name: "07_deps", Tier: TierGated, extract: deps},
	{Name: "08_imports", Tier: TierGated, extract: imports},
	{Name: "09_tests", Tier: TierInfo, extract: antiCheat},
	{Name: "10_protocol", Tier: TierNamed, Identity: dirEntity, Extends: addsTokens, extract: protocol},
	{Name: "11_api", Tier: TierNamed, Identity: dirEntity, extract: api},
}

func dirEntity(e Entry) string       { return e.Dir + "\x00" + e.Entity }
func dirEntityDetail(e Entry) string { return e.Dir + "\x00" + e.Entity + "\x00" + e.Detail }

// defaultsIdentity is the function and the assigned field: "field:X" or
// "assign:opt.X".
func defaultsIdentity(e Entry) string {
	lhs, _, _ := strings.Cut(e.Detail, " = ")
	return e.Dir + "\x00" + e.Entity + "\x00" + lhs
}

// Extract scans root and returns the sorted entries of every layer, keyed by
// layer name. It only parses files; nothing under root is compiled or run.
func Extract(root string) (map[string][]Entry, error) {
	t, err := load(root)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]Entry, len(Layers))
	for _, l := range Layers {
		entries, err := l.extract(t)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", l.Name, err)
		}
		if entries == nil {
			entries = []Entry{}
		}
		sortEntries(entries)
		out[l.Name] = entries
	}
	return out, nil
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Line() < entries[j].Line() })
}

// skipDirs are directory names whose Go files are not scanned by the source
// layers. Layer 9 still scans them.
var skipDirs = map[string]bool{
	"example":   true,
	"doctests":  true,
	"e2e":       true,
	"fuzz":      true,
	"dockers":   true,
	"scripts":   true,
	"customvet": true,
}

// selfDir is the guard itself. It is not scanned by the source layers, so a
// change to the guard does not show up as a change to the library.
const selfDir = "internal/guard"

type srcFile struct {
	file *ast.File
	dir  string // relative, slash-separated; "." for the root
	test bool
}

// tree is one parsed source tree.
type tree struct {
	root string
	fset *token.FileSet
	all  []srcFile // every .go file, tests included
	src  []srcFile // non-test files outside skipDirs and selfDir
}

// load parses every .go file under root once, in walk order. Hidden
// directories, vendor, testdata and tmp are never entered.
func load(root string) (*tree, error) {
	t := &tree{root: filepath.Clean(root), fset: token.NewFileSet()}
	err := filepath.WalkDir(t.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != t.root && isNeverScanned(name) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || !d.Type().IsRegular() {
			return nil
		}
		file, err := parser.ParseFile(t.fset, path, nil, parser.SkipObjectResolution|parser.ParseComments)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		rel, err := filepath.Rel(t.root, filepath.Dir(path))
		if err != nil {
			return err
		}
		f := srcFile{file: file, dir: filepath.ToSlash(rel), test: strings.HasSuffix(name, "_test.go")}
		t.all = append(t.all, f)
		if !f.test && isSourceDir(f.dir) {
			t.src = append(t.src, f)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan %s: %w", root, err)
	}
	return t, nil
}

func isNeverScanned(name string) bool {
	return strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "tmp"
}

func isSourceDir(dir string) bool {
	if dir == selfDir || strings.HasPrefix(dir, selfDir+"/") {
		return false
	}
	for _, part := range strings.Split(dir, "/") {
		if skipDirs[part] {
			return false
		}
	}
	return true
}

type inspector func(f srcFile, add func(Entry))

// collect runs inspect on every source file and fills Dir and Package.
func collect(t *tree, inspect inspector) []Entry {
	var entries []Entry
	for _, f := range t.src {
		inspect(f, func(e Entry) {
			e.Dir = f.dir
			e.Package = f.file.Name.Name
			entries = append(entries, e)
		})
	}
	return entries
}

// Layer 1: constants, including unexported ones.
func constants(t *tree) ([]Entry, error) {
	return collect(t, func(f srcFile, add func(Entry)) {
		for _, decl := range f.file.Decls {
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
					add(Entry{Entity: name.Name, Detail: detail})
				}
			}
		}
	}), nil
}

// Layer 2: default values. Scans New* functions, functions with "default" in
// the name, and init methods on *Options types. Captures composite literal
// fields and assignments to selectors (opt.MaxRetries = 3).
func defaults(t *tree) ([]Entry, error) {
	return collect(t, func(f srcFile, add func(Entry)) {
		for _, decl := range f.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isDefaultsFunc(fn) {
				continue
			}
			entity := funcEntity(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CompositeLit:
					for _, elt := range x.Elts {
						kv, ok := elt.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						if key, ok := kv.Key.(*ast.Ident); ok {
							add(Entry{
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
							add(Entry{
								Entity: entity,
								Detail: fmt.Sprintf("assign:%s = %s", exprStr(lhs), exprStr(x.Rhs[i])),
							})
						}
					}
				}
				return true
			})
		}
	}), nil
}

// Layer 3: error construction and panics.
func errorCalls(t *tree) ([]Entry, error) {
	return collect(t, func(f srcFile, add func(Entry)) {
		ast.Inspect(f.file, func(n ast.Node) bool {
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
					add(Entry{Entity: id.Name, Detail: method, RawArgs: argStrs(call.Args)})
				}
			case *ast.Ident:
				if fun.Name == "panic" {
					add(Entry{Entity: "builtin", Detail: "panic", RawArgs: argStrs(call.Args)})
				} else if isErrorFactory(fun.Name) {
					add(Entry{Entity: "func", Detail: fun.Name, RawArgs: argStrs(call.Args)})
				}
			}
			return true
		})
	}), nil
}

// Layer 4: log calls and metric definitions.
func logCalls(t *tree) ([]Entry, error) {
	return collect(t, func(f srcFile, add func(Entry)) {
		ast.Inspect(f.file, func(n ast.Node) bool {
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
				add(Entry{Entity: recv, Detail: sel.Sel.Name, RawArgs: argStrs(call.Args)})
			}
			return true
		})
	}), nil
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
