//go:build guard

package guard

import (
	"go/ast"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Layer 6: unhandled crashes. Lists every panic, log.Fatal*, log.Panic* and
// os.Exit call that can run outside a constructor. A site is left out when:
//
//   - the enclosing function is a constructor: New*, new[A-Z]*, Must*, init;
//   - the enclosing function is only called from constructors (directly or
//     through other constructor-only functions);
//   - the enclosing function defers a recover();
//   - the package is main (generators and tools).
//
// There is no in-code exemption. A new site fails the test until a
// maintainer accepts it with -guard.update.
//
// Calls are matched by name within a package, without type information. A
// name shared with other callers makes a function not constructor-only, so
// the matching errs on the side of reporting.
func TestGuard_6_NoUnhandledPanic(t *testing.T) {
	type funcInfo struct {
		pkgDir  string
		pkg     string
		name    string // bare name, used for call matching
		entity  string
		ctor    bool
		recover bool
		sites   []string // crash calls
	}
	var funcs []*funcInfo
	callers := map[string][]*funcInfo{} // pkgDir+"."+callee name -> callers

	for _, file := range guardSources(t) {
		pos := sourcesFset.Position(file.Pos())
		pkgDir := filepath.Dir(pos.Filename)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			fi := &funcInfo{
				pkgDir: pkgDir,
				pkg:    file.Name.Name,
				name:   fn.Name.Name,
				entity: funcEntity(fn),
				ctor:   isConstructorName(fn.Name.Name) && fn.Recv == nil,
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.DeferStmt:
					if defersRecover(x) {
						fi.recover = true
					}
				case *ast.CallExpr:
					if name := calleeName(x); name != "" {
						key := pkgDir + "." + name
						callers[key] = append(callers[key], fi)
					}
					if isCrashCall(x) {
						fi.sites = append(fi.sites, exprStr(x))
					}
				}
				return true
			})
			funcs = append(funcs, fi)
		}
	}

	// Fixed point: constructor-only = has callers, and all are constructors
	// or constructor-only.
	ctorOnly := map[*funcInfo]bool{}
	for changed := true; changed; {
		changed = false
		for _, fi := range funcs {
			if fi.ctor || ctorOnly[fi] {
				continue
			}
			cs := callers[fi.pkgDir+"."+fi.name]
			if len(cs) == 0 {
				continue
			}
			all := true
			for _, c := range cs {
				if c != fi && !c.ctor && !ctorOnly[c] {
					all = false
					break
				}
			}
			if all {
				ctorOnly[fi] = true
				changed = true
			}
		}
	}

	var entries []GuardEntry
	for _, fi := range funcs {
		if fi.ctor || ctorOnly[fi] || fi.recover || fi.pkg == "main" {
			continue
		}
		for _, call := range fi.sites {
			entries = append(entries, GuardEntry{Package: fi.pkg, Entity: fi.entity, Detail: call})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entryKey(entries[i]) < entryKey(entries[j]) })
	compareGolden(t, "guard_6_panics.json", entries)
}

func isConstructorName(name string) bool {
	if name == "init" || strings.HasPrefix(name, "New") || strings.HasPrefix(name, "Must") {
		return true
	}
	return len(name) > 3 && strings.HasPrefix(name, "new") && name[3] >= 'A' && name[3] <= 'Z'
}

func isCrashCall(call *ast.CallExpr) bool {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name == "panic"
	case *ast.SelectorExpr:
		id, ok := f.X.(*ast.Ident)
		if !ok {
			return false
		}
		m := f.Sel.Name
		switch id.Name {
		case "log":
			return strings.HasPrefix(m, "Fatal") || strings.HasPrefix(m, "Panic")
		case "os":
			return m == "Exit"
		}
	}
	return false
}

// defersRecover matches "defer func() { ... recover() ... }()".
func defersRecover(d *ast.DeferStmt) bool {
	lit, ok := d.Call.Fun.(*ast.FuncLit)
	if !ok {
		return false
	}
	found := false
	ast.Inspect(lit.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "recover" {
				found = true
			}
		}
		return !found
	})
	return found
}
