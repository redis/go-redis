//go:build guard

package guard

import (
	"go/ast"
	"strings"
	"testing"
)

// Layer 5: error policy. Records, in source order:
//
//   - decide:  each return of an error predicate (a func that takes an error
//     and returns a bool, e.g. shouldRetry, isBadConn, isMovedError), with the
//     conditions on the path to it. A changed, removed or reordered check
//     shows up as a diff.
//   - check:   each call of an error predicate outside predicates, so a
//     dropped shouldRetry or isBadConn call site shows up.
//   - dispose: each connection removal or close, with its conditions.
func TestGuard_5_ErrorPolicy(t *testing.T) {
	preds := errorPredicateNames(guardSources(t))
	runGuardOrdered(t, "guard_5_error_policy.json", func(file *ast.File, add func(GuardEntry)) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			entity := funcEntity(fn)
			isPred := isErrorPredicate(fn)
			walkPaths(fn.Body.List, nil, func(n ast.Node, conds []string) {
				switch x := n.(type) {
				case *ast.ReturnStmt:
					if isPred {
						add(GuardEntry{Entity: entity, Detail: "decide: " + pathStr(conds) + " -> " + exprList(x.Results)})
					}
				case *ast.CallExpr:
					name := calleeName(x)
					switch {
					case isConnDisposal(name, len(x.Args)):
						add(GuardEntry{Entity: entity, Detail: "dispose: " + exprStr(x) + " when " + pathStr(conds)})
					case !isPred && preds[name]:
						add(GuardEntry{Entity: entity, Detail: "check: " + exprStr(x)})
					}
				}
			})
		}
	})
}

// isErrorPredicate reports whether fn takes an error and returns at least
// one bool, with any other results of basic types (isMovedError returns
// moved, ask bool and addr string).
func isErrorPredicate(fn *ast.FuncDecl) bool {
	ft := fn.Type
	if ft.Results == nil {
		return false
	}
	hasBool := false
	for _, r := range ft.Results.List {
		id, ok := r.Type.(*ast.Ident)
		if !ok {
			return false
		}
		switch id.Name {
		case "bool":
			hasBool = true
		case "string", "int", "int64":
		default:
			return false
		}
	}
	if !hasBool {
		return false
	}
	for _, p := range ft.Params.List {
		if id, ok := p.Type.(*ast.Ident); ok && id.Name == "error" {
			return true
		}
	}
	return false
}

// errorPredicateNames returns the names of all error predicates. Calls are
// matched by name only (no type info), so proto.IsLoadingError and
// redis.IsLoadingError both match "IsLoadingError".
func errorPredicateNames(files []*ast.File) map[string]bool {
	names := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && isErrorPredicate(fn) {
				names[fn.Name.Name] = true
			}
		}
	}
	return names
}

// isConnDisposal matches pool calls that remove or close a connection.
// Remove and RemoveWithoutTurn take (ctx, cn, err); the arity check keeps
// unrelated Remove methods (lists, maps) out.
func isConnDisposal(name string, nargs int) bool {
	switch name {
	case "Remove", "RemoveWithoutTurn":
		return nargs == 3
	case "CloseConn", "closeConn", "removeConn", "removeConnInternal", "removeConnWithLock":
		return true
	}
	return false
}

func funcEntity(fn *ast.FuncDecl) string {
	if fn.Recv != nil && len(fn.Recv.List) > 0 {
		return recvTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
	}
	return fn.Name.Name
}

func calleeName(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

func pathStr(conds []string) string {
	if len(conds) == 0 {
		return "always"
	}
	return strings.Join(conds, " && ")
}

func exprList(es []ast.Expr) string {
	if len(es) == 0 {
		return "(bare)"
	}
	return strings.Join(argStrs(es), ", ")
}

// walkPaths visits statements and calls in source order, passing the
// conditions that must hold to reach each one. Else branches get the
// negated condition; switch cases get "tag == v"; loop bodies get "loop";
// function literals get "func literal".
func walkPaths(stmts []ast.Stmt, conds []string, visit func(ast.Node, []string)) {
	for _, s := range stmts {
		walkStmt(s, conds, visit)
	}
}

func walkStmt(s ast.Stmt, conds []string, visit func(ast.Node, []string)) {
	with := func(c string) []string {
		out := make([]string, len(conds), len(conds)+1)
		copy(out, conds)
		return append(out, c)
	}
	switch x := s.(type) {
	case *ast.BlockStmt:
		walkPaths(x.List, conds, visit)
	case *ast.IfStmt:
		if x.Init != nil {
			walkStmt(x.Init, conds, visit)
		}
		walkCalls(x.Cond, conds, visit)
		c := exprStr(x.Cond)
		walkPaths(x.Body.List, with(c), visit)
		if x.Else != nil {
			walkStmt(x.Else, with("!("+c+")"), visit)
		}
	case *ast.SwitchStmt:
		if x.Init != nil {
			walkStmt(x.Init, conds, visit)
		}
		if x.Tag != nil {
			walkCalls(x.Tag, conds, visit)
		}
		for _, cs := range x.Body.List {
			cc := cs.(*ast.CaseClause)
			walkPaths(cc.Body, with(caseLabel(x.Tag, cc.List)), visit)
		}
	case *ast.TypeSwitchStmt:
		for _, cs := range x.Body.List {
			cc := cs.(*ast.CaseClause)
			label := "default"
			if cc.List != nil {
				label = "type " + strings.Join(argStrs(cc.List), " | ")
			}
			walkPaths(cc.Body, with(label), visit)
		}
	case *ast.SelectStmt:
		for _, cs := range x.Body.List {
			cc := cs.(*ast.CommClause)
			walkPaths(cc.Body, with("select"), visit)
		}
	case *ast.ForStmt:
		walkPaths(x.Body.List, with("loop"), visit)
	case *ast.RangeStmt:
		walkPaths(x.Body.List, with("loop"), visit)
	case *ast.LabeledStmt:
		walkStmt(x.Stmt, conds, visit)
	default:
		visit(s, conds)
		walkCalls(s, conds, visit)
	}
}

// walkCalls visits the calls inside n. Function literal bodies are walked
// as statements so their conditions are tracked.
func walkCalls(n ast.Node, conds []string, visit func(ast.Node, []string)) {
	ast.Inspect(n, func(m ast.Node) bool {
		switch x := m.(type) {
		case *ast.FuncLit:
			lit := make([]string, len(conds), len(conds)+1)
			copy(lit, conds)
			walkPaths(x.Body.List, append(lit, "func literal"), visit)
			return false
		case *ast.CallExpr:
			visit(x, conds)
		}
		return true
	})
}

func caseLabel(tag ast.Expr, list []ast.Expr) string {
	if list == nil {
		return "default"
	}
	vals := argStrs(list)
	if tag == nil {
		return strings.Join(vals, " || ")
	}
	return exprStr(tag) + " == " + strings.Join(vals, " | ")
}
