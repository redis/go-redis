package guard

import (
	"go/ast"
	"go/token"
	"strings"
)

// Layer 11: exported API, read from the syntax tree (no type checking, so
// it is safe to run on untrusted code). Covers packages outside internal/
// and main:
//
//	func F                func(T1, T2) R
//	method *T.M           func(T1) R              (methods of exported or promoted types)
//	method I.M            func(T1) R              (interface methods)
//	embed I.X             X                       (embedded interfaces)
//	type T                struct | interface | = Alias | <underlying type>
//	field T.F             <type>                  (embedded: "embedded X")
//	var V                 <type> or "= <value>"
//
// Methods of an unexported type count too when the type is embedded,
// directly or through other unexported types, in an exported struct of the
// same package: they are promoted (cmdable embedded in Client gives
// Client.Get, Client.DoRaw, ...).
//
// Signatures list types only, so renaming a parameter is not a change. Each
// method, field and embed is its own identity, so adding one is report only
// and changing or removing one needs approval.
func api(t *tree) ([]Entry, error) {
	promoted := promotedTypes(t)
	return collect(t, func(f srcFile, add func(Entry)) {
		if !isAPIDir(f.dir) || f.file.Name.Name == "main" {
			return
		}
		for _, decl := range f.file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				if d.Recv == nil {
					add(Entry{Entity: "func " + d.Name.Name, Detail: funcSig(d.Type)})
					continue
				}
				if len(d.Recv.List) == 0 {
					continue
				}
				recv := strings.TrimPrefix(recvTypeName(d.Recv.List[0].Type), "*")
				if !ast.IsExported(recv) && !promoted[f.dir][recv] {
					continue
				}
				add(Entry{Entity: "method " + funcEntity(d), Detail: funcSig(d.Type)})
			case *ast.GenDecl:
				apiGenDecl(d, add)
			}
		}
	}), nil
}

// promotedTypes returns, per directory, the unexported types whose methods
// are promoted into an exported struct.
func promotedTypes(t *tree) map[string]map[string]bool {
	embeds := map[string]map[string][]string{} // dir -> struct type -> embedded local types
	for _, f := range t.src {
		for _, decl := range f.file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts := spec.(*ast.TypeSpec)
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, fld := range st.Fields.List {
					if len(fld.Names) > 0 {
						continue
					}
					name := strings.TrimPrefix(recvTypeName(fld.Type), "*")
					if strings.Contains(name, ".") || ast.IsExported(name) {
						continue
					}
					if embeds[f.dir] == nil {
						embeds[f.dir] = map[string][]string{}
					}
					embeds[f.dir][ts.Name.Name] = append(embeds[f.dir][ts.Name.Name], name)
				}
			}
		}
	}
	out := map[string]map[string]bool{}
	for dir, byType := range embeds {
		seen := map[string]bool{}
		var queue []string
		for typ, embedded := range byType {
			if ast.IsExported(typ) {
				queue = append(queue, embedded...)
			}
		}
		for len(queue) > 0 {
			name := queue[0]
			queue = queue[1:]
			if seen[name] {
				continue
			}
			seen[name] = true
			queue = append(queue, byType[name]...)
		}
		out[dir] = seen
	}
	return out
}

func apiGenDecl(d *ast.GenDecl, add func(Entry)) {
	switch d.Tok {
	case token.TYPE:
		for _, spec := range d.Specs {
			ts := spec.(*ast.TypeSpec)
			if !ts.Name.IsExported() {
				continue
			}
			apiType(ts, add)
		}
	case token.VAR:
		for _, spec := range d.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if !name.IsExported() {
					continue
				}
				detail := "?"
				switch {
				case vs.Type != nil:
					detail = exprStr(vs.Type)
				case i < len(vs.Values):
					detail = "= " + exprStr(vs.Values[i])
				}
				add(Entry{Entity: "var " + name.Name, Detail: detail})
			}
		}
	}
}

func apiType(ts *ast.TypeSpec, add func(Entry)) {
	name := ts.Name.Name
	entity := "type " + name
	tparams := ""
	if ts.TypeParams != nil {
		tparams = "[" + fieldTypes(ts.TypeParams) + "] "
	}
	if ts.Assign.IsValid() {
		add(Entry{Entity: entity, Detail: tparams + "= " + exprStr(ts.Type)})
		return
	}
	switch x := ts.Type.(type) {
	case *ast.StructType:
		add(Entry{Entity: entity, Detail: tparams + "struct"})
		for _, fld := range x.Fields.List {
			typ := typeStr(fld.Type)
			if len(fld.Names) == 0 {
				emb := strings.TrimPrefix(recvTypeName(fld.Type), "*")
				if i := strings.LastIndexByte(emb, '.'); i >= 0 {
					emb = emb[i+1:]
				}
				if ast.IsExported(emb) {
					add(Entry{Entity: "field " + name + "." + emb, Detail: "embedded " + typ})
				}
				continue
			}
			for _, fn := range fld.Names {
				if fn.IsExported() {
					add(Entry{Entity: "field " + name + "." + fn.Name, Detail: typ})
				}
			}
		}
	case *ast.InterfaceType:
		add(Entry{Entity: entity, Detail: tparams + "interface"})
		for _, m := range x.Methods.List {
			if ft, ok := m.Type.(*ast.FuncType); ok && len(m.Names) > 0 {
				for _, mn := range m.Names {
					add(Entry{Entity: "method " + name + "." + mn.Name, Detail: funcSig(ft)})
				}
				continue
			}
			// Embedded interface or type constraint.
			emb := typeStr(m.Type)
			add(Entry{Entity: "embed " + name + "." + emb, Detail: emb})
		}
	default:
		add(Entry{Entity: entity, Detail: tparams + typeStr(ts.Type)})
	}
}

// isAPIDir reports whether a package in dir can be imported by users.
func isAPIDir(dir string) bool {
	for _, part := range strings.Split(dir, "/") {
		if part == "internal" {
			return false
		}
	}
	return true
}

// funcSig renders a function type with types only: "func(string, int) error".
func funcSig(ft *ast.FuncType) string {
	var b strings.Builder
	b.WriteString("func")
	if ft.TypeParams != nil {
		b.WriteString("[" + fieldTypes(ft.TypeParams) + "]")
	}
	b.WriteString("(" + fieldTypes(ft.Params) + ")")
	if ft.Results != nil && len(ft.Results.List) > 0 {
		res := fieldTypes(ft.Results)
		if len(ft.Results.List) == 1 && len(ft.Results.List[0].Names) <= 1 {
			b.WriteString(" " + res)
		} else {
			b.WriteString(" (" + res + ")")
		}
	}
	return b.String()
}

// fieldTypes lists the types of a field list, one per name.
func fieldTypes(fl *ast.FieldList) string {
	if fl == nil {
		return ""
	}
	var parts []string
	for _, fld := range fl.List {
		n := len(fld.Names)
		if n == 0 {
			n = 1
		}
		typ := typeStr(fld.Type)
		for i := 0; i < n; i++ {
			parts = append(parts, typ)
		}
	}
	return strings.Join(parts, ", ")
}

// typeStr renders a type expression with function types inside it
// written without parameter names.
func typeStr(e ast.Expr) string {
	return exprStr(stripParamNames(e))
}

// stripParamNames returns a copy of e's function types without parameter
// names. It copies only the nodes it changes.
func stripParamNames(e ast.Expr) ast.Expr {
	switch x := e.(type) {
	case *ast.FuncType:
		return &ast.FuncType{TypeParams: x.TypeParams, Params: unnamed(x.Params), Results: unnamed(x.Results)}
	case *ast.StarExpr:
		return &ast.StarExpr{X: stripParamNames(x.X)}
	case *ast.ArrayType:
		return &ast.ArrayType{Len: x.Len, Elt: stripParamNames(x.Elt)}
	case *ast.MapType:
		return &ast.MapType{Key: stripParamNames(x.Key), Value: stripParamNames(x.Value)}
	case *ast.ChanType:
		return &ast.ChanType{Dir: x.Dir, Value: stripParamNames(x.Value)}
	case *ast.Ellipsis:
		return &ast.Ellipsis{Elt: stripParamNames(x.Elt)}
	}
	return e
}

func unnamed(fl *ast.FieldList) *ast.FieldList {
	if fl == nil {
		return nil
	}
	out := &ast.FieldList{}
	for _, fld := range fl.List {
		n := len(fld.Names)
		if n == 0 {
			n = 1
		}
		for i := 0; i < n; i++ {
			out.List = append(out.List, &ast.Field{Type: stripParamNames(fld.Type)})
		}
	}
	return out
}
