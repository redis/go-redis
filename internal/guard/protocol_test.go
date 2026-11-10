//go:build guard

package guard

import (
	"go/ast"
	"go/token"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// redisToken matches literals that look like command names or flags:
// "set", "EX", "FT.SEARCH", "WITHSCORES", "CLIENT", "NO-EVICT".
var redisToken = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]{0,39}$`)

// Layer 10: wire protocol. For each function in package redis that builds a
// command (calls a New*Cmd constructor), records the command tokens it can
// send, in source order: string literals that look like Redis words. A
// renamed command, a misspelled or reordered flag, or a dropped option is a
// one-line diff that neither the compiler nor apidiff sees.
func TestGuard_10_Protocol(t *testing.T) {
	runGuard(t, "guard_10_protocol.json", func(file *ast.File, add func(GuardEntry)) {
		if file.Name.Name != "redis" {
			return
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !buildsCmd(fn.Body) {
				continue
			}
			var tokens []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				s, err := strconv.Unquote(lit.Value)
				if err == nil && redisToken.MatchString(s) {
					tokens = append(tokens, s)
				}
				return true
			})
			if len(tokens) > 0 {
				add(GuardEntry{Entity: funcEntity(fn), Detail: strings.Join(tokens, " ")})
			}
		}
	})
}

// buildsCmd reports whether body calls a New<Something>Cmd constructor.
func buildsCmd(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			name := calleeName(call)
			if strings.HasPrefix(name, "New") && strings.HasSuffix(name, "Cmd") && len(name) > len("NewCmd") {
				found = true
			}
		}
		return !found
	})
	return found
}
