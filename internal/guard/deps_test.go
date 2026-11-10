//go:build guard

package guard

import (
	"bufio"
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Layer 7: dependencies. Records every go.mod in the repository (all
// modules, including examples and tests): module path, go and toolchain
// directives, require, replace and exclude lines. Adding, removing or
// bumping a dependency, or raising the go directive, fails until accepted
// with -guard.update.
func TestGuard_7_Deps(t *testing.T) {
	var entries []GuardEntry
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != repoRoot && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "tmp") {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != "go.mod" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		entries = append(entries, parseGoMod(data)...)
		return nil
	})
	if err != nil {
		t.Fatalf("scan go.mod: %v", err)
	}
	sortEntries(entries)
	compareGolden(t, "guard_7_deps.json", entries)
}

// parseGoMod extracts directives from a go.mod without x/mod. Entity is the
// module path; Detail is "<directive> <args>", with "// indirect" kept.
func parseGoMod(data []byte) []GuardEntry {
	var out []GuardEntry
	module := ""
	block := ""
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		indirect := strings.Contains(line, "// indirect")
		if i := strings.Index(line, "//"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line == "" {
			continue
		}
		if block != "" {
			if line == ")" {
				block = ""
				continue
			}
			out = append(out, depEntry(module, block, line, indirect))
			continue
		}
		verb, rest, _ := strings.Cut(line, " ")
		rest = strings.TrimSpace(rest)
		switch verb {
		case "module":
			module = strings.Trim(rest, `"`)
			continue
		case "require", "replace", "exclude", "retract", "tool", "ignore", "godebug":
			if rest == "(" {
				block = verb
				continue
			}
		}
		out = append(out, depEntry(module, verb, rest, indirect))
	}
	return out
}

func depEntry(module, verb, args string, indirect bool) GuardEntry {
	detail := verb + " " + strings.Join(strings.Fields(args), " ")
	if indirect {
		detail += " // indirect"
	}
	return GuardEntry{Package: "go.mod", Entity: module, Detail: detail}
}
