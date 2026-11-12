package guard

import (
	"bufio"
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Layer 7: dependencies. Records every go.mod in the repository (all
// modules, including examples and tests): module path, go and toolchain
// directives, require, replace and exclude lines.
func deps(t *tree) ([]Entry, error) {
	var entries []Entry
	err := filepath.WalkDir(t.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != t.root && isNeverScanned(name) {
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
		rel, err := filepath.Rel(t.root, filepath.Dir(path))
		if err != nil {
			return err
		}
		for _, e := range parseGoMod(data) {
			e.Dir = filepath.ToSlash(rel)
			entries = append(entries, e)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan go.mod: %w", err)
	}
	return entries, nil
}

// parseGoMod extracts directives from a go.mod without x/mod. Entity is the
// module path; Detail is "<directive> <args>", with "// indirect" kept.
func parseGoMod(data []byte) []Entry {
	var out []Entry
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

func depEntry(module, verb, args string, indirect bool) Entry {
	detail := verb + " " + strings.Join(strings.Fields(args), " ")
	if indirect {
		detail += " // indirect"
	}
	return Entry{Package: "go.mod", Entity: module, Detail: detail}
}
