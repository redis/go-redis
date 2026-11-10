//go:build guard

package guard

import (
	"path/filepath"
	"strconv"
	"testing"
)

// Layer 8: imports. Records the set of import paths of each package
// directory (non-test files). A new edge (a new third-party package, root
// importing a telemetry library, internal/proto importing internal/pool, a
// new use of unsafe or os/exec) fails until accepted with -guard.update.
func TestGuard_8_Imports(t *testing.T) {
	seen := map[[2]string]bool{}
	var entries []GuardEntry
	for _, file := range guardSources(t) {
		dir, _ := filepath.Rel(repoRoot, filepath.Dir(sourcesFset.Position(file.Pos()).Filename))
		dir = filepath.ToSlash(dir)
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("bad import %s in %s", imp.Path.Value, dir)
			}
			k := [2]string{dir, path}
			if seen[k] {
				continue
			}
			seen[k] = true
			entries = append(entries, GuardEntry{Package: file.Name.Name, Entity: dir, Detail: path})
		}
	}
	sortEntries(entries)
	compareGolden(t, "guard_8_imports.json", entries)
}
