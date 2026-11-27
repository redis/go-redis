package guard

import (
	"fmt"
	"strconv"
)

// Layer 8: imports. Records the set of import paths of each package
// directory (non-test files): a new third-party package, root importing a
// telemetry library, internal/proto importing internal/pool, a new use of
// unsafe or os/exec.
func imports(t *tree) ([]Entry, error) {
	seen := map[[2]string]bool{}
	var entries []Entry
	for _, f := range t.src {
		for _, imp := range f.file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return nil, fmt.Errorf("bad import %s in %s", imp.Path.Value, f.dir)
			}
			k := [2]string{f.dir, path}
			if seen[k] {
				continue
			}
			seen[k] = true
			entries = append(entries, Entry{Dir: f.dir, Package: f.file.Name.Name, Entity: f.dir, Detail: path})
		}
	}
	return entries, nil
}
