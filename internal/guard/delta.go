package guard

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WriteDump writes one <layer>.json file per layer to dir.
func WriteDump(dir string, layers map[string][]Entry) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, l := range Layers {
		entries, ok := layers[l.Name]
		if !ok {
			return fmt.Errorf("layer %s missing", l.Name)
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false) // keep "->" and "&&" readable
		enc.SetIndent("", "  ")
		if err := enc.Encode(entries); err != nil {
			return fmt.Errorf("encode %s: %w", l.Name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, l.Name+".json"), buf.Bytes(), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// ReadDump reads a dump written by WriteDump. Every layer must be present,
// so a missing or deleted file is an error, not an empty layer.
func ReadDump(dir string) (map[string][]Entry, error) {
	out := make(map[string][]Entry, len(Layers))
	for _, l := range Layers {
		data, err := os.ReadFile(filepath.Join(dir, l.Name+".json"))
		if err != nil {
			return nil, err
		}
		var entries []Entry
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("parse %s: %w", l.Name, err)
		}
		if entries == nil {
			return nil, fmt.Errorf("parse %s: not a list", l.Name)
		}
		out[l.Name] = entries
	}
	return out, nil
}

// Change is one entry added (+) or removed (-) between two trees.
type Change struct {
	Layer string
	Sign  byte // '+' or '-'
	Entry Entry
	Gated bool // needs approval
}

// Line is the change as it appears in the report and the digest input:
// "<sign> <layer> | <dir> | <entity> | <detail>[ | <arg>...]".
func (c Change) Line() string {
	return string(c.Sign) + " " + c.Layer + " | " + c.Entry.Line()
}

// Delta compares two extractions and classifies every change. The result
// is sorted by line.
func Delta(base, head map[string][]Entry) []Change {
	var out []Change
	for _, l := range Layers {
		out = append(out, layerDelta(l, base[l.Name], head[l.Name])...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Line() < out[j].Line() })
	return out
}

func layerDelta(l Layer, base, head []Entry) []Change {
	// Multiset difference: duplicates count.
	count := map[string]int{}
	for _, e := range base {
		count[e.Line()]++
	}
	var added []Entry
	for _, e := range head {
		if k := e.Line(); count[k] > 0 {
			count[k]--
			continue
		}
		added = append(added, e)
	}
	var removed []Entry
	for _, e := range base {
		if k := e.Line(); count[k] > 0 {
			count[k]--
			removed = append(removed, e)
		}
	}

	// Identities an addition is compared with.
	known := map[string]bool{}
	switch l.Tier {
	case TierNamed:
		for _, e := range base {
			known[l.Identity(e)] = true
		}
	case TierPaired:
		for _, e := range removed {
			known[l.Identity(e)] = true
		}
	}

	extended := extendedIdentities(l, removed, added)
	out := make([]Change, 0, len(added)+len(removed))
	for _, e := range removed {
		gated := l.Tier != TierInfo && !extended[identity(l, e)]
		out = append(out, Change{Layer: l.Name, Sign: '-', Entry: e, Gated: gated})
	}
	for _, e := range added {
		gated := false
		switch l.Tier {
		case TierGated:
			gated = true
		case TierNamed, TierPaired:
			id := l.Identity(e)
			gated = known[id] && !extended[id]
		}
		out = append(out, Change{Layer: l.Name, Sign: '+', Entry: e, Gated: gated})
	}
	return out
}

func identity(l Layer, e Entry) string {
	if l.Identity == nil {
		return ""
	}
	return l.Identity(e)
}

// extendedIdentities returns the identities with exactly one removed and
// one added entry where the added one extends the removed one.
func extendedIdentities(l Layer, removed, added []Entry) map[string]bool {
	if l.Extends == nil {
		return nil
	}
	type pair struct{ old, new []Entry }
	byID := map[string]*pair{}
	get := func(id string) *pair {
		if byID[id] == nil {
			byID[id] = &pair{}
		}
		return byID[id]
	}
	for _, e := range removed {
		p := get(l.Identity(e))
		p.old = append(p.old, e)
	}
	for _, e := range added {
		p := get(l.Identity(e))
		p.new = append(p.new, e)
	}
	out := map[string]bool{}
	for id, p := range byID {
		if len(p.old) == 1 && len(p.new) == 1 && l.Extends(p.old[0], p.new[0]) {
			out[id] = true
		}
	}
	return out
}

// Gated returns the changes that need approval.
func Gated(changes []Change) []Change {
	var out []Change
	for _, c := range changes {
		if c.Gated {
			out = append(out, c)
		}
	}
	return out
}

// Digest identifies a gated delta: the first 12 hex characters of the
// SHA-256 of the sorted gated lines, each followed by "\n". It is "" when
// nothing needs approval. Report-only changes do not affect it.
func Digest(changes []Change) string {
	lines := make([]string, 0, len(changes))
	for _, c := range changes {
		if c.Gated {
			lines = append(lines, c.Line())
		}
	}
	if len(lines) == 0 {
		return ""
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n") + "\n"))
	return hex.EncodeToString(sum[:])[:12]
}

// WriteReport prints the delta as plain text.
func WriteReport(w io.Writer, baseLabel, headLabel string, changes []Change) error {
	var b strings.Builder
	fmt.Fprintf(&b, "guard delta: %s -> %s\n", baseLabel, headLabel)
	section := func(title string, gated bool) {
		var lines []string
		for _, c := range changes {
			if c.Gated == gated {
				lines = append(lines, c.Line())
			}
		}
		fmt.Fprintf(&b, "\n%s: %d\n", title, len(lines))
		for _, l := range lines {
			b.WriteString("  " + l + "\n")
		}
	}
	section("needs approval", true)
	section("for information", false)
	if d := Digest(changes); d != "" {
		fmt.Fprintf(&b, "\ndigest: %s\n", d)
	} else {
		b.WriteString("\ndigest: none (nothing needs approval)\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}
