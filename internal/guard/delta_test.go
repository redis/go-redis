//go:build guard

package guard

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// layersOf builds an extraction with every layer empty except the given ones.
func layersOf(m map[string][]Entry) map[string][]Entry {
	out := map[string][]Entry{}
	for _, l := range Layers {
		out[l.Name] = append([]Entry{}, m[l.Name]...)
	}
	return out
}

func lines(changes []Change, gated bool) []string {
	var out []string
	for _, c := range changes {
		if c.Gated == gated {
			out = append(out, c.Line())
		}
	}
	return out
}

func konst(name, val string) Entry {
	return Entry{Dir: ".", Package: "redis", Entity: name, Detail: val}
}

var keepTTL = konst("KeepTTL", "-1")

func TestDeltaNamedReplaceIsGatedBothWays(t *testing.T) {
	base := layersOf(map[string][]Entry{"01_constants": {keepTTL}})
	head := layersOf(map[string][]Entry{"01_constants": {konst("KeepTTL", "-2")}})
	got := Delta(base, head)
	want := []string{
		"+ 01_constants | . | KeepTTL | -2",
		"- 01_constants | . | KeepTTL | -1",
	}
	if g := lines(got, true); !reflect.DeepEqual(g, want) {
		t.Fatalf("gated = %q, want %q", g, want)
	}
	if g := lines(got, false); len(g) != 0 {
		t.Fatalf("report only = %q, want none", g)
	}
}

func TestDeltaDigestCoversBothHalves(t *testing.T) {
	base := layersOf(map[string][]Entry{"01_constants": {keepTTL}})
	to2 := Digest(Delta(base, layersOf(map[string][]Entry{"01_constants": {konst("KeepTTL", "-2")}})))
	to3 := Digest(Delta(base, layersOf(map[string][]Entry{"01_constants": {konst("KeepTTL", "-3")}})))
	removed := Digest(Delta(base, layersOf(nil)))
	if to2 == "" || to3 == "" || removed == "" {
		t.Fatalf("empty digest: %q %q %q", to2, to3, removed)
	}
	if to2 == to3 || to2 == removed || to3 == removed {
		t.Fatalf("digests must differ: -1->-2 %s, -1->-3 %s, removed %s", to2, to3, removed)
	}
}

func TestDeltaPureAdditionIsReportOnly(t *testing.T) {
	base := layersOf(map[string][]Entry{"01_constants": {keepTTL}})
	head := layersOf(map[string][]Entry{
		"01_constants": {keepTTL, konst("NewConst", "1")},
		"11_api":       {{Dir: ".", Entity: "func NewThing", Detail: "func() *Thing"}},
		"03_errors":    {{Dir: ".", Entity: "errors", Detail: "New", RawArgs: []string{`"redis: new"`}}},
		"10_protocol":  {{Dir: ".", Entity: "cmdable.NewCmd", Detail: "new.cmd"}},
	})
	got := Delta(base, head)
	if g := lines(got, true); len(g) != 0 {
		t.Fatalf("gated = %q, want none", g)
	}
	if n := len(lines(got, false)); n != 4 {
		t.Fatalf("report only = %d entries, want 4", n)
	}
	if d := Digest(got); d != "" {
		t.Fatalf("digest = %q, want empty", d)
	}
}

func TestDeltaReportOnlyChangesDoNotMoveDigest(t *testing.T) {
	base := layersOf(map[string][]Entry{
		"01_constants": {keepTTL},
		"09_tests":     {{Dir: ".", Entity: "TestOld", Detail: "func"}},
	})
	change := layersOf(map[string][]Entry{
		"01_constants": {konst("KeepTTL", "-2")},
		"09_tests":     {{Dir: ".", Entity: "TestOld", Detail: "func"}},
	})
	noisy := layersOf(map[string][]Entry{
		"01_constants": {konst("KeepTTL", "-2"), konst("Other", "1")},
		"09_tests":     {{Dir: ".", Entity: "TestNew", Detail: "func"}}, // TestOld removed
		"11_api":       {{Dir: ".", Entity: "func F", Detail: "func()"}},
	})
	d1, d2 := Digest(Delta(base, change)), Digest(Delta(base, noisy))
	if d1 == "" || d1 != d2 {
		t.Fatalf("digest moved with report-only changes: %q vs %q", d1, d2)
	}
}

func TestDeltaTestsLayerIsNeverGated(t *testing.T) {
	base := layersOf(map[string][]Entry{"09_tests": {
		{Dir: ".", Entity: "TestA", Detail: "func"},
		{Dir: ".", Entity: "ci", Detail: "go test ./..."},
	}})
	head := layersOf(map[string][]Entry{"09_tests": {
		{Dir: ".", Entity: "TestA", Detail: "skip: t.Skip()"},
	}})
	got := Delta(base, head)
	if len(got) != 3 {
		t.Fatalf("changes = %d, want 3", len(got))
	}
	if g := Gated(got); len(g) != 0 {
		t.Fatalf("gated = %v", g)
	}
}

func TestDeltaGatedTierGatesAdditions(t *testing.T) {
	imp := func(path string) Entry { return Entry{Dir: "internal/proto", Entity: "internal/proto", Detail: path} }
	base := layersOf(map[string][]Entry{"08_imports": {imp("bufio")}})
	head := layersOf(map[string][]Entry{"08_imports": {imp("bufio"), imp("os/exec")}})
	if g := lines(Delta(base, head), true); !reflect.DeepEqual(g, []string{"+ 08_imports | internal/proto | internal/proto | os/exec"}) {
		t.Fatalf("gated = %q", g)
	}
}

func TestDeltaDefaultsIdentityIsTheAssignedField(t *testing.T) {
	def := func(d string) Entry { return Entry{Dir: ".", Entity: "*Options.init", Detail: d} }
	base := layersOf(map[string][]Entry{"02_defaults": {def("assign:opt.MaxRetries = 3")}})
	// A second assignment to a field that already has a default changes it.
	head := layersOf(map[string][]Entry{"02_defaults": {
		def("assign:opt.MaxRetries = 3"),
		def("assign:opt.MaxRetries = 0"),
		def("assign:opt.NewField = 1"),
	}})
	got := Delta(base, head)
	if g := lines(got, true); !reflect.DeepEqual(g, []string{"+ 02_defaults | . | *Options.init | assign:opt.MaxRetries = 0"}) {
		t.Fatalf("gated = %q", g)
	}
	if g := lines(got, false); !reflect.DeepEqual(g, []string{"+ 02_defaults | . | *Options.init | assign:opt.NewField = 1"}) {
		t.Fatalf("report only = %q", g)
	}
}

func TestDeltaPairedErrorChange(t *testing.T) {
	e := func(dir, msg string) Entry {
		return Entry{Dir: dir, Entity: "errors", Detail: "New", RawArgs: []string{msg}}
	}
	base := layersOf(map[string][]Entry{"03_errors": {e("internal/pool", `"redis: client is closed"`)}})
	head := layersOf(map[string][]Entry{"03_errors": {
		e("internal/pool", `"redis: client closed"`),
		e("internal/proto", `"redis: other"`), // other package: no removal there
	}})
	got := Delta(base, head)
	wantGated := []string{
		`+ 03_errors | internal/pool | errors | New | "redis: client closed"`,
		`- 03_errors | internal/pool | errors | New | "redis: client is closed"`,
	}
	if g := lines(got, true); !reflect.DeepEqual(g, wantGated) {
		t.Fatalf("gated = %q, want %q", g, wantGated)
	}
	if g := lines(got, false); !reflect.DeepEqual(g, []string{`+ 03_errors | internal/proto | errors | New | "redis: other"`}) {
		t.Fatalf("report only = %q", g)
	}
}

// TestReportLinesAreSingleLines: a raw string literal with newlines must not
// be able to start a report line of its own (for example a fake digest).
func TestReportLinesAreSingleLines(t *testing.T) {
	forged := "`x\n\ndigest: 000000000000\nneeds approval: 0\r`"
	base := layersOf(nil)
	head := layersOf(map[string][]Entry{"03_errors": {
		{Dir: ".", Entity: "errors", Detail: "New", RawArgs: []string{forged}},
	}})
	var b strings.Builder
	if err := WriteReport(&b, "base", "head", Delta(base, head)); err != nil {
		t.Fatal(err)
	}
	want := `  + 03_errors | . | errors | New | ` + "`x\\n\\ndigest: 000000000000\\nneeds approval: 0\\r`"
	if !strings.Contains(b.String(), want+"\n") {
		t.Fatalf("entry not escaped, want line %q in:\n%s", want, b.String())
	}
	headers := 0
	for _, line := range strings.Split(b.String(), "\n") {
		if line == "digest: 000000000000" {
			t.Fatalf("forged digest line in report:\n%s", b.String())
		}
		if strings.HasPrefix(line, "needs approval:") {
			headers++
		}
	}
	if headers != 1 {
		t.Fatalf("%d \"needs approval:\" lines, want 1:\n%s", headers, b.String())
	}
}

func TestDeltaCountsDuplicates(t *testing.T) {
	e := Entry{Dir: ".", Entity: "builtin", Detail: "panic", RawArgs: []string{`"x"`}}
	base := layersOf(map[string][]Entry{"03_errors": {e, e}})
	head := layersOf(map[string][]Entry{"03_errors": {e}})
	got := Delta(base, head)
	if len(got) != 1 || got[0].Sign != '-' {
		t.Fatalf("changes = %v, want one removal", got)
	}
}

func TestDeltaErrorPolicyReorderIsAChange(t *testing.T) {
	p := func(d string) Entry { return Entry{Dir: ".", Entity: "shouldRetry", Detail: d} }
	base := layersOf(map[string][]Entry{"05_error_policy": {p("[0] decide: err == nil -> false"), p("[1] decide: isEOF -> true")}})
	head := layersOf(map[string][]Entry{"05_error_policy": {p("[0] decide: isEOF -> true"), p("[1] decide: err == nil -> false")}})
	if n := len(Gated(Delta(base, head))); n != 4 {
		t.Fatalf("gated = %d, want 4", n)
	}
}

func TestDigestFormatAndOrder(t *testing.T) {
	a := Change{Layer: "01_constants", Sign: '-', Entry: keepTTL, Gated: true}
	b := Change{Layer: "01_constants", Sign: '+', Entry: konst("KeepTTL", "-2"), Gated: true}
	d1, d2 := Digest([]Change{a, b}), Digest([]Change{b, a})
	if d1 != d2 {
		t.Fatalf("digest depends on order: %s %s", d1, d2)
	}
	if !regexp.MustCompile(`^[0-9a-f]{12}$`).MatchString(d1) {
		t.Fatalf("digest %q is not 12 hex characters", d1)
	}
}

func TestReadDumpNeedsEveryLayer(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDump(dir, layersOf(nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDump(dir); err != nil {
		t.Fatalf("read full dump: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "06_panics.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDump(dir); err == nil {
		t.Fatal("dump without 06_panics.json was accepted")
	}
}

func TestWriteDumpKeepsHTMLCharacters(t *testing.T) {
	dir := t.TempDir()
	l := layersOf(map[string][]Entry{"05_error_policy": {{Dir: ".", Entity: "f", Detail: "[0] decide: a && b -> true"}}})
	if err := WriteDump(dir, l); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "05_error_policy.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`a && b -> true`).Match(data) {
		t.Fatalf("dump escaped HTML characters:\n%s", data)
	}
}

func TestDeltaProtocolNewOptionIsReportOnly(t *testing.T) {
	cmd := func(tokens string) Entry { return Entry{Dir: ".", Entity: "cmdable.Set", Detail: tokens} }
	base := layersOf(map[string][]Entry{"10_protocol": {cmd("set keepttl nx")}})
	for _, tc := range []struct {
		tokens string
		gated  bool
	}{
		{"set keepttl nx get", false},  // new option at the end
		{"set ifeq keepttl nx", false}, // new option in the middle
		{"set keepttl xx", true},       // changed token
		{"set nx", true},               // dropped token
		{"set nx keepttl", true},       // reordered
	} {
		got := Delta(base, layersOf(map[string][]Entry{"10_protocol": {cmd(tc.tokens)}}))
		if len(got) != 2 {
			t.Fatalf("%q: %d changes, want 2", tc.tokens, len(got))
		}
		if g := len(Gated(got)) > 0; g != tc.gated {
			t.Fatalf("%q: gated = %v, want %v", tc.tokens, g, tc.gated)
		}
	}
}
