// Command csccmdgen generates csc_command_table.go, the command-metadata
// table used by CSC eligibility. It reads the COMMAND reply from a running
// Redis (use the pinned client-libs-test image so module metadata is
// included) and writes the normalized metadata as Go source:
//
//	go run ./internal/csccmdgen -addr localhost:6379 -out csc_command_table.go
//
// The output is checked in, so a regeneration is a reviewable git diff and
// the root eligibility tests gate the result — the generator stays simple.
//
// Normalization: names lowercased, subcommands flattened to "parent|child",
// non-readonly commands pruned (missing entries fail closed anyway). Key
// specs marked "incomplete" or "not_key" never prove keyedness. Extraction
// metadata is emitted only when the key positions are unambiguous; anything
// else is served uncached. When regenerating against a new release, check
// newly eligible commands with a tracking probe before trusting them (see the
// dont_cache overrides in csc_commands.go for known server-side gaps).
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"go/format"
	"os"
	"regexp"
	"sort"
	"strings"

	redis "github.com/redis/go-redis/v9"
)

// ackExtraKeySpecs lists commands whose extra unknown key specs are handled
// elsewhere. sort_ro's unknown spec is the optional BY/GET patterns, which
// isCacheable rejects at the argument level.
var ackExtraKeySpecs = map[string]bool{
	"sort_ro": true,
}

type keySpec struct {
	beginType  string // begin_search type: index | keyword | unknown
	findType   string // find_keys type: range | keynum | unknown
	bsIndex    int64
	keyStep    int64
	keyNumIdx  int64
	fkFirstKey int64
	incomplete bool
}

func (s keySpec) complete() bool {
	return (s.beginType == "index" || s.beginType == "keyword") &&
		(s.findType == "range" || s.findType == "keynum") &&
		!s.incomplete
}

type cmdMeta struct {
	name     string
	flags    map[string]bool
	tips     map[string]bool
	first    int64
	last     int64
	step     int64
	keySpecs []keySpec
}

func main() {
	addr := flag.String("addr", "localhost:6379", "address of the Redis server to read COMMAND metadata from")
	out := flag.String("out", "csc_command_table.go", "output file")
	flag.Parse()

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: *addr, Protocol: 3})
	defer client.Close()

	reply, err := client.Do(ctx, "command").Result()
	if err != nil {
		fatalf("COMMAND failed: %v", err)
	}
	entries, ok := reply.([]interface{})
	if !ok {
		fatalf("unexpected COMMAND reply type %T", reply)
	}

	var cmds []cmdMeta
	for _, e := range entries {
		cmds = append(cmds, parseEntry(e)...)
	}
	sort.Slice(cmds, func(i, j int) bool { return cmds[i].name < cmds[j].name })

	src, emitted := generate(cmds, describeServer(ctx, client))
	formatted, err := format.Source(src)
	if err != nil {
		fatalf("gofmt of generated source failed: %v\n%s", err, src)
	}
	if err := os.WriteFile(*out, formatted, 0o644); err != nil {
		fatalf("write %s: %v", *out, err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s: %d readonly command entries (of %d total)\n",
		*out, emitted, len(cmds))
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "csccmdgen: "+format+"\n", args...)
	os.Exit(1)
}

// parseEntry decodes one COMMAND entry — [name, arity, flags, first, last,
// step, acl-categories, tips, key-specs, subcommands] — and flattens its
// subcommands (already named "parent|child" on the wire).
func parseEntry(e interface{}) []cmdMeta {
	fields, ok := e.([]interface{})
	if !ok {
		fatalf("unexpected COMMAND entry type %T", e)
	}
	// The legacy pre-7 shape has no tips or key specs, so accepting it would
	// silently drop the negative signals.
	if len(fields) < 10 {
		fatalf("COMMAND entry %v has %d fields, want >= 10 (Redis >= 7 reply shape)", fields, len(fields))
	}
	m := cmdMeta{
		name:  strings.ToLower(asString(fields[0])),
		flags: map[string]bool{},
		tips:  map[string]bool{},
		first: asInt(fields[3]),
		last:  asInt(fields[4]),
		step:  asInt(fields[5]),
	}
	for _, f := range asSlice(fields[2]) {
		m.flags[asString(f)] = true
	}
	for _, t := range asSlice(fields[7]) {
		m.tips[asString(t)] = true
	}
	for _, s := range asSlice(fields[8]) {
		m.keySpecs = append(m.keySpecs, parseKeySpec(s))
	}
	out := []cmdMeta{m}
	for _, sub := range asSlice(fields[9]) {
		out = append(out, parseEntry(sub)...)
	}
	return out
}

func parseKeySpec(v interface{}) keySpec {
	spec := asMap(v)
	ks := keySpec{}
	for _, f := range asSlice(spec["flags"]) {
		switch asString(f) {
		case "incomplete", "not_key":
			// Neither can prove real key positions (not_key marks a channel
			// or pattern, e.g. SPUBLISH), so treat both as incomplete.
			ks.incomplete = true
		}
	}
	bs := asMap(spec["begin_search"])
	ks.beginType = asString(bs["type"])
	bsSpec := asMap(bs["spec"])
	ks.bsIndex = asInt(bsSpec["index"])

	fk := asMap(spec["find_keys"])
	ks.findType = asString(fk["type"])
	fkSpec := asMap(fk["spec"])
	ks.keyStep = asInt(fkSpec["keystep"])
	ks.keyNumIdx = asInt(fkSpec["keynumidx"])
	ks.fkFirstKey = asInt(fkSpec["firstkey"])
	return ks
}

func describeServer(ctx context.Context, client *redis.Client) string {
	desc := "unknown"
	if info, err := client.Info(ctx, "server").Result(); err == nil {
		if m := regexp.MustCompile(`redis_version:(\S+)`).FindStringSubmatch(info); m != nil {
			desc = "redis_version " + m[1]
		}
	}
	var mods []string
	if reply, err := client.Do(ctx, "module", "list").Result(); err == nil {
		for _, m := range asSlice(reply) {
			fields := asMap(m)
			if name := asString(fields["name"]); name != "" {
				mods = append(mods, fmt.Sprintf("%s %v", name, fields["ver"]))
			}
		}
		sort.Strings(mods)
	}
	if len(mods) > 0 {
		desc += "; modules: " + strings.Join(mods, ", ")
	}
	return desc
}

func generate(cmds []cmdMeta, server string) ([]byte, int) {
	var b bytes.Buffer
	fmt.Fprintf(&b, `// Code generated by internal/csccmdgen; DO NOT EDIT.
//
// Source: %s
//
// COMMAND metadata for CSC eligibility (see csc_commands.go), pruned to
// readonly commands: the rest are never cacheable and missing entries fail
// closed anyway.

package redis

var cscCommandTable = map[string]cscCommandMeta{
`, server)

	emitted := 0
	for _, c := range cmds {
		if !c.flags["readonly"] {
			continue
		}
		// Skip module-internal commands (_FT.*, search.*, timeseries.*);
		// applications never issue them.
		if strings.HasPrefix(c.name, "_") ||
			strings.HasPrefix(c.name, "search.") ||
			strings.HasPrefix(c.name, "timeseries.") {
			continue
		}
		fmt.Fprintf(&b, "\t%q: {%s},\n", c.name, entryFields(c))
		emitted++
	}
	b.WriteString("}\n")
	return b.Bytes(), emitted
}

func entryFields(c cmdMeta) string {
	var bits []string
	if c.flags["readonly"] {
		bits = append(bits, "cscFlagReadonly")
	}
	if c.flags["script_runner"] {
		bits = append(bits, "cscFlagScriptRunner")
	}
	if c.flags["blocking"] {
		bits = append(bits, "cscFlagBlocking")
	}
	if c.tips["nondeterministic_output"] {
		bits = append(bits, "cscTipNondeterministicOutput")
	}
	if c.tips["dont_cache"] {
		bits = append(bits, "cscTipDontCache")
	}
	var complete, other int
	var keynum keySpec
	for _, s := range c.keySpecs {
		if s.complete() {
			complete++
			if s.findType == "keynum" {
				keynum = s
			}
		} else {
			other++
		}
	}
	if complete > 0 {
		bits = append(bits, "cscHasKeySpec")
	}

	fields := []string{"bits: " + strings.Join(bits, " | ")}

	// Extraction metadata only when the key positions are unambiguous: at
	// most one complete spec (the legacy triple describes only the first
	// spec, so more would give a partial key list — dropped invalidations,
	// stale entries). Everything else is served uncached.
	canExtract := (other == 0 && complete <= 1) || ackExtraKeySpecs[c.name]
	switch {
	case canExtract && c.first > 0 && c.step > 0:
		fields = append(fields,
			"extract: cscKeyExtractRange",
			fmt.Sprintf("firstKey: %d", c.first),
			fmt.Sprintf("lastKey: %d", c.last),
			fmt.Sprintf("step: %d", c.step))
	case canExtract && complete == 1 && keynum.findType == "keynum" && keynum.beginType == "index":
		fields = append(fields,
			"extract: cscKeyExtractKeynum",
			fmt.Sprintf("numkeysAt: %d", keynum.bsIndex+keynum.keyNumIdx),
			fmt.Sprintf("firstKey: %d", keynum.bsIndex+keynum.fkFirstKey),
			fmt.Sprintf("step: %d", keynum.keyStep))
	}
	return strings.Join(fields, ", ")
}

func asString(v interface{}) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return ""
	}
}

func asInt(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}

func asSlice(v interface{}) []interface{} {
	s, _ := v.([]interface{})
	return s
}

func asMap(v interface{}) map[string]interface{} {
	switch m := v.(type) {
	case map[string]interface{}:
		return m
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for k, val := range m {
			out[asString(k)] = val
		}
		return out
	default:
		return nil
	}
}
