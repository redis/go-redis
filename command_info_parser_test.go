package redis

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func commandInfoTestBulk(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func commandInfoTestVerbatim(s string) string {
	return fmt.Sprintf("=%d\r\n%s\r\n", len(s), s)
}

func commandInfoTestInt(v int64) string {
	return fmt.Sprintf(":%d\r\n", v)
}

func commandInfoTestArray(values ...string) string {
	return fmt.Sprintf("*%d\r\n%s", len(values), strings.Join(values, ""))
}

func commandInfoTestSet(values ...string) string {
	return fmt.Sprintf("~%d\r\n%s", len(values), strings.Join(values, ""))
}

// COMMAND encodes maps as field/value arrays in RESP2.
func commandInfoTestMap(pairs ...string) string {
	return commandInfoTestArray(pairs...)
}

func commandInfoTestRESP3Map(pairs ...string) string {
	return fmt.Sprintf("%%%d\r\n%s", len(pairs)/2, strings.Join(pairs, ""))
}

func commandInfoTestEntry6(name string) string {
	return commandInfoTestArray(
		commandInfoTestBulk(name),
		commandInfoTestInt(-2),
		commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
	)
}

func commandInfoTestEntry7(name string) string {
	return commandInfoTestArray(
		commandInfoTestBulk(name),
		commandInfoTestInt(-2),
		commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestArray(commandInfoTestBulk("@read")),
	)
}

func commandInfoTestEntry10(
	name string,
	flags, tips, keySpecs, subcommands string,
) string {
	return commandInfoTestArray(
		commandInfoTestBulk(name),
		commandInfoTestInt(-2),
		flags,
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestArray(),
		tips,
		keySpecs,
		subcommands,
	)
}

func commandInfoTestRangeKeySpec(
	flagsValue, beginTypeValue, beginIndexValue string,
) string {
	return commandInfoTestMap(
		commandInfoTestBulk("flags"), flagsValue,
		commandInfoTestBulk("begin_search"), commandInfoTestMap(
			commandInfoTestBulk("type"), beginTypeValue,
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("index"), beginIndexValue,
			),
		),
		commandInfoTestBulk("find_keys"), commandInfoTestMap(
			commandInfoTestBulk("type"), commandInfoTestBulk("range"),
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("lastkey"), commandInfoTestInt(0),
				commandInfoTestBulk("keystep"), commandInfoTestInt(1),
				commandInfoTestBulk("limit"), commandInfoTestInt(0),
			),
		),
	)
}

func commandInfoTestValidRangeKeySpec() string {
	return commandInfoTestRangeKeySpec(
		commandInfoTestArray(commandInfoTestBulk("RO")),
		commandInfoTestBulk("index"),
		commandInfoTestInt(1),
	)
}

func commandInfoTestReadReply(t *testing.T, raw string) *CommandsInfoCmd {
	t.Helper()
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(raw))); err != nil {
		t.Fatalf("readReply: %v", err)
	}
	return cmd
}

func TestCommandsInfoMalformedRecordDoesNotAbortReply(t *testing.T) {
	validSpec := commandInfoTestValidRangeKeySpec()
	validFlags := commandInfoTestArray(commandInfoTestBulk("readonly"))
	empty := commandInfoTestArray()

	tests := []struct {
		name string
		bad  string
	}{
		{
			name: "RESP2 nil tip",
			bad: commandInfoTestEntry10("bad", validFlags,
				commandInfoTestArray("$-1\r\n"), commandInfoTestArray(validSpec), empty),
		},
		{
			name: "RESP3 nil tip",
			bad: commandInfoTestEntry10("bad", validFlags,
				commandInfoTestArray("_\r\n"), commandInfoTestArray(validSpec), empty),
		},
		{
			name: "integer tip",
			bad: commandInfoTestEntry10("bad", validFlags,
				commandInfoTestArray(commandInfoTestInt(1)), commandInfoTestArray(validSpec), empty),
		},
		{
			name: "nil command flag",
			bad: commandInfoTestEntry10("bad", commandInfoTestArray("$-1\r\n"),
				empty, commandInfoTestArray(validSpec), empty),
		},
		{
			name: "nil key spec flag",
			bad: commandInfoTestEntry10("bad", validFlags, empty,
				commandInfoTestArray(commandInfoTestRangeKeySpec(
					commandInfoTestArray("$-1\r\n"), commandInfoTestBulk("index"), commandInfoTestInt(1),
				)), empty),
		},
		{
			name: "scalar key spec flags",
			bad: commandInfoTestEntry10("bad", validFlags, empty,
				commandInfoTestArray(commandInfoTestRangeKeySpec(
					commandInfoTestBulk("RO"), commandInfoTestBulk("index"), commandInfoTestInt(1),
				)), empty),
		},
		{
			name: "aggregate begin search type",
			bad: commandInfoTestEntry10("bad", validFlags, empty,
				commandInfoTestArray(commandInfoTestRangeKeySpec(
					commandInfoTestArray(commandInfoTestBulk("RO")), empty, commandInfoTestInt(1),
				)), empty),
		},
		{
			name: "aggregate numeric spec value",
			bad: commandInfoTestEntry10("bad", validFlags, empty,
				commandInfoTestArray(commandInfoTestRangeKeySpec(
					commandInfoTestArray(commandInfoTestBulk("RO")), commandInfoTestBulk("index"),
					commandInfoTestArray(commandInfoTestInt(1)),
				)), empty),
		},
		{
			name: "odd RESP2 key spec map",
			bad: commandInfoTestEntry10("bad", validFlags, empty,
				commandInfoTestArray(commandInfoTestArray(
					commandInfoTestBulk("flags"), empty, commandInfoTestBulk("dangling"),
				)), empty),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := commandInfoTestArray(
				commandInfoTestEntry6("before"),
				tt.bad,
				commandInfoTestEntry6("after"),
			)
			cmd := commandInfoTestReadReply(t, raw)
			if cmd.val["before"] == nil || cmd.val["after"] == nil {
				t.Fatalf("valid siblings were lost: %#v", cmd.val)
			}
			bad, exists := cmd.val["bad"]
			if !exists || bad != nil {
				t.Fatalf("malformed record = %#v, exists=%v; want nil tombstone", bad, exists)
			}
		})
	}
}

func TestCommandsInfoDuplicateNameFailsClosed(t *testing.T) {
	valid := commandInfoTestEntry6("duplicate")
	malformed := commandInfoTestEntry10(
		"duplicate",
		commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray("_\r\n"),
		commandInfoTestArray(commandInfoTestValidRangeKeySpec()),
		commandInfoTestArray(),
	)
	for _, entries := range [][]string{{malformed, valid}, {valid, malformed}} {
		cmd := commandInfoTestReadReply(t, commandInfoTestArray(
			entries[0], entries[1], commandInfoTestEntry6("after"),
		))
		if got, exists := cmd.val["duplicate"]; !exists || got != nil {
			t.Fatalf("duplicate record = %#v, exists=%v; want sticky tombstone", got, exists)
		}
		if cmd.val["after"] == nil {
			t.Fatal("duplicate record desynchronized the following record")
		}
	}

	cmd := commandInfoTestReadReply(t, commandInfoTestArray(valid, valid))
	if cmd.val["duplicate"] == nil {
		t.Fatal("two valid COMMAND INFO duplicates were tombstoned")
	}

	conflicting := commandInfoTestEntry10(
		"duplicate", commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray(commandInfoTestBulk("dont_cache")),
		commandInfoTestArray(commandInfoTestValidRangeKeySpec()), commandInfoTestArray(),
	)
	cmd = commandInfoTestReadReply(t, commandInfoTestArray(valid, conflicting, commandInfoTestEntry6("after")))
	if info, ok := cmd.val["duplicate"]; !ok || info != nil {
		t.Fatalf("conflicting duplicate=%+v present=%v, want tombstone", info, ok)
	}
	if cmd.val["after"] == nil {
		t.Fatal("conflicting duplicate desynchronized the following record")
	}
}

func TestCommandsInfoUnknownKeySpecAlgorithmsArePreservedFailClosed(t *testing.T) {
	validFlags := commandInfoTestArray(commandInfoTestBulk("readonly"))
	empty := commandInfoTestArray()
	tests := []struct {
		name string
		spec string
	}{
		{
			name: "begin search",
			spec: commandInfoTestRangeKeySpec(
				commandInfoTestArray(commandInfoTestBulk("RO")),
				commandInfoTestBulk("future-index"), commandInfoTestInt(1),
			),
		},
		{
			name: "find keys",
			spec: commandInfoTestMap(
				commandInfoTestBulk("flags"), commandInfoTestArray(commandInfoTestBulk("RO")),
				commandInfoTestBulk("begin_search"), commandInfoTestMap(
					commandInfoTestBulk("type"), commandInfoTestBulk("index"),
					commandInfoTestBulk("spec"), commandInfoTestMap(
						commandInfoTestBulk("index"), commandInfoTestInt(1),
					),
				),
				commandInfoTestBulk("find_keys"), commandInfoTestMap(
					commandInfoTestBulk("type"), commandInfoTestBulk("future-range"),
					commandInfoTestBulk("spec"), commandInfoTestMap(
						commandInfoTestBulk("lastkey"), commandInfoTestInt(0),
						commandInfoTestBulk("keystep"), commandInfoTestInt(1),
						commandInfoTestBulk("limit"), commandInfoTestInt(0),
					),
				),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bad := commandInfoTestEntry10(
				"bad", validFlags, empty, commandInfoTestArray(tt.spec), empty,
			)
			cmd := commandInfoTestReadReply(t, commandInfoTestArray(bad, commandInfoTestEntry6("after")))
			info, exists := cmd.val["bad"]
			if !exists || info == nil {
				t.Fatalf("well-typed unknown key algorithm = %#v, exists=%v; want preserved record", info, exists)
			}
			if got := cscDeriveMeta(info); got.extract != cscKeyExtractNone {
				t.Fatalf("unknown key algorithm produced CSC extraction: %+v", got)
			}
			if got := deriveRoutingCommandMeta("bad", info); got.keyState != routingKeysUnknown {
				t.Fatalf("unknown key algorithm routing state = %v, want unknown", got.keyState)
			}
			if cmd.val["after"] == nil {
				t.Fatal("unknown key algorithm desynchronized the following record")
			}
		})
	}
}

func TestCommandsInfoPositionalRecordRejectsRESP3Set(t *testing.T) {
	setRecord := commandInfoTestSet(
		commandInfoTestBulk("bad"),
		commandInfoTestInt(-2),
		commandInfoTestSet(commandInfoTestBulk("readonly")),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(
		commandInfoTestEntry6("before"), setRecord, commandInfoTestEntry6("after"),
	))
	if cmd.val["before"] == nil || cmd.val["after"] == nil {
		t.Fatalf("positional set desynchronized siblings: %#v", cmd.val)
	}
	if _, exists := cmd.val["bad"]; exists {
		t.Fatalf("unordered positional record should be omitted, got %#v", cmd.val["bad"])
	}
}

func TestCommandsInfoUnknownMetadataFieldsAreDrained(t *testing.T) {
	begin := commandInfoTestMap(
		commandInfoTestBulk("type"), commandInfoTestBulk("index"),
		commandInfoTestBulk("future_begin"), commandInfoTestArray(commandInfoTestBulk("ignored")),
		commandInfoTestBulk("spec"), commandInfoTestMap(
			commandInfoTestBulk("index"), commandInfoTestInt(1),
			commandInfoTestBulk("future_index"), commandInfoTestMap(
				commandInfoTestBulk("nested"), commandInfoTestBulk("ignored"),
			),
		),
	)
	find := commandInfoTestMap(
		commandInfoTestBulk("type"), commandInfoTestBulk("range"),
		commandInfoTestBulk("spec"), commandInfoTestMap(
			commandInfoTestBulk("lastkey"), commandInfoTestInt(0),
			commandInfoTestBulk("keystep"), commandInfoTestInt(1),
			commandInfoTestBulk("limit"), commandInfoTestInt(0),
		),
	)
	spec := commandInfoTestMap(
		commandInfoTestBulk("flags"), commandInfoTestArray(
			commandInfoTestBulk("RO"), commandInfoTestBulk("future_key_flag"),
		),
		commandInfoTestBulk("future_key_spec"), commandInfoTestArray(commandInfoTestBulk("ignored")),
		commandInfoTestBulk("begin_search"), begin,
		commandInfoTestBulk("find_keys"), find,
	)
	entry := commandInfoTestEntry10(
		"extended",
		commandInfoTestArray(commandInfoTestBulk("readonly"), commandInfoTestBulk("future_flag")),
		commandInfoTestArray(commandInfoTestBulk("future_tip")),
		commandInfoTestArray(spec),
		commandInfoTestArray(),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(entry, commandInfoTestEntry6("after")))
	info := cmd.val["extended"]
	if info == nil {
		t.Fatal("well-typed extensions must not tombstone the record")
	}
	if len(info.KeySpecs) != 1 || info.KeySpecs[0].Index != 1 || info.KeySpecs[0].KeyStep != 1 {
		t.Fatalf("known key metadata was not retained: %+v", info.KeySpecs)
	}
	if cmd.val["after"] == nil {
		t.Fatal("unknown nested field desynchronized the following record")
	}
}

func TestCommandsInfoKeySpecSectionsCannotOverwriteEachOther(t *testing.T) {
	// A misplaced field must not overwrite the valid find_keys value.
	spec := commandInfoTestMap(
		commandInfoTestBulk("flags"), commandInfoTestArray(commandInfoTestBulk("RO")),
		commandInfoTestBulk("find_keys"), commandInfoTestMap(
			commandInfoTestBulk("type"), commandInfoTestBulk("range"),
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("lastkey"), commandInfoTestInt(-1),
				commandInfoTestBulk("keystep"), commandInfoTestInt(1),
				commandInfoTestBulk("limit"), commandInfoTestInt(0),
			),
		),
		commandInfoTestBulk("begin_search"), commandInfoTestMap(
			commandInfoTestBulk("type"), commandInfoTestBulk("index"),
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("index"), commandInfoTestInt(1),
				commandInfoTestBulk("lastkey"), commandInfoTestInt(0),
			),
		),
	)
	entry := commandInfoTestEntry10(
		"safe", commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray(), commandInfoTestArray(spec), commandInfoTestArray(),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(entry))
	info := cmd.val["safe"]
	if info == nil || len(info.KeySpecs) != 1 || info.KeySpecs[0].LastKey != -1 {
		t.Fatalf("cross-section field changed key range: %+v", info)
	}
	meta := cscDeriveMeta(info)
	keys := cscExtractRedisKeys(meta, NewCmd(context.Background(), "safe", "one", "two"))
	if len(keys) != 2 || keys[0] != "one" || keys[1] != "two" {
		t.Fatalf("cross-section field caused partial extraction: %v (%+v)", keys, meta)
	}
}

func TestCommandsInfoFindKeysCannotOverwriteBeginKeyword(t *testing.T) {
	spec := commandInfoTestMap(
		commandInfoTestBulk("flags"), commandInfoTestArray(commandInfoTestBulk("RO")),
		commandInfoTestBulk("begin_search"), commandInfoTestMap(
			commandInfoTestBulk("type"), commandInfoTestBulk("keyword"),
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("keyword"), commandInfoTestBulk("KEYS"),
				commandInfoTestBulk("startfrom"), commandInfoTestInt(1),
			),
		),
		commandInfoTestBulk("find_keys"), commandInfoTestMap(
			commandInfoTestBulk("type"), commandInfoTestBulk("range"),
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("lastkey"), commandInfoTestInt(-1),
				commandInfoTestBulk("keystep"), commandInfoTestInt(1),
				commandInfoTestBulk("limit"), commandInfoTestInt(0),
				commandInfoTestBulk("keyword"), commandInfoTestBulk("WRONG"),
			),
		),
	)
	entry := commandInfoTestEntry10(
		"safe", commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray(), commandInfoTestArray(spec), commandInfoTestArray(),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(entry))
	info := cmd.val["safe"]
	if info == nil || len(info.KeySpecs) != 1 || info.KeySpecs[0].Keyword != "KEYS" {
		t.Fatalf("find_keys field changed begin-search keyword: %+v", info)
	}
}

func TestCommandsInfoMalformedVerbatimIsRecordLocal(t *testing.T) {
	bad := commandInfoTestEntry10(
		"bad", commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray(commandInfoTestVerbatim("broken")),
		commandInfoTestArray(), commandInfoTestArray(),
	)
	good := commandInfoTestEntry10(
		"good", commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray(), commandInfoTestArray(), commandInfoTestArray(),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(bad, good))
	if info, ok := cmd.val["bad"]; !ok || info != nil {
		t.Fatalf("malformed verbatim record=%+v present=%v, want tombstone", info, ok)
	}
	if cmd.val["good"] == nil {
		t.Fatal("malformed verbatim discarded a valid sibling record")
	}
}

func TestCommandsInfoRESP3KeySpecMapsAndSevenFieldRecord(t *testing.T) {
	resp3Spec := commandInfoTestRESP3Map(
		commandInfoTestBulk("flags"), commandInfoTestArray(commandInfoTestBulk("RO")),
		commandInfoTestBulk("begin_search"), commandInfoTestRESP3Map(
			commandInfoTestBulk("type"), commandInfoTestBulk("index"),
			commandInfoTestBulk("spec"), commandInfoTestRESP3Map(
				commandInfoTestBulk("index"), commandInfoTestInt(1),
			),
		),
		commandInfoTestBulk("find_keys"), commandInfoTestRESP3Map(
			commandInfoTestBulk("type"), commandInfoTestBulk("range"),
			commandInfoTestBulk("spec"), commandInfoTestRESP3Map(
				commandInfoTestBulk("lastkey"), commandInfoTestInt(0),
				commandInfoTestBulk("keystep"), commandInfoTestInt(1),
				commandInfoTestBulk("limit"), commandInfoTestInt(0),
			),
		),
	)
	resp3 := commandInfoTestEntry10(
		"resp3", commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray(), commandInfoTestArray(resp3Spec), commandInfoTestArray(),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(commandInfoTestEntry7("redis6"), resp3))

	legacy := cmd.val["redis6"]
	if legacy == nil || legacy.Arity != -2 || len(legacy.ACLFlags) != 1 || legacy.ACLFlags[0] != "@read" {
		t.Fatalf("seven-field record did not parse: %+v", legacy)
	}
	if _, ok := cmd.legacyRecords["redis6"]; !ok {
		t.Fatal("seven-field record lost its legacy wire-shape provenance")
	}
	info := cmd.val["resp3"]
	if info == nil || len(info.KeySpecs) != 1 || info.KeySpecs[0].Index != 1 || info.KeySpecs[0].KeyStep != 1 {
		t.Fatalf("RESP3 key-spec maps did not parse: %+v", info)
	}
	if _, ok := cmd.legacyRecords["resp3"]; ok {
		t.Fatal("ten-field record was incorrectly marked legacy")
	}
}

func TestCommandsInfoRESP3CollectionsUseSets(t *testing.T) {
	resp3Spec := commandInfoTestRESP3Map(
		commandInfoTestBulk("flags"), commandInfoTestSet(commandInfoTestBulk("RO"), commandInfoTestBulk("access")),
		commandInfoTestBulk("begin_search"), commandInfoTestRESP3Map(
			commandInfoTestBulk("type"), commandInfoTestBulk("index"),
			commandInfoTestBulk("spec"), commandInfoTestRESP3Map(
				commandInfoTestBulk("index"), commandInfoTestInt(1),
			),
		),
		commandInfoTestBulk("find_keys"), commandInfoTestRESP3Map(
			commandInfoTestBulk("type"), commandInfoTestBulk("range"),
			commandInfoTestBulk("spec"), commandInfoTestRESP3Map(
				commandInfoTestBulk("lastkey"), commandInfoTestInt(0),
				commandInfoTestBulk("keystep"), commandInfoTestInt(1),
				commandInfoTestBulk("limit"), commandInfoTestInt(0),
			),
		),
	)
	child := commandInfoTestEntry10(
		"parent|child",
		commandInfoTestSet(commandInfoTestBulk("readonly")),
		commandInfoTestSet(),
		commandInfoTestSet(resp3Spec),
		commandInfoTestSet(),
	)
	parent := commandInfoTestEntry10(
		"parent",
		commandInfoTestSet(),
		commandInfoTestSet(commandInfoTestBulk("future_tip")),
		commandInfoTestSet(),
		commandInfoTestSet(child),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(parent, commandInfoTestEntry6("after")))

	info := cmd.val["parent|child"]
	if info == nil || !info.ReadOnly || len(info.KeySpecs) != 1 || info.KeySpecs[0].Index != 1 {
		t.Fatalf("RESP3 set collections did not parse: %+v", info)
	}
	if cmd.val["parent"] == nil || cmd.val["after"] == nil {
		t.Fatalf("RESP3 set collection desynchronized sibling records: %#v", cmd.val)
	}
}

func TestCommandsInfoMalformedSubcommandIsIsolated(t *testing.T) {
	empty := commandInfoTestArray()
	validFlags := commandInfoTestArray(commandInfoTestBulk("readonly"))
	badChild := commandInfoTestEntry10(
		"parent|bad", validFlags, commandInfoTestArray("_\r\n"),
		commandInfoTestArray(commandInfoTestValidRangeKeySpec()), empty,
	)
	parent := commandInfoTestEntry10(
		"parent", commandInfoTestArray(), empty, empty,
		commandInfoTestArray(
			commandInfoTestEntry6("parent|before"),
			badChild,
			commandInfoTestEntry6("parent|after"),
		),
	)
	cmd := commandInfoTestReadReply(t, commandInfoTestArray(parent))
	for _, name := range []string{"parent", "parent|before", "parent|after"} {
		if cmd.val[name] == nil {
			t.Fatalf("valid record %q was lost", name)
		}
	}
	if bad, exists := cmd.val["parent|bad"]; !exists || bad != nil {
		t.Fatalf("malformed child = %#v, exists=%v; want nil tombstone", bad, exists)
	}
}

func TestCommandsInfoUnknownEntryShapesFailClosed(t *testing.T) {
	known := []string{
		commandInfoTestBulk("bad"),
		commandInfoTestInt(-2),
		commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestArray(),
		commandInfoTestArray(),
		commandInfoTestArray(commandInfoTestValidRangeKeySpec()),
		commandInfoTestArray(),
	}
	tests := map[string]string{
		"eight fields": commandInfoTestArray(known[:8]...),
		"nine fields":  commandInfoTestArray(known[:9]...),
		"future field": commandInfoTestArray(append(append([]string(nil), known...),
			commandInfoTestMap(commandInfoTestBulk("future"), commandInfoTestBulk("ignored")))...),
	}
	for name, bad := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := commandInfoTestReadReply(t, commandInfoTestArray(bad, commandInfoTestEntry6("after")))
			if got, exists := cmd.val["bad"]; !exists || got != nil {
				t.Fatalf("unknown-shape record = %#v, exists=%v; want nil tombstone", got, exists)
			}
			if cmd.val["after"] == nil {
				t.Fatal("unknown entry shape desynchronized the following record")
			}
		})
	}
}

func TestCommandsInfoUnnamedMalformedEntryIsOmitted(t *testing.T) {
	raw := commandInfoTestArray(
		commandInfoTestEntry6("before"),
		commandInfoTestBulk("not-an-entry"),
		commandInfoTestEntry6("after"),
	)
	cmd := commandInfoTestReadReply(t, raw)
	if len(cmd.val) != 2 || cmd.val["before"] == nil || cmd.val["after"] == nil {
		t.Fatalf("unexpected parsed records: %#v", cmd.val)
	}
}

func TestCommandsInfoMalformedDrainFailureIsFatal(t *testing.T) {
	raw := "*1\r\n*2\r\n$3\r\nbad\r\n$5\r\nx"
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(raw))); err == nil {
		t.Fatal("truncated value returned nil error")
	}
}

func TestCommandsInfoTopLevelRedisErrorIsTypedAndConsumed(t *testing.T) {
	rd := proto.NewReader(strings.NewReader(
		"-NOPERM this user has no permissions to run the 'command' command\r\n" +
			commandInfoTestArray(commandInfoTestEntry6("after")),
	))
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	err := cmd.readReply(rd)
	if err == nil || !isRedisError(err) || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("top-level error = %T %v, want typed NOPERM", err, err)
	}

	next := NewCommandsInfoCmd(context.Background(), "command")
	if err := next.readReply(rd); err != nil {
		t.Fatalf("next aligned COMMAND reply failed: %v", err)
	}
	if next.val["after"] == nil {
		t.Fatal("next aligned COMMAND record was not parsed")
	}
}

func TestCommandsInfoExcessiveUnknownNestingIsFatal(t *testing.T) {
	nested := commandInfoTestBulk("ignored")
	for range maxCommandInfoDepth + 2 {
		nested = commandInfoTestArray(nested)
	}
	known := []string{
		commandInfoTestBulk("future"),
		commandInfoTestInt(-2),
		commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestInt(1),
		commandInfoTestArray(),
		commandInfoTestArray(),
		commandInfoTestArray(commandInfoTestValidRangeKeySpec()),
		commandInfoTestArray(),
		nested,
	}
	raw := commandInfoTestArray(commandInfoTestArray(known...))
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(raw))); err == nil {
		t.Fatal("excessively nested extension returned nil error")
	}
}

func TestCommandsInfoExcessiveAttributeNestingIsFatal(t *testing.T) {
	raw := strings.Repeat("|0\r\n", maxCommandInfoDepth+2) +
		commandInfoTestArray(commandInfoTestEntry6("get"))
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(raw))); err == nil {
		t.Fatal("excessively nested attributes returned nil error")
	}
}

func TestCommandsInfoHugeTruncatedCountDoesNotPreallocate(t *testing.T) {
	cmd := NewCommandsInfoCmd(context.Background(), "command")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(
		"*9223372036854775807\r\n",
	))); err == nil {
		t.Fatal("truncated huge aggregate returned nil error")
	}
}

func TestCommandsInfoNumericKeySpecFields(t *testing.T) {
	indexRange := commandInfoTestValidRangeKeySpec()
	keywordKeynum := commandInfoTestMap(
		commandInfoTestBulk("flags"), commandInfoTestArray(commandInfoTestBulk("RO")),
		commandInfoTestBulk("begin_search"), commandInfoTestMap(
			commandInfoTestBulk("type"), commandInfoTestBulk("keyword"),
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("keyword"), commandInfoTestBulk("KEYS"),
				commandInfoTestBulk("startfrom"), commandInfoTestInt(2),
			),
		),
		commandInfoTestBulk("find_keys"), commandInfoTestMap(
			commandInfoTestBulk("type"), commandInfoTestBulk("keynum"),
			commandInfoTestBulk("spec"), commandInfoTestMap(
				commandInfoTestBulk("keynumidx"), commandInfoTestInt(0),
				commandInfoTestBulk("firstkey"), commandInfoTestInt(1),
				commandInfoTestBulk("keystep"), commandInfoTestInt(2),
			),
		),
	)
	entry := commandInfoTestEntry10(
		"numeric", commandInfoTestArray(commandInfoTestBulk("readonly")),
		commandInfoTestArray(), commandInfoTestArray(indexRange, keywordKeynum), commandInfoTestArray(),
	)
	info := commandInfoTestReadReply(t, commandInfoTestArray(entry)).val["numeric"]
	if info == nil || len(info.KeySpecs) != 2 {
		t.Fatalf("numeric record did not parse: %+v", info)
	}
	rangeSpec, keynumSpec := info.KeySpecs[0], info.KeySpecs[1]
	if rangeSpec.Index != 1 || rangeSpec.LastKey != 0 || rangeSpec.KeyStep != 1 || rangeSpec.Limit != 0 {
		t.Errorf("range fields = %+v", rangeSpec)
	}
	if keynumSpec.Keyword != "KEYS" || keynumSpec.StartFrom != 2 || keynumSpec.KeyNumIdx != 0 ||
		keynumSpec.FirstKey != 1 || keynumSpec.KeyStep != 2 {
		t.Errorf("keynum fields = %+v", keynumSpec)
	}
}

func TestCommandsInfoClonePreservesMalformedTombstone(t *testing.T) {
	cmd := &CommandsInfoCmd{val: map[string]*CommandInfo{
		"bad":  nil,
		"good": {Name: "good"},
	}, legacyRecords: map[string]struct{}{"good": {}}}
	clone := cmd.Clone().(*CommandsInfoCmd)
	if got, exists := clone.val["bad"]; !exists || got != nil {
		t.Fatalf("cloned tombstone = %#v, exists=%v", got, exists)
	}
	if clone.val["good"] == nil || clone.val["good"] == cmd.val["good"] {
		t.Fatal("valid record was not deep-cloned")
	}
	if _, ok := clone.legacyRecords["good"]; !ok {
		t.Fatal("legacy wire-shape provenance was not cloned")
	}
	delete(clone.legacyRecords, "good")
	if _, ok := cmd.legacyRecords["good"]; !ok {
		t.Fatal("clone shares its legacy provenance map with the original")
	}
}
