# Module commands (RediSearch / TimeSeries / VectorSet / Bloom)

Module subcommands (`FT.*`, `TS.*`, `V*`, `BF.*`/`CF.*`) follow the core pattern but with three differences. Read `references/core-command-pattern.md` first. Worked example: **FTHybrid** (`search_commands.go`).

## 1. Where the spec lives

Module command specs are **not** in `redis/redis/src/commands/`. They live in the module's own repo:
- RediSearch → `RediSearch/RediSearch`
- TimeSeries → `RedisTimeSeries/RedisTimeSeries`
- VectorSet / Bloom → their respective repos

If Step 0's redis/redis fetch 404s, the command is a module command — get the spec from the module repo or ask the user for the spec file / PR.

## 2. Where the Cmder type lives

Module Cmd types are defined in the **module's** `*_commands.go` file, not `command.go`. `FTHybridCmd` lives in `search_commands.go` (grep `type FTHybridCmd`), registered on the `SearchCmdable` interface at the top of that file, which is embedded in `Cmdable` (`commands.go`). TimeSeries → `timeseries_commands.go`, VectorSet → `vectorset_commands.go`. The required-method set is identical to core (SetVal/Val/Result/String/readReply/Clone).

## 3. RESP2 vs RESP3 reply shapes

Module commands frequently return a **map in RESP3** and a **flat array in RESP2** for the same logical reply. The parser must handle both. FTHybrid reads the raw slice, then `parseFTHybrid` walks it tolerating three shapes:

```go
func (cmd *FTHybridCmd) readReply(rd *proto.Reader) (err error) {
	data, err := rd.ReadSlice()   // generic slice read
	if err != nil {
		return err
	}
	result, cursorResult, err := parseFTHybrid(data, cmd.withCursor)
	// ...assign cmd.val / cmd.cursorVal...
}

// inside parseFTHybrid, per result item:
if itemMap, ok := item.(map[string]interface{}); ok {      // RESP3 map
	results = append(results, itemMap)
} else if rawMap, ok := item.(map[interface{}]interface{}); ok {  // alt RESP3
	// stringify keys
} else if itemData, ok := item.([]interface{}); ok {       // RESP2 array of k/v pairs
	// walk pairs
}
```

Pattern: don't assume RESP3. Try the map shape, fall back to the array (key-value pairs) shape. Tests run RESP3 by default but RESP2 must still parse for older servers.

## 4. Common module conventions

- Subcommand keyword is uppercase in args (`"SEARCH"`, `"VSIM"`, `"WITHCURSOR"`); the command token is lowercase (`"ft.hybrid"`).
- Cursor-style commands carry parse state on the Cmd struct (FTHybrid's `withCursor`, `cursorVal`) — set it in the constructor from the options.
- Module commands are almost always `FooWithArgs(ctx, index, *FooOptions)` plus a thin positional `Foo(...)`.
- Version-gate integration tests against the **module** version, not the core Redis version.
