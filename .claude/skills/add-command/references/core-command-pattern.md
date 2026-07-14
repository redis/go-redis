# Core command pattern

The 7-step Go pattern for adding a command to the root `redis` package. Worked example: **LCS** (grep `func (c cmdable) LCS` in `string_commands.go` and `type LCSCmd struct` in `command.go`) — it uses a `cmdable` receiver, a query struct, a custom Cmder, and a registered `CmdType`. Copy its shape exactly.

## 1. Method on the `cmdable` interface

In the matching `*_commands.go` file. Receiver is `cmdable` — this is what makes the command available on every client type (`Client`, `ClusterClient`, `Ring`, sentinel, pipelines). A `*Client` receiver compiles but is unreachable via `UniversalClient` / `Cmdable`.

```go
func (c cmdable) LCS(ctx context.Context, q *LCSQuery) *LCSCmd {
	cmd := NewLCSCmd(ctx, q)
	_ = c(ctx, cmd)
	return cmd
}
```

For commands with many optional flags, expose `Foo(...)` for the common path plus `FooWithArgs(ctx, key, *FooArgs)` for the full surface (see `FTHybrid` / `FTHybridWithArgs` in `search_commands.go`).

Conventions:
- Lowercase the command name in the args slice (`"lcs"`, not `"LCS"`); subcommand flags are uppercase (`"LEN"`, `"IDX"`, `"MINMATCHLEN"`).
- Pre-size the args slice (`make([]interface{}, 3, 7)`).
- Convert `time.Duration` to whatever the server expects — never pass it raw (server gets nanoseconds). Use `formatMs` / `formatSec` in `commands.go`.
- Doc comment: state `Redis X.Y+` (from the spec `since`) and link the redis.io command page.

## 2. New `XxxCmdable` interface goes into `Cmdable`

Only if you added a **new** `*_commands.go` file with its own interface. Embed it in `Cmdable` (`commands.go`, alphabetical in the embed list — e.g. `SearchCmdable` at `commands.go:242`). Without this the method is unreachable through `UniversalClient`.

## 3. Custom Cmder type

Define only when no existing Cmd type (`*IntCmd`, `*StringCmd`, `*BoolCmd`, `*StringSliceCmd`, `*MapStringStringCmd`, …) fits the reply. Put it at the bottom of `command.go` (module commands may keep it in their own `*_commands.go`, like `FTHybridCmd`). Follow `LCSCmd`:

```go
type LCSCmd struct {
	baseCmd
	readType uint8      // command-specific parse state
	val      *LCSMatch
}

var _ Cmder = (*LCSCmd)(nil)

func NewLCSCmd(ctx context.Context, q *LCSQuery) *LCSCmd {
	args := make([]interface{}, 3, 7)
	args[0], args[1], args[2] = "lcs", q.Key1, q.Key2
	// ...append optional flags...
	cmd := &LCSCmd{readType: 1}
	cmd.baseCmd = baseCmd{ctx: ctx, args: args, cmdType: CmdTypeLCS}
	return cmd
}

func (cmd *LCSCmd) SetVal(val *LCSMatch)              { cmd.val = val }
func (cmd *LCSCmd) Val() *LCSMatch                    { return cmd.val }
func (cmd *LCSCmd) Result() (*LCSMatch, error)        { return cmd.val, cmd.err }
func (cmd *LCSCmd) String() string                    { return cmdString(cmd, cmd.val) }
func (cmd *LCSCmd) readReply(rd *proto.Reader) error  { /* parse RESP */ }
func (cmd *LCSCmd) Clone() Cmder                       { /* deep-copy val */ }
```

Required methods — the `setval` analyzer (`internal/customvet/checks/setval`) fails the build if `Result` exists without `SetVal`:
- `SetVal(...)` — hooks mutate results through it. (`SetErr` is inherited from `baseCmd`.)
- `Val()`, `Result()` — public accessors.
- `String()` — via `cmdString`.
- `readReply(rd *proto.Reader) error` — RESP parser. Use `rd.ReadArrayLen`, `rd.ReadInt`, `rd.ReadString`, `rd.ReadFloat`, `rd.ReadFixedMapLen`, `rd.ReadFixedArrayLen`. Validate element counts when the protocol guarantees a fixed shape. Always propagate the reader's error — never return `nil` after a partial read.
- `Clone()` — deep-copy `val` (including slices/maps) so pipeline/transaction reuse is safe.

**CmdType:** set `cmdType: CmdTypeXxx` in the constructor. If you add a new `CmdTypeXxx` constant (`command.go`), also add the matching case in the `ExtractCommandValue` switch further down `command.go` — this is what lets hooks read the typed value. Grep `CmdTypeLCS` to find both the const declaration and its `case` in `ExtractCommandValue`; copy that shape.

## 4. RESP3 vs RESP2

If the reply differs between protocols, the parser must handle both — this applies to **any** command with a protocol-dependent shape, core or module, not just modules. Typical case: a map in RESP3, a flat key/value array in RESP2. Branch on the reply type with `rd.PeekReplyType()` (check `proto.RespMap` for RESP3) or `rd.IsResp3()`. Tests run RESP3 by default but RESP2 must still parse for older servers. See `references/module-commands.md` for the FTHybrid worked example.

## 5. Tests — both layers

- **Unit test** in `xxx_commands_unit_test.go` (no Redis) — assert the args slice your method builds for the interesting flag combinations. Pattern: `search_commands_unit_test.go:120` captures `m.lastCmd.(*FTHybridCmd)` and checks args.
- **Integration test** in `xxx_commands_test.go` (Ginkgo, docker stack). Gate version-specific commands: `SkipBeforeRedisVersion(8.8, "X requires Redis 8.8+")` (version from spec `since`). Cover happy path, errors, optional-arg combos.

```sh
go test -run TestFTHybridWithArgsRejectsUnsupportedVectors .
go test -run TestGinkgoSuite . -ginkgo.focus="LCS"
```

## 6. Custom vet check

```sh
cd internal/customvet && go build .
go vet -vettool ./internal/customvet/customvet ./...
```

Same check CI runs at the end of `make test.ci`; catches a missing `SetVal`.

## 7. Format

```sh
make fmt
```

(`gofumpt` + `goimports -local github.com/redis/go-redis`.)

## Pitfalls

- Forgetting to embed the new `XxxCmdable` interface in `Cmdable` — compiles on `*Client`, silently unreachable via `UniversalClient`.
- Forgetting `Clone()` — pipelines reuse Cmders; shared `val` causes cross-execution bugs.
- Skipping `SetVal` because "nothing calls it" — hooks do, and custom-vet fails the build.
- Returning `nil` from `readReply` after a partial read — always propagate the reader's error.
- `time.Duration` passed raw — server gets nanoseconds; convert per spec.
- New `CmdTypeXxx` added but not registered in the type-routing switch in `command.go`.
