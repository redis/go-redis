package redis

import "context"

// The HIMPORT command family (Redis 8.10+, "hinted hash templates") provides
// fast ingestion of many hashes sharing the same field names. HIMPORT PREPARE
// registers the field names once under a fieldset name, then HIMPORT SET
// creates hashes by sending only the values.
//
// The server scopes a fieldset to the physical connection that prepared it.
// Because go-redis pools connections, the client additionally keeps a
// client-side registry of fieldsets registered through HImportPrepare and
// lazily replays the PREPARE (at most once per connection session) on any
// pooled connection about to execute an HImportSet that references it. See
// himport.go.

// himportCmder marks HIMPORT commands that participate in client-side
// fieldset tracking. Process paths do a single interface assertion on the
// hot path and inspect the concrete type only for HIMPORT commands.
type himportCmder interface {
	Cmder
	himportCmd()
}

var (
	_ himportCmder = (*HImportPrepareCmd)(nil)
	_ himportCmder = (*HImportSetCmd)(nil)
	_ himportCmder = (*HImportDiscardCmd)(nil)
	_ himportCmder = (*HImportDiscardAllCmd)(nil)
)

// HImportPrepareCmd represents an HIMPORT PREPARE command.
type HImportPrepareCmd struct {
	StatusCmd

	fieldsetName string
	fields       []string

	// registryVersion is non-zero only on commands injected by the client to
	// replay a registered fieldset onto a connection; on success the
	// connection is marked as prepared at this version.
	registryVersion uint64
}

func (cmd *HImportPrepareCmd) himportCmd() {}

// NewHImportPrepareCmd returns an HIMPORT PREPARE command.
func NewHImportPrepareCmd(ctx context.Context, fieldsetName string, fields ...string) *HImportPrepareCmd {
	args := make([]interface{}, 3+len(fields))
	args[0] = "himport"
	args[1] = "prepare"
	args[2] = fieldsetName
	for i, field := range fields {
		args[3+i] = field
	}
	return &HImportPrepareCmd{
		StatusCmd: StatusCmd{
			baseCmd: baseCmd{
				ctx:     ctx,
				args:    args,
				cmdType: CmdTypeStatus,
			},
		},
		fieldsetName: fieldsetName,
		fields:       append([]string(nil), fields...),
	}
}

// HImportSetCmd represents an HIMPORT SET command.
type HImportSetCmd struct {
	StatusCmd

	fieldsetName string
}

func (cmd *HImportSetCmd) himportCmd() {}

// NewHImportSetCmd returns an HIMPORT SET command.
func NewHImportSetCmd(ctx context.Context, key, fieldsetName string, values ...interface{}) *HImportSetCmd {
	args := make([]interface{}, 4+len(values))
	args[0] = "himport"
	args[1] = "set"
	args[2] = key
	args[3] = fieldsetName
	copy(args[4:], values)
	cmd := &HImportSetCmd{
		StatusCmd: StatusCmd{
			baseCmd: baseCmd{
				ctx:     ctx,
				args:    args,
				cmdType: CmdTypeStatus,
			},
		},
		fieldsetName: fieldsetName,
	}
	cmd.SetFirstKeyPos(2)
	return cmd
}

// HImportDiscardCmd represents an HIMPORT DISCARD command.
type HImportDiscardCmd struct {
	IntCmd

	fieldsetName string
}

func (cmd *HImportDiscardCmd) himportCmd() {}

// NewHImportDiscardCmd returns an HIMPORT DISCARD command.
func NewHImportDiscardCmd(ctx context.Context, fieldsetName string) *HImportDiscardCmd {
	return &HImportDiscardCmd{
		IntCmd: IntCmd{
			baseCmd: baseCmd{
				ctx:     ctx,
				args:    []interface{}{"himport", "discard", fieldsetName},
				cmdType: CmdTypeInt,
			},
		},
		fieldsetName: fieldsetName,
	}
}

// HImportDiscardAllCmd represents an HIMPORT DISCARDALL command.
type HImportDiscardAllCmd struct {
	IntCmd
}

func (cmd *HImportDiscardAllCmd) himportCmd() {}

// NewHImportDiscardAllCmd returns an HIMPORT DISCARDALL command.
func NewHImportDiscardAllCmd(ctx context.Context) *HImportDiscardAllCmd {
	return &HImportDiscardAllCmd{
		IntCmd: IntCmd{
			baseCmd: baseCmd{
				ctx:     ctx,
				args:    []interface{}{"himport", "discardall"},
				cmdType: CmdTypeInt,
			},
		},
	}
}

// HImportPrepare registers an ordered list of hash field names under
// fieldsetName for use by subsequent HImportSet calls:
//
//	HIMPORT PREPARE fieldset_name field [field ...]
//
// The server keeps the fieldset in the session of the connection that
// executed the command. On pooled clients (Client, Conn, Pipeline, Tx) the
// fieldset is also remembered client-side and the PREPARE is replayed
// lazily — at most once per connection session — on any pooled connection
// about to execute an HImportSet referencing it, so HImportSet works
// transparently across the pool. Preparing an existing fieldset name again
// silently replaces it.
//
// ClusterClient and Ring do not fan out the PREPARE to all nodes: the
// registry is scoped to the node client that executed it. For cluster use,
// prepare and set on a dedicated connection per node.
//
// Requires Redis 8.10 or newer.
func (c cmdable) HImportPrepare(ctx context.Context, fieldsetName string, fields ...string) *StatusCmd {
	cmd := NewHImportPrepareCmd(ctx, fieldsetName, fields...)
	_ = c(ctx, cmd)
	return &cmd.StatusCmd
}

// HImportSet creates or fully replaces the hash at key using the field list
// registered under fieldsetName, pairing values positionally with the
// prepared fields:
//
//	HIMPORT SET key fieldset_name value [value ...]
//
// The number of values must equal the fieldset's field count. The resulting
// key is a regular hash readable and writable by all hash commands. If the
// fieldset was registered through HImportPrepare on this client, it is
// prepared automatically on whichever pooled connection executes the command;
// otherwise the fieldset must have been prepared on the executing connection
// or the server replies "ERR no such fieldset".
//
// A single-command HImportSet that fails with "no such fieldset" for a
// registered fieldset (the connection lost its session state, e.g. RESET) is
// transparently re-prepared and retried once. Pipelined sets are not
// auto-retried — re-running the batch would repeat side effects of commands
// that already succeeded — but the connection's prepared flags are
// invalidated, so retrying the pipeline replays the PREPARE and succeeds.
//
// Requires Redis 8.10 or newer.
func (c cmdable) HImportSet(ctx context.Context, key, fieldsetName string, values ...interface{}) *StatusCmd {
	cmd := NewHImportSetCmd(ctx, key, fieldsetName, values...)
	_ = c(ctx, cmd)
	return &cmd.StatusCmd
}

// HImportDiscard removes fieldsetName from the executing connection's session
// and from the client-side registry, stopping further automatic replay:
//
//	HIMPORT DISCARD fieldset_name
//
// It returns 1 if the fieldset was removed from the executing connection's
// session and 0 if that connection had no fieldset with this name. Other
// pooled connections may retain the fieldset server-side until they close;
// hashes already created through the fieldset are not affected.
//
// Requires Redis 8.10 or newer.
func (c cmdable) HImportDiscard(ctx context.Context, fieldsetName string) *IntCmd {
	cmd := NewHImportDiscardCmd(ctx, fieldsetName)
	_ = c(ctx, cmd)
	return &cmd.IntCmd
}

// HImportDiscardAll removes all fieldsets from the executing connection's
// session and clears the client-side registry:
//
//	HIMPORT DISCARDALL
//
// It returns the number of fieldsets removed from the executing connection's
// session.
//
// Requires Redis 8.10 or newer.
func (c cmdable) HImportDiscardAll(ctx context.Context) *IntCmd {
	cmd := NewHImportDiscardAllCmd(ctx)
	_ = c(ctx, cmd)
	return &cmd.IntCmd
}
