package redis

import "context"

// Co-allocating a command with its argument buffer.
//
// A two-argument command like GET costs TWO allocations: the command struct,
// and the []interface{} that carries its arguments. The second is pure
// overhead — the arguments are written once at construction and live exactly as
// long as the command — so putting a small buffer in the same heap object
// removes it. An allocation profile of a 1024-caller full-duplex run put
// cmdable.Get alone at 34% of all objects allocated, most of it this slice.
//
// The buffer lives in a wrapper struct rather than in baseCmd on purpose:
// baseCmd is embedded in every command type, so a buffer there would grow all
// of them by its full size, and most command types would never use it.
//
// SAFETY: args aliases memory inside the same heap object as the command, so
// neither can outlive the other. That is sound only because no command is ever
// copied by value — baseCmd appears exclusively as an embedded field, never as
// a value parameter and never as a dereference copy. A by-value copy would
// leave the copy's args pointing into the original's buffer. Keep it that way.
//
// Appending past a buffer's capacity is still correct: append then moves the
// arguments to the heap exactly as before.

// stringCmdInline is a StringCmd plus room for two arguments, which is the
// shape of GET.
type stringCmdInline struct {
	StringCmd
	buf [2]interface{}
}

// newStringCmd2 builds a two-argument StringCmd in one allocation.
func newStringCmd2(ctx context.Context, a0, a1 interface{}) *StringCmd {
	w := &stringCmdInline{}
	w.buf[0] = a0
	w.buf[1] = a1
	w.ctx = ctx
	w.args = w.buf[:]
	w.cmdType = CmdTypeString
	return &w.StringCmd
}

// statusCmdInline is a StatusCmd plus room for five arguments: SET with an
// expiry option ("set" key value "px" ms) is the longest form Set builds.
type statusCmdInline struct {
	StatusCmd
	buf [5]interface{}
}

// newStatusCmdInline returns a StatusCmd co-allocated with an argument buffer,
// and an empty slice backed by that buffer for the caller to append into. The
// caller must assign the finished slice back to cmd.args.
func newStatusCmdInline(ctx context.Context) (*StatusCmd, []interface{}) {
	w := &statusCmdInline{}
	w.ctx = ctx
	w.cmdType = CmdTypeStatus
	return &w.StatusCmd, w.buf[:0]
}
