package redis

import (
	"context"
	"errors"
)

// Batch submission on the full-duplex wire.
//
// WHY THIS EXISTS. AutoPipeliner.Pipeline() delegates to the underlying
// client, so a pipeline runs through processPipeline on a POOLED connection and
// never touches the full-duplex engine — the engine's only entry point is
// submit(), which takes one Cmder. That leaves go-redis without the shape
// rueidis offers as DoMulti: hand over N commands at once, block until every
// reply lands, on ONE multiplexed connection, spending no extra goroutines.
//
// The submit queue already has the primitive for it. fdQueue.pushBatch appends
// a whole slice under one lock, and because the queue is FIFO and the writer can
// only take contiguous prefixes, the commands stay ADJACENT on the wire — no
// other caller's command can land between them. That is a stronger guarantee
// than issuing N single commands gives, and it is what makes this a pipeline
// rather than a loop.
//
// Each command still carries its OWN completion batch. One shared batch would
// complete on the first reply, since the reader closes the batch per command, so
// the caller would return before the rest had landed.

// ErrFDPipelineUnavailable is returned when batch submission is requested on an
// autopipeliner that has no full-duplex engine. It is deliberately an error
// rather than a silent fallback to the pooled pipeline path: a caller asking for
// this wants the commands on the FD wire, and quietly running them somewhere
// else would be measured (or relied upon) as something it is not.
var ErrFDPipelineUnavailable = errors.New(
	"redis: FDPipelined requires an autopipeliner with FullDuplex enabled")

// ErrFDPipelineDiverts is returned when a batch contains a command the engine
// cannot stream (blocking, per-command read timeout, runs-outside-pipeline,
// HIMPORT, or one the caller's own mustDivert rejects). Such a command has to
// leave the pipe, which would break the batch's contiguity, so the whole call is
// refused instead of being silently split.
var ErrFDPipelineDiverts = errors.New(
	"redis: FDPipelined got a command that must be diverted off the full-duplex pipe")

// FDPipelined submits cmds as one contiguous batch on the full-duplex
// connection and blocks until every reply has landed. It returns the first
// command error, ignoring Nil the way Pipeline.Exec does.
//
// Compared with the two shapes that already exist:
//
//   - Pipeline().Exec() batches up front and blocks, but on a pooled
//     connection: a separate socket, its own round trip, no FD engine.
//   - Submit()/the async face keeps N commands in flight on the FD wire with
//     individual completion, but the caller issues them one at a time and they
//     may interleave with other callers' commands.
//
// This is the third: batched up front, contiguous, blocking, on the FD wire.
//
// The commands are still completed individually by the reader, so after this
// returns each cmd carries its own result and error.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (ap *AutoPipeliner) FDPipelined(ctx context.Context, cmds []Cmder) error {
	if len(cmds) == 0 {
		return nil
	}
	if ap.fd == nil {
		return ErrFDPipelineUnavailable
	}
	// Refuse a batch containing anything that would leave the pipe. Checked for
	// EVERY command before anything is submitted, so the call either goes as one
	// contiguous unit or not at all — a half-submitted batch would have the
	// diverted command complete out of order relative to its neighbours.
	for _, cmd := range cmds {
		if cmd == nil {
			return ErrFDPipelineDiverts
		}
		if cmd.readTimeout() != nil || runsOutsidePipeline(cmd.Name()) ||
			isBlockingCmd(cmd) || isHImportCmd(cmd) ||
			(ap.mustDivert != nil && ap.mustDivert(ctx, cmd)) {
			return ErrFDPipelineDiverts
		}
	}
	// The WHOLE batch goes to ONE engine, chosen from its first command. A
	// pipeline split across engines would lose its internal order, which is the
	// one thing a pipeline guarantees.
	batches, err := ap.fdFor(cmds[0]).submitBatch(ctx, cmds)
	if err != nil {
		return err
	}
	if batches == nil {
		// Submit-time rejection (closed, or ctx expired while backpressured).
		// Every command carries its own error; report the first.
		return firstCmdErr(cmds)
	}
	// Mirror submit()'s contract on the deferred face: the batch is installed on
	// the command so its result accessors self-gate, which matters for a caller
	// that keeps the Cmder after this returns.
	if !ap.blocking {
		for i, cmd := range cmds {
			cmd.setReady(batches[i])
		}
	}
	var first error
	for i, cmd := range cmds {
		// AutoFuture.Wait carries the executor-goroutine self-deadlock guard, so
		// waiting through it rather than on batch.done keeps a pipeline hook that
		// calls this from behaving differently than it does elsewhere.
		if werr := (AutoFuture{cmd: cmd, batch: batches[i]}).Wait(); werr != nil &&
			!errors.Is(werr, Nil) && first == nil {
			first = werr
		}
	}
	return first
}

// firstCmdErr reports the first non-Nil error across cmds.
func firstCmdErr(cmds []Cmder) error {
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil && !errors.Is(err, Nil) {
			return err
		}
	}
	return nil
}

// submitBatch enqueues cmds as one contiguous run and returns their completion
// batches, one per command. A nil slice with a nil error means submit-time
// rejection, with each command's own error already set (same convention as
// submit()'s completedBatch return).
//
// Admission is all-or-nothing: pushBatch either takes the whole slice or takes
// none of it, so the run cannot be split across two waves by a queue that fills
// halfway through.
func (fd *fdEngine) submitBatch(ctx context.Context, cmds []Cmder) ([]*apBatch, error) {
	// A batch larger than the queue itself can NEVER be admitted, and the
	// room-wait loop below would spin forever waiting for space that cannot
	// exist. Report it instead of hanging: the caller can split, or raise
	// FullDuplexWindow.
	if n := fd.q.capacity(); n > 0 && len(cmds) > n {
		return nil, ErrFDPipelineTooLarge
	}
	if fd.ap.isClosed() {
		setCmdsErr(cmds, ErrClosed)
		return nil, nil
	}
	// Hooks are per COMMAND on this engine (withProcessHook, not the batch
	// pipeline hook), matching submit(): a batch submitted here is reported as N
	// individual commands, exactly as if they had been submitted one by one.
	hooked := fd.ap.pipeliner.hookCount() > 0

	reqs := make([]fdReq, len(cmds))
	batches := make([]*apBatch, len(cmds))
	var hookDones []chan struct{}
	if hooked {
		hookDones = make([]chan struct{}, len(cmds))
	}
	for i, cmd := range cmds {
		b := newAPBatch() // never pooled: pooled batches are the blocking face's single-waiter signal
		batches[i] = b
		var hd chan struct{}
		if hooked {
			hd = make(chan struct{})
			hookDones[i] = hd
		}
		reqs[i] = fdReq{cmd: cmd, batch: b, hookDone: hd, ctx: ctx, attempts: 1}
	}

	fd.submitMu.RLock()
	for {
		if fd.closed {
			fd.submitMu.RUnlock()
			setCmdsErr(cmds, ErrClosed)
			return nil, nil
		}
		switch fd.q.pushBatch(reqs) {
		case fdPushOK:
			// Hosts start only after admission, so a rejected batch never leaks
			// goroutines. Under the gate, so the Add is ordered before the shutdown
			// drain's WLock.
			if hooked {
				for i := range cmds {
					fd.hostWg.Add(1)
					go fd.hostHook(ctx, cmds[i], batches[i], hookDones[i])
				}
			}
			fd.submitMu.RUnlock()
			return batches, nil
		case fdPushClosed:
			fd.submitMu.RUnlock()
			setCmdsErr(cmds, ErrClosed)
			return nil, nil
		}
		// Queue full. Wait for the writer to take a wave, then retry the whole
		// batch. Same release conditions as submit()'s backpressure wait.
		select {
		case <-fd.q.roomCh():
			if fd.q.depth() < fd.q.capacity() {
				fd.q.signalRoom()
			}
		case <-ctx.Done():
			fd.submitMu.RUnlock()
			setCmdsErr(cmds, ctx.Err())
			return nil, nil
		case <-fd.ap.ctx.Done():
			fd.submitMu.RUnlock()
			setCmdsErr(cmds, ErrClosed)
			return nil, nil
		}
	}
}

// ErrFDPipelineTooLarge is returned when a batch cannot fit the submit queue
// even when the queue is empty.
var ErrFDPipelineTooLarge = errors.New(
	"redis: FDPipelined batch is larger than the full-duplex submit queue")
