package redis

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

func TestRecordBatchOutcomesPostExecHookError(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
	})}

	cmds := []Cmder{NewStatusCmd(context.Background(), "set", "k", "v")}

	// Executed batch, every reply read fine, then a post-exec hook injected
	// a retryable error without stamping the commands: the commands are
	// authoritative — no phantom failures, no stamping, no replay signal.
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, true); got != 0 {
		t.Errorf("transportFailures = %d for an executed all-success batch, want 0", got)
	}
	if err := cmds[0].Err(); err != nil {
		t.Errorf("executed batch had its successful command stamped with %v", err)
	}

	// Not executed (hook aborted before next): the batch error stands in
	// for the commands and is stamped so callers see it.
	resetCmds(cmds)
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, false); got != 1 {
		t.Errorf("transportFailures = %d for an unexecuted batch, want 1", got)
	}
	if err := cmds[0].Err(); err == nil {
		t.Error("unexecuted batch left the command unstamped")
	}
}

func TestMarkPipelineExecuted(t *testing.T) {
	flag := new(atomic.Bool)
	markPipelineExecuted(context.WithValue(context.Background(), pipelineExecutedKey{}, flag))
	if !flag.Load() {
		t.Error("marker did not flip the flag")
	}
	// Without a marker in the context it must be a no-op, not a panic.
	markPipelineExecuted(context.Background())
}
