package redis

import (
	"context"
	"sync"
	"testing"
)

// TestTxPipelineRestoresNoRetryForDuplicateCommand pins that the temporary
// NoRetry marker TxPipeline sets on the wrapped batch is restored correctly even
// when the SAME Cmder is queued twice. The second occurrence snapshots the
// already-set marker, so a forward-order restore would end on prev=true and
// leave the shared command permanently non-retryable for later ordinary use.
func TestTxPipelineRestoresNoRetryForDuplicateCommand(t *testing.T) {
	core := newRemovedActiveCore(t) // exec returns fast; the deferred restore still runs
	mdb := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex)}
	mdb.initHooks(hooks{
		process:    core.process,
		pipeline:   core.processPipeline,
		txPipeline: core.processTxPipeline,
	})

	cmd := NewStatusCmd(context.Background(), "set", "k", "v")
	if cmd.NoRetry() {
		t.Fatal("precondition: fresh command must be retryable")
	}
	pipe := mdb.TxPipeline()
	_ = pipe.Process(context.Background(), cmd)
	_ = pipe.Process(context.Background(), cmd) // same object queued twice
	_, _ = pipe.Exec(context.Background())      // error expected; restore must still run

	if cmd.NoRetry() {
		t.Fatal("NoRetry left set on a command queued twice in a TxPipeline — a later ordinary request would silently lose retries")
	}
}
