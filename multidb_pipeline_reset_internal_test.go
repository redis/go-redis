package redis

import (
	"context"
	"testing"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestProcessPipelineReusedCmdErrorsDoNotMaskTransportFailure pins the reset
// before execution. A caller may hand the batch a command that still carries a
// Redis error from a prior use. When connection acquisition then fails before
// anything executes, the stale per-command errors must not mask the transport
// failure: setCmdsErr fills only empty slots, so without the reset it never
// stamps the batch error, every command classifies as an unexecuted success,
// transportFailures stays 0, and the outage is neither recorded on the detector
// nor failed over. The reset clears the stale errors so the connection-acquire
// failure is recorded.
func TestProcessPipelineReusedCmdErrorsDoNotMaskTransportFailure(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det, CommandRetries: 0})
	dead := NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer dead.Close()
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, c: dead, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	})}
	core.active.Store(0)

	cmds := []Cmder{
		NewStatusCmd(context.Background(), "set", "k", "v"),
		NewStatusCmd(context.Background(), "get", "k"),
	}
	// Reused commands still carrying a Redis error from a prior batch.
	for _, cmd := range cmds {
		cmd.SetErr(proto.RedisError("ERR from a prior use"))
	}

	_ = core.processPipeline(context.Background(), cmds)

	if det.failures == 0 {
		t.Fatalf("transport failure not recorded: stale per-command errors masked the connection-acquire failure, so no failover would trigger")
	}
}

// TestProcessTxPipelineReusedCmdErrorsDoNotMaskTransportFailure is the tx
// analogue: processTxPipeline classifies only the user slice inside the
// MULTI/EXEC envelope, and a reused user command carrying a stale Redis error
// would otherwise mask a connection-acquire failure exactly as in the plain
// pipeline path. The reset targets the user slice; the envelope is clean.
func TestProcessTxPipelineReusedCmdErrorsDoNotMaskTransportFailure(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det, CommandRetries: 0})
	dead := NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer dead.Close()
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, c: dead, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	})}
	core.active.Store(0)

	// [MULTI, set, get, EXEC]: the user slice is the middle two commands, and
	// they are the reused ones carrying a stale Redis error.
	multi := NewStatusCmd(context.Background(), "multi")
	exec := NewSliceCmd(context.Background(), "exec")
	set := NewStatusCmd(context.Background(), "set", "k", "v")
	get := NewStatusCmd(context.Background(), "get", "k")
	set.SetErr(proto.RedisError("ERR from a prior use"))
	get.SetErr(proto.RedisError("ERR from a prior use"))

	_ = core.processTxPipeline(context.Background(), []Cmder{multi, set, get, exec})

	if det.failures == 0 {
		t.Fatalf("transport failure not recorded: stale user-command errors masked the connection-acquire failure inside the transaction")
	}
}
