package redis

import (
	"context"
	"sync"
	"testing"
)

type noopRaceHook struct{}

func (noopRaceHook) DialHook(next DialHook) DialHook          { return next }
func (noopRaceHook) ProcessHook(next ProcessHook) ProcessHook { return next }
func (noopRaceHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// AddHook publishes a fresh hooks snapshot via hs.state, while the command-path
// getters (processHook, processPipelineHook, processTxPipelineHook), the slice
// readers (withProcessHook, withProcessPipelineHook) and the AddHook slice
// append all touch the same hook state. Exercise concurrent AddHook writers
// against both reader groups; run with -race to catch a torn read of the
// published snapshot or an unsynchronized slice append.
func TestHooksMixinCurrentConcurrent(t *testing.T) {
	var hs hooksMixin
	hs.initHooks(hooks{})

	ctx := context.Background()
	cmd := NewCmd(ctx, "ping")
	cmds := []Cmder{cmd}

	const writers = 2

	var wg sync.WaitGroup
	wg.Add(writers + 1)

	done := make(chan struct{})

	// Writers: concurrent AddHook calls, each replacing hs.state copy-on-write.
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					hs.AddHook(noopRaceHook{})
				}
			}
		}()
	}

	noopProcess := func(context.Context, Cmder) error { return nil }
	noopPipeline := func(context.Context, []Cmder) error { return nil }

	// Reader: the command-path getters Load hs.state lock-free; the slice
	// readers (withProcessHook, withProcessPipelineHook) iterate the published
	// snapshot's hook slice.
	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < 3000; i++ {
			_ = hs.processHook(ctx, cmd)
			_ = hs.processPipelineHook(ctx, cmds)
			_ = hs.processTxPipelineHook(ctx, cmds)
			_ = hs.withProcessHook(ctx, cmd, noopProcess)
			_ = hs.withProcessPipelineHook(ctx, cmds, noopPipeline)
		}
	}()

	wg.Wait()
}
