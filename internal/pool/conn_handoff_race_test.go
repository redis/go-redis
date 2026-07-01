package pool

import (
	"sync"
	"testing"
)

// TestMarkQueuedForHandoffRollbackDoesNotResurrectClearedState is a regression
// test for the handoff CAS-rollback race.
//
// When MarkQueuedForHandoff cannot transition the connection to UNUSABLE it
// rolls back the handoff metadata it just CAS'd. A concurrent handoff worker
// may have completed the handoff and run ClearHandoffState in that exact
// window. The rollback must NOT clobber the worker's cleared state by
// resurrecting ShouldHandoff=true — doing so wedges the connection forever,
// because OnGet rejects any connection reporting ShouldHandoff.
//
// The offending interleaving (the clear landing between the metadata CAS and
// the rollback store) is timing-dependent, so this hammers it many times. With
// the previous plain-Store rollback a resurrected ShouldHandoff is observed
// within these iterations; with the CAS rollback it never is.
func TestMarkQueuedForHandoffRollbackDoesNotResurrectClearedState(t *testing.T) {
	iterations := 20000
	if testing.Short() {
		iterations = 2000
	}

	for i := 0; i < iterations; i++ {
		cn := NewConn(nil)
		if err := cn.MarkForHandoff("new-endpoint:6379", int64(i+1)); err != nil {
			t.Fatalf("iter %d: MarkForHandoff: %v", i, err)
		}
		// Force MarkQueuedForHandoff down its rollback path: StateClosed is not a
		// valid source for the UNUSABLE transition, so TryTransition fails and the
		// metadata rollback runs.
		cn.GetStateMachine().Transition(StateClosed)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = cn.MarkQueuedForHandoff()
		}()
		go func() {
			defer wg.Done()
			<-start
			// Represents a worker that finished the handoff and cleared state.
			cn.ClearHandoffState()
		}()
		close(start)
		wg.Wait()

		// ClearHandoffState always runs, so the connection's final handoff state
		// must be cleared. A resurrected ShouldHandoff means the rollback
		// clobbered the worker's clear.
		if cn.ShouldHandoff() {
			t.Fatalf("iter %d: ShouldHandoff resurrected after ClearHandoffState — rollback clobbered the cleared state", i)
		}
	}
}
