package redis

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

// autoPipelineRing is a pre-allocated ring buffer queue for autopipelining.
// It provides lock-free enqueue and FIFO ordering guarantees.
//
// Ring buffer architecture:
// - Pre-allocated slots (no allocations during enqueue)
// - Per-slot channels for request-response matching
// - Atomic write pointer for lock-free enqueue
// - Separate read pointers for write and read goroutines
//
// The ring buffer uses three pointers:
// - write: Where app goroutines add commands (atomic increment)
// - read1: Where flush goroutine reads commands to send
// - read2: Where result goroutine matches responses (currently unused, for future optimization)
type autoPipelineRing struct {
	store     []autoPipelineSlot // Pre-allocated slots
	mask      uint32             // Size - 1 (for fast modulo via bitwise AND)
	write     uint32             // Write position (atomic, incremented by app goroutines)
	read1     uint32             // Read position for flush goroutine
	read2     uint32             // Read position for result matching (reserved for future use)
	cmds      []Cmder            // Persistent buffer for collecting commands (reused, no allocations)
	doneChans []chan struct{}    // Persistent buffer for collecting done channels (reused, no allocations)
}

// autoPipelineSlot represents a single command slot in the ring buffer.
type autoPipelineSlot struct {
	c1    *sync.Cond     // Condition variable for write synchronization (shared mutex with c2)
	c2    *sync.Cond     // Condition variable for wait/signal (shared mutex with c1)
	cmd   Cmder          // The command to execute
	done  chan struct{}  // Completion notification channel (pre-allocated, reused)
	mark  uint32         // State: 0=empty, 1=queued, 2=sent (atomic)
	slept bool           // Whether writer goroutine is sleeping on this slot
}

// State constants for autoPipelineSlot.mark
const (
	apSlotEmpty  uint32 = 0 // Slot is empty and available
	apSlotQueued uint32 = 1 // Command queued, ready to be sent
	apSlotSent   uint32 = 2 // Command sent, waiting for response
	apSlotClosed uint32 = 3 // Ring is closed, stop waiting
)

// newAutoPipelineRing creates a new ring buffer with the specified size.
// Size will be rounded up to the next power of 2 for efficient modulo operations.
func newAutoPipelineRing(size int) *autoPipelineRing {
	// Round up to power of 2 for fast modulo via bitwise AND
	if size <= 0 {
		size = 1024 // Default size
	}
	if size&(size-1) != 0 {
		// Not a power of 2, round up
		size = 1 << (32 - bits.LeadingZeros32(uint32(size)))
	}

	r := &autoPipelineRing{
		store:     make([]autoPipelineSlot, size),
		mask:      uint32(size - 1),
		cmds:      make([]Cmder, 0, size),         // Persistent buffer, reused
		doneChans: make([]chan struct{}, 0, size), // Persistent buffer, reused
	}

	// Initialize each slot with condition variables and pre-allocated channel
	for i := range r.store {
		m := &sync.Mutex{}
		r.store[i].c1 = sync.NewCond(m)
		r.store[i].c2 = sync.NewCond(m) // Share the same mutex
		r.store[i].done = make(chan struct{}, 1) // Buffered channel for signal (not close)
	}

	return r
}

// putOne enqueues a command into the ring buffer.
// Returns the done channel that will be signaled when the command completes.
//
// Ring buffer enqueue implementation:
// - Atomic increment for write position
// - Wait on condition variable if slot is full
// - Signal reader if it's sleeping
func (r *autoPipelineRing) putOne(cmd Cmder) <-chan struct{} {
	// Atomic increment to get next slot
	slot := &r.store[atomic.AddUint32(&r.write, 1)&r.mask]

	// Lock the slot
	slot.c1.L.Lock()

	// Wait if slot is not empty (mark != 0)
	for slot.mark != 0 {
		slot.c1.Wait()
	}

	// Store command and mark as queued
	slot.cmd = cmd
	slot.mark = 1
	s := slot.slept

	slot.c1.L.Unlock()

	// If reader is sleeping, wake it up
	if s {
		slot.c2.Broadcast()
	}

	return slot.done
}

// nextWriteCmd tries to get the next command (non-blocking).
// Returns nil if no command is available.
// Should only be called by the flush goroutine.
func (r *autoPipelineRing) nextWriteCmd() (Cmder, chan struct{}) {
	r.read1++
	p := r.read1 & r.mask
	slot := &r.store[p]

	slot.c1.L.Lock()
	if slot.mark == 1 {
		cmd := slot.cmd
		done := slot.done
		slot.mark = 2
		slot.c1.L.Unlock()
		return cmd, done
	}
	// No command available, rollback read position
	r.read1--
	slot.c1.L.Unlock()
	return nil, nil
}

// waitForWrite waits for the next command (blocking).
// Should only be called by the flush goroutine.
// Returns nil if the ring is closed.
func (r *autoPipelineRing) waitForWrite() (Cmder, chan struct{}) {
	r.read1++
	p := r.read1 & r.mask
	slot := &r.store[p]

	slot.c1.L.Lock()
	// Wait until command is available (mark == 1) or closed (mark == 3)
	for slot.mark != 1 && slot.mark != apSlotClosed {
		slot.slept = true
		slot.c2.Wait() // c1 and c2 share the same mutex
		slot.slept = false
	}

	// Check if closed
	if slot.mark == apSlotClosed {
		r.read1-- // Rollback read position
		slot.c1.L.Unlock()
		return nil, nil
	}

	cmd := slot.cmd
	done := slot.done
	slot.mark = 2
	slot.c1.L.Unlock()
	return cmd, done
}

// finishCmd marks a command as completed and clears the slot.
// Should only be called by the flush goroutine.
func (r *autoPipelineRing) finishCmd() {
	r.read2++
	p := r.read2 & r.mask
	slot := &r.store[p]

	slot.c1.L.Lock()
	if slot.mark == 2 {
		// Drain the done channel before reusing
		select {
		case <-slot.done:
		default:
		}

		// Clear slot for reuse
		slot.cmd = nil
		slot.mark = 0
	}
	slot.c1.L.Unlock()
	slot.c1.Signal() // Wake up any writer waiting on this slot
}

// len returns the approximate number of queued commands.
// This is an estimate and may not be exact due to concurrent access.
func (r *autoPipelineRing) len() int {
	write := atomic.LoadUint32(&r.write)
	read := atomic.LoadUint32(&r.read1)
	
	// Handle wrap-around
	if write >= read {
		return int(write - read)
	}
	// Wrapped around
	return int(write + (^uint32(0) - read) + 1)
}

// cap returns the capacity of the ring buffer.
func (r *autoPipelineRing) cap() int {
	return len(r.store)
}

// reset resets the ring buffer to empty state.
// This should only be called when no goroutines are accessing the ring.
func (r *autoPipelineRing) reset() {
	atomic.StoreUint32(&r.write, 0)
	atomic.StoreUint32(&r.read1, 0)
	atomic.StoreUint32(&r.read2, 0)

	for i := range r.store {
		r.store[i].c1.L.Lock()
		r.store[i].cmd = nil
		r.store[i].mark = 0
		r.store[i].slept = false
		r.store[i].c1.L.Unlock()
	}
}

// wakeAll wakes up all waiting goroutines.
// This is used during shutdown to unblock the flusher.
func (r *autoPipelineRing) wakeAll() {
	for i := range r.store {
		r.store[i].c1.L.Lock()
		if r.store[i].mark == 0 {
			r.store[i].mark = apSlotClosed
		}
		r.store[i].c1.L.Unlock()
		r.store[i].c2.Broadcast()
	}
}

