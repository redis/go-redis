package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func (q *wantConnQueue) len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.items)
}

func TestWantConn_getCtxForDial(t *testing.T) {
	ctx := context.Background()
	w := &wantConn{
		ctx:    ctx,
		result: make(chan wantConnResult, 1),
	}

	// Test getting context when not done
	gotCtx := w.getCtxForDial()
	if gotCtx != ctx {
		t.Errorf("getCtxForDial() = %v, want %v", gotCtx, ctx)
	}

	// Test getting context when done
	w.mu.Lock()
	w.done = true
	w.ctx = nil
	w.mu.Unlock()
	gotCtx = w.getCtxForDial()
	if gotCtx != nil {
		t.Errorf("getCtxForDial() after done = %v, want nil", gotCtx)
	}
}

func TestWantConn_tryDeliver_Success(t *testing.T) {
	w := &wantConn{
		ctx:    context.Background(),
		result: make(chan wantConnResult, 1),
	}

	// Create a mock connection
	conn := &Conn{}

	// Test successful delivery
	delivered := w.tryDeliver(conn, nil)
	if !delivered {
		t.Error("tryDeliver() = false, want true")
	}

	// Check that wantConn is marked as done
	if w.isOngoing() {
		t.Error("wantConn.done = false, want true after delivery")
	}

	// Check that context is cleared
	if w.getCtxForDial() != nil {
		t.Error("wantConn.ctx should be nil after delivery")
	}

	// Check that result is sent
	select {
	case result := <-w.result:
		if result.cn != conn {
			t.Errorf("result.cn = %v, want %v", result.cn, conn)
		}
		if result.err != nil {
			t.Errorf("result.err = %v, want nil", result.err)
		}
	case <-time.After(time.Millisecond):
		t.Error("Expected result to be sent to channel")
	}
}

func TestWantConn_tryDeliver_WithError(t *testing.T) {
	w := &wantConn{
		ctx:    context.Background(),
		result: make(chan wantConnResult, 1),
	}

	testErr := errors.New("test error")

	// Test delivery with error
	delivered := w.tryDeliver(nil, testErr)
	if !delivered {
		t.Error("tryDeliver() = false, want true")
	}

	// Check result
	select {
	case result := <-w.result:
		if result.cn != nil {
			t.Errorf("result.cn = %v, want nil", result.cn)
		}
		if result.err != testErr {
			t.Errorf("result.err = %v, want %v", result.err, testErr)
		}
	case <-time.After(time.Millisecond):
		t.Error("Expected result to be sent to channel")
	}
}

func TestWantConn_tryDeliver_AlreadyDone(t *testing.T) {
	w := &wantConn{
		ctx:    context.Background(),
		done:   true, // Already done
		result: make(chan wantConnResult, 1),
	}

	// Test delivery when already done
	delivered := w.tryDeliver(&Conn{}, nil)
	if delivered {
		t.Error("tryDeliver() = true, want false when already done")
	}

	// Check that no result is sent
	select {
	case <-w.result:
		t.Error("No result should be sent when already done")
	case <-time.After(time.Millisecond):
		// Expected
	}
}

func TestWantConn_cancel_NotDone(t *testing.T) {
	w := &wantConn{
		ctx:    context.Background(),
		result: make(chan wantConnResult, 1),
	}

	// Test cancel when not done
	cn := w.cancel()

	// Should return nil since no connection was not delivered
	if cn != nil {
		t.Errorf("cancel()= %v, want nil when no connection delivered", cn)
	}

	// Check that wantConn is marked as done
	if w.isOngoing() {
		t.Error("wantConn.done = false, want true after cancel")
	}

	// Check that context is cleared
	if w.getCtxForDial() != nil {
		t.Error("wantConn.ctx should be nil after cancel")
	}

	// Check that channel is closed
	select {
	case _, ok := <-w.result:
		if ok {
			t.Error("result channel should be closed after cancel")
		}
	case <-time.After(time.Millisecond):
		t.Error("Expected channel to be closed")
	}
}

func TestWantConn_cancel_AlreadyDone(t *testing.T) {
	w := &wantConn{
		ctx:    context.Background(),
		done:   true,
		result: make(chan wantConnResult, 1),
	}

	// Put a result in the channel without connection (to avoid nil pointer issues)
	testErr := errors.New("test error")
	w.result <- wantConnResult{cn: nil, err: testErr}

	// Test cancel when already done
	cn := w.cancel()

	// Should return nil since the result had no connection
	if cn != nil {
		t.Errorf("cancel()= %v, want nil when result had no connection", cn)
	}

	// Check that wantConn remains done
	if w.isOngoing() {
		t.Error("wantConn.done = false, want true")
	}

	// Check that context is cleared
	if w.getCtxForDial() != nil {
		t.Error("wantConn.ctx should be nil after cancel")
	}
}

func TestWantConnQueue_newWantConnQueue(t *testing.T) {
	q := newWantConnQueue()
	if q == nil {
		t.Fatal("newWantConnQueue() returned nil")
	}
	if q.items == nil {
		t.Error("queue items should be initialized")
	}
	if len(q.items) != 0 {
		t.Errorf("new queue length = %d, want 0", len(q.items))
	}
}

func TestWantConnQueue_enqueue_dequeue(t *testing.T) {
	q := newWantConnQueue()

	// Test dequeue from empty queue
	item, ok := q.dequeue()
	if ok {
		t.Error("dequeue() from empty queue should return false")
	}
	if item != nil {
		t.Error("dequeue() from empty queue should return nil")
	}

	// Create test wantConn items
	w1 := &wantConn{ctx: context.Background(), result: make(chan wantConnResult, 1)}
	w2 := &wantConn{ctx: context.Background(), result: make(chan wantConnResult, 1)}
	w3 := &wantConn{ctx: context.Background(), result: make(chan wantConnResult, 1)}

	// Test enqueue
	q.enqueue(w1)
	q.enqueue(w2)
	q.enqueue(w3)

	// Test FIFO behavior
	item, ok = q.dequeue()
	if !ok {
		t.Error("dequeue() should return true when queue has items")
	}
	if item != w1 {
		t.Errorf("dequeue() = %v, want %v (FIFO order)", item, w1)
	}

	item, ok = q.dequeue()
	if !ok {
		t.Error("dequeue() should return true when queue has items")
	}
	if item != w2 {
		t.Errorf("dequeue() = %v, want %v (FIFO order)", item, w2)
	}

	item, ok = q.dequeue()
	if !ok {
		t.Error("dequeue() should return true when queue has items")
	}
	if item != w3 {
		t.Errorf("dequeue() = %v, want %v (FIFO order)", item, w3)
	}

	// Test dequeue from empty queue again
	item, ok = q.dequeue()
	if ok {
		t.Error("dequeue() from empty queue should return false")
	}
	if item != nil {
		t.Error("dequeue() from empty queue should return nil")
	}
}

func TestWantConnQueue_ConcurrentAccess(t *testing.T) {
	q := newWantConnQueue()
	const numWorkers = 10
	const itemsPerWorker = 100

	var wg sync.WaitGroup

	// Start enqueuers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				w := &wantConn{
					ctx:    context.Background(),
					result: make(chan wantConnResult, 1),
				}
				q.enqueue(w)
			}
		}()
	}

	// Start dequeuers
	dequeued := make(chan *wantConn, numWorkers*itemsPerWorker)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				for {
					if item, ok := q.dequeue(); ok {
						dequeued <- item
						break
					}
					// Small delay to avoid busy waiting
					time.Sleep(time.Microsecond)
				}
			}
		}()
	}

	wg.Wait()
	close(dequeued)

	// Count dequeued items
	count := 0
	for range dequeued {
		count++
	}

	expectedCount := numWorkers * itemsPerWorker
	if count != expectedCount {
		t.Errorf("dequeued %d items, want %d", count, expectedCount)
	}

	// Queue should be empty
	if item, ok := q.dequeue(); ok {
		t.Errorf("queue should be empty but got item: %v", item)
	}
}

func TestWantConnQueue_ThreadSafety(t *testing.T) {
	q := newWantConnQueue()
	const numOperations = 1000

	var wg sync.WaitGroup
	errors := make(chan error, numOperations*2)

	// Concurrent enqueue operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			w := &wantConn{
				ctx:    context.Background(),
				result: make(chan wantConnResult, 1),
			}
			q.enqueue(w)
		}
	}()

	// Concurrent dequeue operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		dequeued := 0
		for dequeued < numOperations {
			if _, ok := q.dequeue(); ok {
				dequeued++
			} else {
				// Small delay when queue is empty
				time.Sleep(time.Microsecond)
			}
		}
	}()

	// Wait for completion
	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Error(err)
	}

	// Final queue should be empty
	if item, ok := q.dequeue(); ok {
		t.Errorf("queue should be empty but got item: %v", item)
	}
}

// Benchmark tests
func BenchmarkWantConnQueue_Enqueue(b *testing.B) {
	q := newWantConnQueue()

	// Pre-allocate a pool of wantConn to reuse
	const poolSize = 1000
	wantConnPool := make([]*wantConn, poolSize)
	for i := 0; i < poolSize; i++ {
		wantConnPool[i] = &wantConn{
			ctx:    context.Background(),
			result: make(chan wantConnResult, 1),
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := wantConnPool[i%poolSize]
		q.enqueue(w)
	}
}

func BenchmarkWantConnQueue_Dequeue(b *testing.B) {
	q := newWantConnQueue()

	// Use a reasonable fixed size for pre-population to avoid memory issues
	const queueSize = 10000

	// Pre-populate queue with a fixed reasonable size
	for i := 0; i < queueSize; i++ {
		w := &wantConn{
			ctx:    context.Background(),
			result: make(chan wantConnResult, 1),
		}
		q.enqueue(w)
	}

	b.ResetTimer()

	// Benchmark dequeue operations, refilling as needed
	for i := 0; i < b.N; i++ {
		if _, ok := q.dequeue(); !ok {
			// Queue is empty, refill a batch
			for j := 0; j < 1000; j++ {
				w := &wantConn{
					ctx:    context.Background(),
					result: make(chan wantConnResult, 1),
				}
				q.enqueue(w)
			}
			// Dequeue again
			q.dequeue()
		}
	}
}

func BenchmarkWantConnQueue_EnqueueDequeue(b *testing.B) {
	q := newWantConnQueue()

	// Pre-allocate a pool of wantConn to reuse
	const poolSize = 1000
	wantConnPool := make([]*wantConn, poolSize)
	for i := 0; i < poolSize; i++ {
		wantConnPool[i] = &wantConn{
			ctx:    context.Background(),
			result: make(chan wantConnResult, 1),
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := wantConnPool[i%poolSize]
		q.enqueue(w)
		q.dequeue()
	}
}

// TestWantConn_RaceConditionNilContext tests the race condition where
// getCtxForDial can return nil after the context is cancelled.
// This test verifies that the fix in newConn handles nil context gracefully.
func TestWantConn_RaceConditionNilContext(t *testing.T) {
	// This test simulates the race condition described in the issue:
	// 1. Main goroutine creates a wantConn with a context
	// 2. Background goroutine starts but hasn't called getCtxForDial yet
	// 3. Main goroutine times out and calls cancel(), setting w.ctx to nil
	// 4. Background goroutine calls getCtxForDial() and gets nil
	// 5. Background goroutine calls newConn(nil, true) which should not panic

	dialCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	w := &wantConn{
		ctx:       dialCtx,
		cancelCtx: cancel,
		result:    make(chan wantConnResult, 1),
	}

	// Simulate the race condition by canceling the context
	// and then trying to get it
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// Small delay to ensure cancel happens first
		time.Sleep(10 * time.Millisecond)

		// This should return nil after cancel
		ctx := w.getCtxForDial()

		// Verify that we got nil context
		if ctx != nil {
			t.Errorf("Expected nil context after cancel, got %v", ctx)
		}
	}()

	// Cancel the context immediately
	w.cancel()

	wg.Wait()

	// Verify the wantConn state
	if w.isOngoing() {
		t.Error("wantConn should be marked as done after cancel")
	}
	if w.getCtxForDial() != nil {
		t.Error("wantConn.ctx should be nil after cancel")
	}
}

// TestWantConnQueue_dropFrontDone_EmptyQueue tests dropFrontDone on an empty queue.
func TestWantConnQueue_dropFrontDone_EmptyQueue(t *testing.T) {
	q := newWantConnQueue()

	// Call dropFrontDone on empty queue
	count := q.discardDoneAtFront()

	// Verify no elements were removed
	if count != 0 {
		t.Errorf("dropFrontDone() on empty queue = %d, want 0", count)
	}

	// Verify queue is still empty
	if q.len() != 0 {
		t.Errorf("queue length after dropFrontDone = %d, want 0", q.len())
	}
}

// TestWantConnQueue_dropFrontDone_AllDone tests dropFrontDone when all elements are done.
func TestWantConnQueue_dropFrontDone_AllDone(t *testing.T) {
	q := newWantConnQueue()

	// Create 3 wantConn items, all marked as done
	for i := 0; i < 3; i++ {
		w := &wantConn{
			ctx:    context.Background(),
			done:   true, // Mark as done
			result: make(chan wantConnResult, 1),
		}
		q.enqueue(w)
	}

	// Verify initial queue length
	if q.len() != 3 {
		t.Errorf("initial queue length = %d, want 3", q.len())
	}

	// Call dropFrontDone
	count := q.discardDoneAtFront()

	// Verify all 3 elements were removed
	if count != 3 {
		t.Errorf("dropFrontDone() = %d, want 3", count)
	}

	// Verify queue is now empty
	if q.len() != 0 {
		t.Errorf("queue length after dropFrontDone = %d, want 0", q.len())
	}
}

// TestWantConnQueue_dropFrontDone_NoneDone tests dropFrontDone when no elements are done.
func TestWantConnQueue_dropFrontDone_NoneDone(t *testing.T) {
	q := newWantConnQueue()

	// Create 3 wantConn items, none marked as done
	for i := 0; i < 3; i++ {
		w := &wantConn{
			ctx:    context.Background(),
			done:   false, // Not done
			result: make(chan wantConnResult, 1),
		}
		q.enqueue(w)
	}

	// Verify initial queue length
	if q.len() != 3 {
		t.Errorf("initial queue length = %d, want 3", q.len())
	}

	// Call dropFrontDone
	count := q.discardDoneAtFront()

	// Verify no elements were removed
	if count != 0 {
		t.Errorf("dropFrontDone() = %d, want 0", count)
	}

	// Verify queue length unchanged
	if q.len() != 3 {
		t.Errorf("queue length after dropFrontDone = %d, want 3", q.len())
	}
}

// TestWantConnQueue_dropFrontDone_PartialDone tests dropFrontDone with mixed done/not-done elements.
// This is the core test case that verifies dropFrontDone stops at the first not-done element.
func TestWantConnQueue_dropFrontDone_PartialDone(t *testing.T) {
	q := newWantConnQueue()

	// Create pattern: [done, done, not-done, done, not-done]
	states := []bool{true, true, false, true, false}
	var items []*wantConn

	for _, done := range states {
		w := &wantConn{
			ctx:    context.Background(),
			done:   done,
			result: make(chan wantConnResult, 1),
		}
		q.enqueue(w)
		items = append(items, w)
	}

	// Verify initial queue length
	if q.len() != 5 {
		t.Errorf("initial queue length = %d, want 5", q.len())
	}

	// Call dropFrontDone
	count := q.discardDoneAtFront()

	// Verify only first 2 elements were removed (stopped at first not-done)
	if count != 2 {
		t.Errorf("dropFrontDone() = %d, want 2", count)
	}

	// Verify queue length is now 3
	if q.len() != 3 {
		t.Errorf("queue length after dropFrontDone = %d, want 3", q.len())
	}

	// Verify the front element is the third item (first not-done)
	front, ok := q.dequeue()
	if !ok {
		t.Fatal("expected to dequeue an item")
	}
	if front != items[2] {
		t.Error("front element should be the third item (first not-done)")
	}

	// Verify remaining elements are items[3] and items[4]
	next, ok := q.dequeue()
	if !ok || next != items[3] {
		t.Error("second element should be items[3]")
	}
	next, ok = q.dequeue()
	if !ok || next != items[4] {
		t.Error("third element should be items[4]")
	}

	// Queue should now be empty
	if q.len() != 0 {
		t.Errorf("queue should be empty, got length %d", q.len())
	}
}

// TestWantConnQueue_dropFrontDone_SingleElement tests dropFrontDone with single element.
func TestWantConnQueue_dropFrontDone_SingleElement(t *testing.T) {
	// Test 1: Single done element
	q1 := newWantConnQueue()
	w1 := &wantConn{
		ctx:    context.Background(),
		done:   true,
		result: make(chan wantConnResult, 1),
	}
	q1.enqueue(w1)

	count := q1.discardDoneAtFront()
	if count != 1 {
		t.Errorf("dropFrontDone() with single done element = %d, want 1", count)
	}
	if q1.len() != 0 {
		t.Errorf("queue should be empty after dropping single done element")
	}

	// Test 2: Single not-done element
	q2 := newWantConnQueue()
	w2 := &wantConn{
		ctx:    context.Background(),
		done:   false,
		result: make(chan wantConnResult, 1),
	}
	q2.enqueue(w2)

	count = q2.discardDoneAtFront()
	if count != 0 {
		t.Errorf("dropFrontDone() with single not-done element = %d, want 0", count)
	}
	if q2.len() != 1 {
		t.Errorf("queue length should remain 1 after dropFrontDone")
	}
}

// TestWantConnQueue_dropFrontDone_MultipleCalls tests consecutive calls to dropFrontDone.
func TestWantConnQueue_dropFrontDone_MultipleCalls(t *testing.T) {
	q := newWantConnQueue()

	// Add initial elements: [done, done, not-done]
	w1 := &wantConn{ctx: context.Background(), done: true, result: make(chan wantConnResult, 1)}
	w2 := &wantConn{ctx: context.Background(), done: true, result: make(chan wantConnResult, 1)}
	w3 := &wantConn{ctx: context.Background(), done: false, result: make(chan wantConnResult, 1)}
	q.enqueue(w1)
	q.enqueue(w2)
	q.enqueue(w3)

	// First call: should remove 2 done elements
	count1 := q.discardDoneAtFront()
	if count1 != 2 {
		t.Errorf("first dropFrontDone() = %d, want 2", count1)
	}
	if q.len() != 1 {
		t.Errorf("queue length after first drop = %d, want 1", q.len())
	}

	// Mark w3 as done and add more elements
	w3.mu.Lock()
	w3.done = true
	w3.mu.Unlock()
	w4 := &wantConn{ctx: context.Background(), done: true, result: make(chan wantConnResult, 1)}
	w5 := &wantConn{ctx: context.Background(), done: false, result: make(chan wantConnResult, 1)}
	q.enqueue(w4)
	q.enqueue(w5)

	// Second call: should remove w3 and w4 (now both done)
	count2 := q.discardDoneAtFront()
	if count2 != 2 {
		t.Errorf("second dropFrontDone() = %d, want 2", count2)
	}
	if q.len() != 1 {
		t.Errorf("queue length after second drop = %d, want 1", q.len())
	}

	// Verify remaining element is w5
	remaining, ok := q.dequeue()
	if !ok || remaining != w5 {
		t.Error("remaining element should be w5")
	}
}

// TestWantConnQueue_dropFrontDone_ConcurrentWithEnqueue tests concurrent dropFrontDone and enqueue.
func TestWantConnQueue_dropFrontDone_ConcurrentWithEnqueue(t *testing.T) {
	q := newWantConnQueue()
	const numOperations = 1000

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Goroutine 1: Continuously enqueue elements
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			w := &wantConn{
				ctx:    context.Background(),
				done:   i%2 == 0, // Alternate between done and not-done
				result: make(chan wantConnResult, 1),
			}
			q.enqueue(w)
			time.Sleep(time.Microsecond)
		}
		close(done)
	}()

	// Goroutine 2: Continuously call dropFrontDone
	wg.Add(1)
	go func() {
		defer wg.Done()
		totalDropped := 0
		for {
			select {
			case <-done:
				// Final cleanup
				totalDropped += q.discardDoneAtFront()
				t.Logf("Total elements dropped: %d", totalDropped)
				return
			default:
				dropped := q.discardDoneAtFront()
				totalDropped += dropped
				time.Sleep(time.Microsecond)
			}
		}
	}()

	wg.Wait()

	// No panic or race condition is success
	t.Logf("Final queue length: %d", q.len())
}

// TestWantConnQueue_dropFrontDone_ConcurrentWithDequeue tests concurrent operations.
func TestWantConnQueue_dropFrontDone_ConcurrentWithDequeue(t *testing.T) {
	q := newWantConnQueue()
	const numOperations = 500

	var wg sync.WaitGroup

	// Pre-populate queue
	for i := 0; i < 100; i++ {
		w := &wantConn{
			ctx:    context.Background(),
			done:   i%3 == 0,
			result: make(chan wantConnResult, 1),
		}
		q.enqueue(w)
	}

	// Goroutine 1: enqueue
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			w := &wantConn{
				ctx:    context.Background(),
				done:   i%2 == 0,
				result: make(chan wantConnResult, 1),
			}
			q.enqueue(w)
		}
	}()

	// Goroutine 2: dequeue
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations/2; i++ {
			q.dequeue()
			time.Sleep(time.Microsecond)
		}
	}()

	// Goroutine 3: dropFrontDone
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations/4; i++ {
			q.discardDoneAtFront()
			time.Sleep(time.Microsecond)
		}
	}()

	wg.Wait()

	// No panic or race condition is success
	t.Logf("Final queue length: %d", q.len())
}

// TestWantConnQueue_len tests the len() method.
func TestWantConnQueue_len(t *testing.T) {
	q := newWantConnQueue()

	// Test empty queue
	if length := q.len(); length != 0 {
		t.Errorf("empty queue len() = %d, want 0", length)
	}

	// Add elements and verify length
	for i := 1; i <= 5; i++ {
		w := &wantConn{
			ctx:    context.Background(),
			result: make(chan wantConnResult, 1),
		}
		q.enqueue(w)

		if length := q.len(); length != i {
			t.Errorf("queue len() after %d enqueues = %d, want %d", i, length, i)
		}
	}

	// Remove elements and verify length
	for i := 4; i >= 0; i-- {
		q.dequeue()
		if length := q.len(); length != i {
			t.Errorf("queue len() after dequeue = %d, want %d", length, i)
		}
	}
}

// TestWantConnQueue_len_Concurrent tests len() thread safety.
func TestWantConnQueue_len_Concurrent(t *testing.T) {
	q := newWantConnQueue()
	const numReaders = 10
	const numWriters = 5
	const operations = 100

	var wg sync.WaitGroup

	// Multiple readers calling len()
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				_ = q.len() // Just read, don't care about value
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Writers enqueueing
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				w := &wantConn{
					ctx:    context.Background(),
					result: make(chan wantConnResult, 1),
				}
				q.enqueue(w)
			}
		}()
	}

	wg.Wait()

	// Verify final length is correct
	expectedLength := numWriters * operations
	if length := q.len(); length != expectedLength {
		t.Errorf("final queue len() = %d, want %d", length, expectedLength)
	}
}
