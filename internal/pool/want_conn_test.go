package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

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
	w.done = true
	w.ctx = nil
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
	if !w.done {
		t.Error("wantConn.done = false, want true after delivery")
	}

	// Check that context is cleared
	if w.ctx != nil {
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

	// Create a mock pool
	pool := &ConnPool{}

	// Test cancel when not done
	w.cancel(context.Background(), pool)

	// Check that wantConn is marked as done
	if !w.done {
		t.Error("wantConn.done = false, want true after cancel")
	}

	// Check that context is cleared
	if w.ctx != nil {
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

	// Create a mock pool
	pool := &ConnPool{
		cfg: &Options{},
	}

	// Test cancel when already done
	w.cancel(context.Background(), pool)

	// Check that wantConn remains done
	if !w.done {
		t.Error("wantConn.done = false, want true")
	}

	// Check that context is cleared
	if w.ctx != nil {
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
