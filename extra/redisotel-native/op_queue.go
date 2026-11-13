package redisotel

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// opQueueMax bounds the samples waiting for the worker; past it a sample is
// recorded on the caller's goroutine.
const opQueueMax = 1 << 16

type opSample struct {
	ctx context.Context
	v   float64
	set attribute.Set
}

// opQueue records success-path db.client.operation.duration samples on its own
// goroutine. Full duplex records every command on its one reader goroutine,
// and the SDK's Record there bounds the whole engine.
//
// A Collect still sees every sample queued before it began: the callback
// registered in newOpQueue runs before the SDK aggregates, waits for the batch
// the worker is recording (rmu), and records what is left. The queue is used
// only after that callback has run once: a View can drop the callback's gauge,
// and then samples are recorded on the caller's goroutine, as without a queue.
//
// Not covered: with several readers on one provider, a reader that drops the
// gauge (its own View or aggregation selector) while keeping the histogram
// runs no callback. Its Collect can miss samples the worker has not recorded
// yet; they land in its next Collect, and Shutdown records what is queued.
type opQueue struct {
	hist   metric.Float64Histogram
	active atomic.Bool // the drain callback has run: Collect drains the queue

	qmu     sync.Mutex
	buf     []opSample
	stopped bool

	rmu   sync.Mutex // held while a batch is recorded
	spare []opSample // guarded by rmu

	wake chan struct{}
	done chan struct{}
	exit chan struct{}
	reg  metric.Registration
}

// newOpQueue starts the worker and registers the drain callback with meter.
// The callback is attached to a gauge it never observes, so the SDK exports
// nothing for it.
func newOpQueue(meter metric.Meter, hist metric.Float64Histogram) (*opQueue, error) {
	q := &opQueue{
		hist: hist,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
		exit: make(chan struct{}),
	}
	gauge, err := meter.Int64ObservableGauge("redis.client.internal.operation_duration_queue")
	if err != nil {
		return nil, err
	}
	reg, err := meter.RegisterCallback(func(context.Context, metric.Observer) error {
		q.active.Store(true)
		q.flush()
		return nil
	}, gauge)
	if err != nil {
		return nil, err
	}
	q.reg = reg
	go q.run()
	return q, nil
}

// record queues a sample, or records it here when no Collect has drained the
// queue yet, the queue is busy (another goroutine holds it: concurrent callers
// record in parallel, as before), full, or stopped.
func (q *opQueue) record(ctx context.Context, v float64, set attribute.Set) {
	if !q.active.Load() || !q.qmu.TryLock() {
		q.hist.Record(ctx, v, metric.WithAttributeSet(set))
		return
	}
	if q.stopped || len(q.buf) >= opQueueMax {
		q.qmu.Unlock()
		q.hist.Record(ctx, v, metric.WithAttributeSet(set))
		return
	}
	wasEmpty := len(q.buf) == 0
	q.buf = append(q.buf, opSample{ctx: ctx, v: v, set: set})
	q.qmu.Unlock()
	if wasEmpty {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
}

// flush records every queued sample.
func (q *opQueue) flush() {
	q.rmu.Lock()
	defer q.rmu.Unlock()
	q.qmu.Lock()
	batch := q.buf
	q.buf = q.spare[:0]
	q.qmu.Unlock()
	for i := range batch {
		q.recordOne(batch[i])
		batch[i] = opSample{} // drop the context
	}
	q.spare = batch[:0]
}

// recordOne records one queued sample. A panicking Record (a custom provider)
// is recovered, as the inline path recovers it: here it would end the worker
// or the collecting goroutine, and with it the process.
func (q *opQueue) recordOne(s opSample) {
	defer func() { _ = recover() }()
	q.hist.Record(s.ctx, s.v, metric.WithAttributeSet(s.set))
}

func (q *opQueue) run() {
	defer close(q.exit)
	for {
		select {
		case <-q.wake:
			q.flush()
		case <-q.done:
			q.flush()
			return
		}
	}
}

// stop records what is queued and ends the worker; later samples are recorded
// on the caller's goroutine.
func (q *opQueue) stop() {
	q.qmu.Lock()
	if q.stopped {
		q.qmu.Unlock()
		return
	}
	q.stopped = true
	q.qmu.Unlock()
	close(q.done)
	<-q.exit
	_ = q.reg.Unregister()
}
