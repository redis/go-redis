package redisotel

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// opQueueMax bounds the samples waiting for the worker; past it a sample is
// recorded on the caller's goroutine.
const opQueueMax = 1 << 16

// opQueuePoll is how often a busy worker flushes. Waking a parked worker costs
// the caller far more than an append, so while samples keep coming the worker
// polls, and callers only append. At 2M samples/s a batch holds ~200.
const opQueuePoll = 100 * time.Microsecond

type opSample struct {
	hist metric.Float64Histogram // per sample: operation duration or wait time
	ctx  context.Context
	v    float64
	opts []metric.RecordOption // cached per attribute set; shared, never modified
}

// opQueue records success-path db.client.operation.duration samples and every
// db.client.connection.wait_time sample on its own goroutine. Full duplex
// records every command on its one reader goroutine, and the SDK's Record
// there bounds the whole engine; on a busy pool every Get records a wait time.
//
// Each sample is recorded exactly once, with the value and options it was
// queued with; only the goroutine and the moment of the Record call change.
// A sample is recorded on the caller's goroutine instead when no Collect has
// run yet, another caller holds the queue, it holds opQueueMax samples, or it
// is stopped.
//
// A Collect still sees every sample queued before it began: the callback
// registered in newOpQueue runs before the SDK aggregates, waits for the batch
// the worker is recording (rmu), and records what is left. The queue is used
// only after that callback has run once: a View can drop the callback's gauge,
// and then samples are recorded on the caller's goroutine, as without a queue.
//
// Not covered: with several readers on one provider, a reader that drops the
// gauge (its own View or aggregation selector) while keeping either histogram
// runs no callback. Its Collect can miss samples the worker has not recorded
// yet; they land in its next Collect, and Shutdown records what is queued.
type opQueue struct {
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
func newOpQueue(meter metric.Meter) (*opQueue, error) {
	q := &opQueue{
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

// record queues a sample for hist, or records it here when no Collect has
// drained the queue yet, the queue is busy (another goroutine holds it:
// concurrent callers record in parallel, as before), full, or stopped.
func (q *opQueue) record(ctx context.Context, hist metric.Float64Histogram, v float64, opts []metric.RecordOption) {
	if !q.active.Load() || !q.qmu.TryLock() {
		hist.Record(ctx, v, opts...)
		return
	}
	if q.stopped || len(q.buf) >= opQueueMax {
		q.qmu.Unlock()
		hist.Record(ctx, v, opts...)
		return
	}
	wasEmpty := len(q.buf) == 0
	q.buf = append(q.buf, opSample{hist: hist, ctx: ctx, v: v, opts: opts})
	q.qmu.Unlock()
	if wasEmpty {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
}

// flush records every queued sample and returns how many it recorded.
func (q *opQueue) flush() int {
	q.rmu.Lock()
	defer q.rmu.Unlock()
	q.qmu.Lock()
	batch := q.buf
	q.buf = q.spare[:0]
	q.qmu.Unlock()
	for i := range batch {
		q.recordOne(batch[i])
		batch[i] = opSample{} // drop the context and instrument
	}
	q.spare = batch[:0]
	return len(batch)
}

// recordOne records one queued sample. A panicking Record (a custom provider)
// is recovered, as the inline path recovers it: here it would end the worker
// or the collecting goroutine, and with it the process.
func (q *opQueue) recordOne(s opSample) {
	defer func() { _ = recover() }()
	s.hist.Record(s.ctx, s.v, s.opts...)
}

func (q *opQueue) run() {
	defer close(q.exit)
	poll := time.NewTimer(opQueuePoll)
	poll.Stop()
	for {
		select {
		case <-q.wake:
		case <-q.done:
			q.flush()
			return
		}
		// Busy: flush every opQueuePoll until a flush finds nothing. A wake sent
		// meanwhile lands in the channel's buffer without readying this goroutine.
		for q.flush() > 0 {
			poll.Reset(opQueuePoll)
			select {
			case <-poll.C:
			case <-q.done:
				q.flush()
				return
			}
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
