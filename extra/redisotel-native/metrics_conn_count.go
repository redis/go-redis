package redisotel

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/redis/go-redis/v9"
)

// poolObserver reports db.client.connection.count and
// db.client.connection.pending_requests by reading each registered pool when
// the SDK collects, so Get and Put do no metric work for them. Either
// instrument is nil when its metric group is off.
type poolObserver struct {
	count   metric.Int64ObservableUpDownCounter
	pending metric.Int64ObservableUpDownCounter
	reg     metric.Registration

	mu     sync.Mutex
	pools  map[redis.Pooler]*poolSeries
	pubSub map[redis.PubSubPooler]*series
	// closed are the exported series of pools that are gone. They are
	// observed at 0 until release: the SDK emits no point for a series that
	// is no longer observed, so a delta reader would never get the final
	// decrement. One entry per series; release drops them.
	closed  []closedSeries
	stopped bool
}

type closedSeries struct {
	inst  metric.Int64ObservableUpDownCounter
	attrs metric.MeasurementOption
}

type poolSeries struct {
	idle, used series
	pending    pendingSeries
}

// series is one pool and state. It is exported from its first non-zero value
// on, as the synchronous counter's series appeared with its first Add.
type series struct {
	attrs metric.MeasurementOption
	seen  bool
}

func (s *series) observe(o metric.Observer, inst metric.Int64ObservableUpDownCounter, v int64) {
	if v == 0 && !s.seen {
		return
	}
	s.seen = true
	o.ObserveInt64(inst, v, s.attrs)
}

// pendingSeries is one pool's pending requests. It is exported from the first
// Get after registration on, as the synchronous counter's series appeared
// with the first Get's +1.
type pendingSeries struct {
	attrs   metric.MeasurementOption
	started uint32 // Gets the pool had started when it was registered
	seen    bool
}

// pendingReader is implemented by the Pooler go-redis hands to a registrar
// for its own connection pools.
type pendingReader interface {
	PendingRequests() (pending, started uint32)
}

// read returns the pool's pending requests and marks the series seen once a
// Get started after registration.
func (s *pendingSeries) read(p redis.Pooler) int64 {
	if pr, ok := p.(pendingReader); ok {
		v, started := pr.PendingRequests()
		if started != s.started {
			s.seen = true
		}
		return int64(v)
	}
	v := p.PoolStats().PendingRequests
	if v != 0 {
		s.seen = true
	}
	return int64(v)
}

func newPoolObserver(meter metric.Meter, count, pending metric.Int64ObservableUpDownCounter) (*poolObserver, error) {
	c := &poolObserver{
		count:   count,
		pending: pending,
		pools:   make(map[redis.Pooler]*poolSeries),
		pubSub:  make(map[redis.PubSubPooler]*series),
	}
	var insts []metric.Observable
	if count != nil {
		insts = append(insts, count)
	}
	if pending != nil {
		insts = append(insts, pending)
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		c.observe(o)
		return nil
	}, insts...)
	if err != nil {
		return nil, err
	}
	c.reg = reg
	return c, nil
}

func (c *poolObserver) observe(o metric.Observer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		c.observePoolsLocked(o)
	}
	for _, s := range c.closed {
		o.ObserveInt64(s.inst, 0, s.attrs)
	}
}

func (c *poolObserver) observePoolsLocked(o metric.Observer) {
	for p, a := range c.pools {
		if collected(p) {
			c.removeLocked(p, a)
			continue
		}
		if c.count != nil {
			s := p.PoolStats()
			a.idle.observe(o, c.count, int64(s.IdleConns))
			a.used.observe(o, c.count, int64(s.TotalConns)-int64(s.IdleConns))
		}
		if c.pending != nil {
			if v := a.pending.read(p); a.pending.seen {
				o.ObserveInt64(c.pending, v, a.pending.attrs)
			}
		}
	}
	for p, a := range c.pubSub {
		if collected(p) {
			c.dropSeriesLocked(a)
			delete(c.pubSub, p)
			continue
		}
		a.observe(o, c.count, int64(p.Stats().Active))
	}
}

// removeLocked removes a pool and keeps its exported series at 0. Caller
// holds c.mu.
func (c *poolObserver) removeLocked(p redis.Pooler, a *poolSeries) {
	delete(c.pools, p)
	c.closeLocked(p, a)
}

// closeLocked keeps a pool's exported series at 0. A pending series counts as
// exported once a Get started. Caller holds c.mu.
func (c *poolObserver) closeLocked(p redis.Pooler, a *poolSeries) {
	if c.pending != nil && !collected(p) {
		a.pending.read(p) // a Get since the last collection still counts
	}
	if c.count != nil {
		c.dropSeriesLocked(&a.idle)
		c.dropSeriesLocked(&a.used)
	}
	if c.pending != nil && a.pending.seen {
		c.closed = append(c.closed, closedSeries{c.pending, a.pending.attrs})
	}
}

// dropSeriesLocked keeps a count series at 0 if it was exported. Caller holds
// c.mu.
func (c *poolObserver) dropSeriesLocked(s *series) {
	if s.seen {
		c.closed = append(c.closed, closedSeries{c.count, s.attrs})
	}
}

// collected reports whether go-redis says the pool behind p was garbage
// collected (its client was dropped without Close).
func collected(p any) bool {
	c, ok := p.(interface{ Collected() bool })
	return ok && c.Collected()
}

// stop is called by Shutdown. Every exported series is observed at 0 from
// then on, so delta readers get the final decrement at their next
// collection. Pools are no longer read and registrations are ignored; the
// pool map is kept for inheritSeen.
func (c *poolObserver) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for p, a := range c.pools {
		c.closeLocked(p, a)
	}
	for _, a := range c.pubSub {
		c.dropSeriesLocked(a)
	}
	c.stopped = true
}

// inheritSeen marks c's series of pools that the stopped observer old had
// exported. The SDK still holds their last value, so they must be observed
// even at 0, or a delta reader misses the decrement.
func (c *poolObserver) inheritSeen(old *poolObserver) {
	old.mu.Lock()
	defer old.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for p, oa := range old.pools {
		if a, ok := c.pools[p]; ok {
			a.idle.seen = a.idle.seen || oa.idle.seen
			a.used.seen = a.used.seen || oa.used.seen
			a.pending.seen = a.pending.seen || oa.pending.seen
		}
	}
	for p, oa := range old.pubSub {
		if a, ok := c.pubSub[p]; ok {
			a.seen = a.seen || oa.seen
		}
	}
}

// release unregisters the callback and drops its series and pools. Init
// calls it for the observers the previous Shutdown stopped. It must not run
// inside the callback.
func (c *poolObserver) release() {
	_ = c.reg.Unregister()
	c.mu.Lock()
	c.closed = nil
	clear(c.pools)
	clear(c.pubSub)
	c.mu.Unlock()
}

// connCountAttrs returns the attribute set for one pool and state, built the
// way the synchronous counter built it per call.
func (r *metricsRecorder) connCountAttrs(poolName, state string, isPubSub bool) metric.MeasurementOption {
	attrs := []attribute.KeyValue{
		attribute.String(AttrDBSystemName, DBSystemRedis),
		getLibraryVersionAttr(),
		attribute.String(AttrDBClientConnectionState, state),
		attribute.Bool(AttrRedisClientConnectionPubSub, isPubSub),
	}
	if poolName != "" {
		attrs = append(attrs, attribute.String(AttrDBClientConnectionPoolName, poolName))
		serverAddr, serverPort, _ := parsePoolName(poolName)
		if serverAddr != "" {
			attrs = append(attrs, attribute.String(AttrServerAddress, serverAddr))
		}
		attrs = addServerPortIfNonDefault(attrs, serverPort)
	}
	return metric.WithAttributeSet(attribute.NewSet(r.customAttrs(attrs)...))
}

// RegisterPool implements redis.OTelPoolRegistrar.
func (r *metricsRecorder) RegisterPool(poolName string, p redis.Pooler) {
	c := r.poolObs
	if c == nil {
		return
	}
	a := &poolSeries{}
	if c.count != nil {
		a.idle.attrs = r.connCountAttrs(poolName, "idle", false)
		a.used.attrs = r.connCountAttrs(poolName, "used", false)
	}
	if c.pending != nil {
		a.pending.attrs = r.attrSet(pendingRequestsAttrs(poolName))
		if pr, ok := p.(pendingReader); ok {
			_, a.pending.started = pr.PendingRequests()
		}
	}
	c.mu.Lock()
	if _, ok := c.pools[p]; !ok && !c.stopped {
		c.pools[p] = a
	}
	c.mu.Unlock()
}

// UnregisterPool implements redis.OTelPoolRegistrar.
func (r *metricsRecorder) UnregisterPool(p redis.Pooler) {
	c := r.poolObs
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if a, ok := c.pools[p]; ok && !c.stopped {
		c.removeLocked(p, a)
	}
}

// RegisterPubSubPool implements redis.OTelPoolRegistrar.
func (r *metricsRecorder) RegisterPubSubPool(poolName string, p redis.PubSubPooler) {
	c := r.poolObs
	if c == nil || c.count == nil {
		return
	}
	a := &series{attrs: r.connCountAttrs(poolName, "used", true)}
	c.mu.Lock()
	if _, ok := c.pubSub[p]; !ok && !c.stopped {
		c.pubSub[p] = a
	}
	c.mu.Unlock()
}

// UnregisterPubSubPool implements redis.OTelPoolRegistrar.
func (r *metricsRecorder) UnregisterPubSubPool(p redis.PubSubPooler) {
	if c := r.poolObs; c != nil {
		c.mu.Lock()
		if a, ok := c.pubSub[p]; ok && !c.stopped {
			c.dropSeriesLocked(a)
			delete(c.pubSub, p)
		}
		c.mu.Unlock()
	}
}
