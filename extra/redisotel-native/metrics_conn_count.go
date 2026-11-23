package redisotel

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/redis/go-redis/v9"
)

// connCounter reports db.client.connection.count by reading each registered
// pool's stats when the SDK collects, so Get and Put do no metric work.
type connCounter struct {
	inst metric.Int64ObservableUpDownCounter
	reg  metric.Registration

	mu     sync.Mutex
	pools  map[redis.Pooler]*poolSeries
	pubSub map[redis.PubSubPooler]*series
	// closed are the exported series of pools that are gone. They are
	// observed at 0 until release: the SDK emits no point for a series that
	// is no longer observed, so a delta reader would never get the final
	// decrement. One entry per series; release drops them.
	closed  []metric.MeasurementOption
	stopped bool
}

type poolSeries struct{ idle, used series }

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

func newConnCounter(meter metric.Meter, inst metric.Int64ObservableUpDownCounter) (*connCounter, error) {
	c := &connCounter{
		inst:   inst,
		pools:  make(map[redis.Pooler]*poolSeries),
		pubSub: make(map[redis.PubSubPooler]*series),
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		c.observe(o)
		return nil
	}, inst)
	if err != nil {
		return nil, err
	}
	c.reg = reg
	return c, nil
}

func (c *connCounter) observe(o metric.Observer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		c.observePoolsLocked(o)
	}
	for _, attrs := range c.closed {
		o.ObserveInt64(c.inst, 0, attrs)
	}
}

func (c *connCounter) observePoolsLocked(o metric.Observer) {
	for p, a := range c.pools {
		if collected(p) {
			c.removeLocked(p, a)
			continue
		}
		s := p.PoolStats()
		a.idle.observe(o, c.inst, int64(s.IdleConns))
		a.used.observe(o, c.inst, int64(s.TotalConns)-int64(s.IdleConns))
	}
	for p, a := range c.pubSub {
		if collected(p) {
			c.dropSeriesLocked(a)
			delete(c.pubSub, p)
			continue
		}
		a.observe(o, c.inst, int64(p.Stats().Active))
	}
}

// removeLocked removes a pool and keeps its exported series at 0. Caller
// holds c.mu.
func (c *connCounter) removeLocked(p redis.Pooler, a *poolSeries) {
	delete(c.pools, p)
	c.dropSeriesLocked(&a.idle)
	c.dropSeriesLocked(&a.used)
}

// dropSeriesLocked keeps s at 0 if it was exported. Caller holds c.mu.
func (c *connCounter) dropSeriesLocked(s *series) {
	if s.seen {
		c.closed = append(c.closed, s.attrs)
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
func (c *connCounter) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range c.pools {
		c.dropSeriesLocked(&a.idle)
		c.dropSeriesLocked(&a.used)
	}
	for _, a := range c.pubSub {
		c.dropSeriesLocked(a)
	}
	c.stopped = true
}

// inheritSeen marks c's series of pools that the stopped counter old had
// exported. The SDK still holds their last value, so they must be observed
// even at 0, or a delta reader misses the decrement.
func (c *connCounter) inheritSeen(old *connCounter) {
	old.mu.Lock()
	defer old.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for p, oa := range old.pools {
		if a, ok := c.pools[p]; ok {
			a.idle.seen = a.idle.seen || oa.idle.seen
			a.used.seen = a.used.seen || oa.used.seen
		}
	}
	for p, oa := range old.pubSub {
		if a, ok := c.pubSub[p]; ok {
			a.seen = a.seen || oa.seen
		}
	}
}

// release unregisters the callback and drops its series and pools. Init
// calls it for the counters the previous Shutdown stopped. It must not run
// inside the callback.
func (c *connCounter) release() {
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
	c := r.connCount
	if c == nil {
		return
	}
	a := &poolSeries{
		idle: series{attrs: r.connCountAttrs(poolName, "idle", false)},
		used: series{attrs: r.connCountAttrs(poolName, "used", false)},
	}
	c.mu.Lock()
	if _, ok := c.pools[p]; !ok && !c.stopped {
		c.pools[p] = a
	}
	c.mu.Unlock()
}

// UnregisterPool implements redis.OTelPoolRegistrar.
func (r *metricsRecorder) UnregisterPool(p redis.Pooler) {
	if c := r.connCount; c != nil {
		c.mu.Lock()
		if a, ok := c.pools[p]; ok && !c.stopped {
			c.removeLocked(p, a)
		}
		c.mu.Unlock()
	}
}

// RegisterPubSubPool implements redis.OTelPoolRegistrar.
func (r *metricsRecorder) RegisterPubSubPool(poolName string, p redis.PubSubPooler) {
	c := r.connCount
	if c == nil {
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
	if c := r.connCount; c != nil {
		c.mu.Lock()
		if a, ok := c.pubSub[p]; ok && !c.stopped {
			c.dropSeriesLocked(a)
			delete(c.pubSub, p)
		}
		c.mu.Unlock()
	}
}
