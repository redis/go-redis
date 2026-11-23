package redisotel

import (
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// attrCache maps a key to measurement options built once. Hits only Load;
// misses go through store under mu, so n is exactly the number of entries.
// Past maxOpAttrSets entries the cache is cleared, so it cannot grow without
// limit. The zero value is ready to use.
type attrCache[K comparable, V any] struct {
	m  sync.Map
	mu sync.Mutex
	n  int
}

func (c *attrCache[K, V]) load(k K) (V, bool) {
	v, ok := c.m.Load(k)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

// store returns the cached value for k, building and caching it if missing.
func (c *attrCache[K, V]) store(k K, build func() V) V {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.m.Load(k); ok {
		return v.(V)
	}
	if c.n >= maxOpAttrSets {
		c.m.Clear()
		c.n = 0
	}
	v := build()
	c.m.Store(k, v)
	c.n++
	return v
}

// poolOpts are the measurement options of the per-pool metrics recorded on
// every Get: pending requests and wait time.
type poolOpts struct {
	pending []metric.AddOption
	wait    []metric.RecordOption
}

// poolOptsFor returns the cached options for poolName.
func (r *metricsRecorder) poolOptsFor(poolName string) *poolOpts {
	if o, ok := r.poolOpts.load(poolName); ok {
		return o
	}
	return r.poolOpts.store(poolName, func() *poolOpts {
		return &poolOpts{
			pending: []metric.AddOption{r.attrSet(pendingRequestsAttrs(poolName))},
			wait:    []metric.RecordOption{r.attrSet(waitTimeAttrs(poolName))},
		}
	})
}

// attrSet is withAttrs with the set built once, for options that are cached.
func (r *metricsRecorder) attrSet(attrs []attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(r.customAttrs(attrs)...))
}
