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

// waitOptsFor returns the cached wait-time options for poolName.
func (r *metricsRecorder) waitOptsFor(poolName string) []metric.RecordOption {
	if o, ok := r.waitOpts.load(poolName); ok {
		return o
	}
	return r.waitOpts.store(poolName, func() []metric.RecordOption {
		return []metric.RecordOption{r.attrSet(waitTimeAttrs(poolName))}
	})
}

// attrSet is withAttrs with the set built once, for options that are cached.
func (r *metricsRecorder) attrSet(attrs []attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(r.customAttrs(attrs)...))
}
