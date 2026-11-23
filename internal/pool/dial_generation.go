package pool

import (
	"context"
	"errors"
)

// ErrDialGenerationChanged means the selected endpoint changed before admission.
// It is an acquisition failure, not evidence that a Redis command was executed.
var ErrDialGenerationChanged = errors.New("redis: endpoint changed during connection acquisition")

type (
	dialGenerationKey struct{}
	dialGeneration    struct{ value uint64 }
)

// SetDialGeneration records the generation selected by an endpoint-aware dialer.
// The slot belongs to one synchronous dial attempt, including its DialHook chain.
func SetDialGeneration(ctx context.Context, generation uint64) {
	if slot, ok := ctx.Value(dialGenerationKey{}).(*dialGeneration); ok {
		slot.value = generation
	}
}

// DialGeneration is immutable after connection construction.
func (cn *Conn) DialGeneration() uint64 { return cn.dialGeneration }

func (p *ConnPool) currentGeneration(cn *Conn) bool {
	return p.cfg.DialGeneration == nil || cn.dialGeneration == p.cfg.DialGeneration.Load()
}
