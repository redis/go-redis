package routing

import (
	"math/rand"
	"sync/atomic"
)

// ShardPicker chooses “one arbitrary shard” when the request_policy is
// ReqDefault and the command has no keys.
type ShardPicker interface {
	Next(total int) int // returns an index in [0,total)
}

/*───────────────────────────────
   Round-robin (default)
────────────────────────────────*/

type RoundRobinPicker struct {
	cnt atomic.Uint32
}

func (p *RoundRobinPicker) Next(total int) int {
	if total == 0 {
		return 0
	}
	i := p.cnt.Add(1)
	return int(i-1) % total
}

/*───────────────────────────────
   Random
────────────────────────────────*/

type RandomPicker struct{}

func (RandomPicker) Next(total int) int {
	if total == 0 {
		return 0
	}
	return rand.Intn(total)
}
