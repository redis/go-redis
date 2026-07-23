package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// defaultRunDuration is how long the example subscribes, publishes and
	// monitors. Override with REDIS_RUN_DURATION (e.g. "30s") for a quick run.
	defaultRunDuration = 5 * time.Minute
	// drainWindow is extra time after publishing stops to receive in-flight messages
	// so they are not falsely counted as lost.
	drainWindow = 3 * time.Second
	// publishInterval is the pause between full publish rounds (one message per channel).
	publishInterval = 200 * time.Millisecond
	// topoPollInterval is how often the cluster topology is polled for changes.
	topoPollInterval = 2 * time.Second
	// channelsPerNode is how many shard channels to target on each master node.
	channelsPerNode = 3
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addrs := clusterAddrs()
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("failed to connect to cluster: %v", err))
	}
	fmt.Println("✓ connected to Redis cluster")

	// Per-seed clients used to discover the full slot map. A single
	// ClusterClient.ClusterSlots() call is routed to one (random) node, and in
	// some deployments a master answers with only its own slot range. Querying
	// every seed and merging the ranges yields a stable, complete topology view.
	seeds := seedClients(addrs)
	defer func() {
		for _, c := range seeds {
			_ = c.Close()
		}
	}()

	// Build a set of channels that spans every master node in the cluster.
	slots := discoverSlots(ctx, seeds)
	channels, nodeForChannel := buildChannels(slots)
	fmt.Printf("✓ prepared %d shard channels across %d master nodes\n",
		len(channels), countNodes(nodeForChannel))

	// Subscribe to all of them; ShardedPubSub maintains one connection per node.
	sps := rdb.SSubscribeSharded(ctx, channels...)
	defer sps.Close()
	msgCh := sps.Channel(redis.WithChannelSize(2048))

	// Wait for all shard connections to be established before publishing.
	for {
		if err := sps.Ping(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	fmt.Println("✓ all shard subscriptions are active")

	// Per-channel counters. The maps are populated up front and never mutated
	// afterwards, so the goroutines below only touch the atomic values.
	sent := make(map[string]*int64, len(channels))
	recv := make(map[string]*int64, len(channels))
	for _, ch := range channels {
		sent[ch] = new(int64)
		recv[ch] = new(int64)
	}

	runFor := runDuration()
	pubCtx, cancelPub := context.WithTimeout(ctx, runFor)
	defer cancelPub()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); receiveLoop(ctx, pubCtx, msgCh, recv) }()
	go func() { defer wg.Done(); publishLoop(pubCtx, rdb, channels, sent) }()
	go func() { defer wg.Done(); monitorTopology(pubCtx, seeds) }()

	fmt.Printf("✓ running for %s (Ctrl-C to stop early)\n\n", runFor)
	wg.Wait()

	printStats(channels, nodeForChannel, sent, recv)
}

// runDuration returns defaultRunDuration, or the value of REDIS_RUN_DURATION
// (any time.ParseDuration string, e.g. "30s") when set.
func runDuration() time.Duration {
	if v := os.Getenv("REDIS_RUN_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		fmt.Printf("warning: invalid REDIS_RUN_DURATION %q, using %s\n", v, defaultRunDuration)
	}
	return defaultRunDuration
}

// clusterAddrs returns the cluster seed addresses from REDIS_CLUSTER_ADDRS
// (comma separated) or a default local docker-cluster set.
func clusterAddrs() []string {
	if v := os.Getenv("REDIS_CLUSTER_ADDRS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{
		"localhost:16600", "localhost:16601", "localhost:16602",
		"localhost:16603", "localhost:16604", "localhost:16605",
	}
}

// seedClients opens one plain client per seed address. These are used only to
// query CLUSTER SLOTS directly (bypassing cluster-client request routing).
func seedClients(addrs []string) []*redis.Client {
	clients := make([]*redis.Client, 0, len(addrs))
	for _, addr := range addrs {
		clients = append(clients, redis.NewClient(&redis.Options{Addr: addr}))
	}
	return clients
}

// discoverSlots queries every seed for its CLUSTER SLOTS view and merges the
// results into a single, complete slot map. Some nodes report only their own
// range; merging by slot-range start (preferring the entry with the most node
// detail) reconstructs the full topology regardless of which node answers.
func discoverSlots(ctx context.Context, clients []*redis.Client) []redis.ClusterSlot {
	byStart := make(map[int]redis.ClusterSlot)
	for _, c := range clients {
		slots, err := c.ClusterSlots(ctx).Result()
		if err != nil {
			continue
		}
		for _, s := range slots {
			if existing, ok := byStart[s.Start]; !ok || len(s.Nodes) > len(existing.Nodes) {
				byStart[s.Start] = s
			}
		}
	}
	out := make([]redis.ClusterSlot, 0, len(byStart))
	for _, s := range byStart {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

// buildChannels generates shard channels so that each master node is covered by
// channelsPerNode channels. Channel names are hashed locally with the same
// CRC16 algorithm Redis uses and routed via the cluster slot map.
func buildChannels(slots []redis.ClusterSlot) ([]string, map[string]string) {
	if len(slots) == 0 {
		panic("failed to discover any cluster slots")
	}

	need := make(map[string]int)
	for _, s := range slots {
		if len(s.Nodes) > 0 {
			need[s.Nodes[0].Addr] = channelsPerNode
		}
	}

	nodeForChannel := make(map[string]string)
	var channels []string
	for i := 0; len(need) > 0; i++ {
		ch := fmt.Sprintf("shard-channel:%d", i)
		addr := masterForSlot(slots, redisSlot(ch))
		if addr == "" || need[addr] <= 0 {
			continue
		}
		channels = append(channels, ch)
		nodeForChannel[ch] = addr
		if need[addr]--; need[addr] == 0 {
			delete(need, addr)
		}
	}
	return channels, nodeForChannel
}

// masterForSlot returns the master address that owns the given slot.
func masterForSlot(slots []redis.ClusterSlot, slot int) string {
	for _, s := range slots {
		if slot >= s.Start && slot <= s.End && len(s.Nodes) > 0 {
			return s.Nodes[0].Addr
		}
	}
	return ""
}

// countNodes returns the number of distinct node addresses in the mapping.
func countNodes(nodeForChannel map[string]string) int {
	set := make(map[string]struct{})
	for _, a := range nodeForChannel {
		set[a] = struct{}{}
	}
	return len(set)
}

// publishLoop publishes a monotonically increasing sequence number to every
// channel each round until ctx is done. Each successful SPUBLISH is counted.
func publishLoop(ctx context.Context, rdb *redis.ClusterClient, channels []string, sent map[string]*int64) {
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		seq++
		for _, ch := range channels {
			payload := fmt.Sprintf("%s#%d", ch, seq)
			if err := rdb.SPublish(ctx, ch, payload).Err(); err != nil {
				// Node may be migrating/unavailable; not counted as sent.
				continue
			}
			atomic.AddInt64(sent[ch], 1)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(publishInterval):
		}
	}
}

// receiveLoop consumes messages until pubCtx is done, then keeps draining for
// drainWindow so in-flight messages are not falsely counted as lost.
func receiveLoop(ctx, pubCtx context.Context, msgCh <-chan *redis.Message, recv map[string]*int64) {
	count := func(ch string) {
		if c, ok := recv[ch]; ok {
			atomic.AddInt64(c, 1)
		}
	}
	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				return
			}
			count(msg.Channel)
		case <-ctx.Done():
			return
		case <-pubCtx.Done():
			drain := time.After(drainWindow)
			for {
				select {
				case msg, ok := <-msgCh:
					if !ok {
						return
					}
					count(msg.Channel)
				case <-drain:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// monitorTopology polls the cluster slot map and reports whenever the set of
// master nodes or their slot ownership changes.
func monitorTopology(ctx context.Context, seeds []*redis.Client) {
	prev := topologySnapshot(ctx, seeds)
	ticker := time.NewTicker(topoPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := topologySnapshot(ctx, seeds)
			if cur == nil || strings.Join(cur, ",") == strings.Join(prev, ",") {
				continue
			}
			fmt.Printf("⚠ [%s] topology change detected\n", time.Now().Format(time.RFC3339))
			fmt.Printf("    masters before: %v\n", mastersOf(prev))
			fmt.Printf("    masters after:  %v\n", mastersOf(cur))
			prev = cur
		}
	}
}

// topologySnapshot returns a sorted, comparable view of slot->master ownership,
// merged across all seeds so a single node's partial view cannot cause flapping.
func topologySnapshot(ctx context.Context, seeds []*redis.Client) []string {
	slots := discoverSlots(ctx, seeds)
	if len(slots) == 0 {
		return nil
	}
	lines := make([]string, 0, len(slots))
	for _, s := range slots {
		master := ""
		if len(s.Nodes) > 0 {
			master = s.Nodes[0].Addr
		}
		lines = append(lines, fmt.Sprintf("%d-%d@%s", s.Start, s.End, master))
	}
	sort.Strings(lines)
	return lines
}

// mastersOf extracts the sorted set of master addresses from a snapshot.
func mastersOf(snapshot []string) []string {
	set := make(map[string]struct{})
	for _, l := range snapshot {
		if i := strings.IndexByte(l, '@'); i >= 0 {
			set[l[i+1:]] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// printStats prints per-channel and total sent/received/lost counters.
func printStats(channels []string, nodeForChannel map[string]string, sent, recv map[string]*int64) {
	fmt.Println("\n========== sharded pub/sub stats ==========")
	sorted := append([]string(nil), channels...)
	sort.Strings(sorted)

	var totalSent, totalRecv int64
	for _, ch := range sorted {
		s := atomic.LoadInt64(sent[ch])
		r := atomic.LoadInt64(recv[ch])
		lost := s - r
		if lost < 0 {
			lost = 0
		}
		totalSent += s
		totalRecv += r
		fmt.Printf("  %-20s node=%-21s sent=%-7d recv=%-7d lost=%d\n",
			ch, nodeForChannel[ch], s, r, lost)
	}

	totalLost := totalSent - totalRecv
	if totalLost < 0 {
		totalLost = 0
	}
	var pct float64
	if totalSent > 0 {
		pct = float64(totalLost) / float64(totalSent) * 100
	}
	fmt.Println("-------------------------------------------")
	fmt.Printf("  TOTAL sent=%d recv=%d lost=%d (%.4f%%)\n", totalSent, totalRecv, totalLost, pct)
	fmt.Println("note: sharded pub/sub has no persistence, so some loss is expected")
	fmt.Println("around topology changes and before subscriptions fully settle.")
}

// redisSlot computes the cluster hash slot for a key/channel, honoring the
// {hashtag} rule, using the CRC16-CCITT (XMODEM) algorithm Redis uses.
func redisSlot(key string) int {
	if start := strings.IndexByte(key, '{'); start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	return int(crc16(key) % 16384)
}

func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
