package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/hashtag"
)

// Scan runs SCAN on a cluster. Iterated, it covers every master.
//
// A SCAN cursor belongs to the node that returned it, and a keyless command on a
// ClusterClient goes to a random node. ScanIterator re-sends the command for
// every page, so an iterator used to send each page to a random node with the
// previous node's cursor and stop early without an error. Here the command
// walks a snapshot of the masters: pages go to one master until it returns
// cursor 0, then the scan continues on the next master from cursor 0. Until the
// last master is done the command reports a non-zero cursor, so the iterator
// keeps fetching. Every page runs inside the cluster's process hooks.
//
// A MATCH pattern with a hash tag can only match keys of one slot, so that scan
// stays on the master that owns the slot. A loop that passes a cursor back to
// Scan by hand cannot work on a cluster, because the cursor does not say which
// node it belongs to: use the iterator, or ForEachMaster with a Scan per node.
//
// Keys that are added, deleted or moved to another node during the scan can be
// returned twice or missed, as SCAN allows on one node.
func (c *ClusterClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *ScanCmd {
	if cursor != 0 {
		return c.cmdable.Scan(ctx, cursor, match, count)
	}
	return c.scanMasters(ctx, match, scanArgs(match, count, ""))
}

// ScanType is Scan with a TYPE filter. See Scan for how it covers the cluster.
func (c *ClusterClient) ScanType(ctx context.Context, cursor uint64, match string, count int64, keyType string) *ScanCmd {
	if cursor != 0 {
		return c.cmdable.ScanType(ctx, cursor, match, count, keyType)
	}
	return c.scanMasters(ctx, match, scanArgs(match, count, keyType))
}

func scanArgs(match string, count int64, keyType string) []interface{} {
	args := []interface{}{"scan", uint64(0)}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	if keyType != "" {
		args = append(args, "type", keyType)
	}
	return args
}

// scanMasters runs the first page of a SCAN that walks every master, or only the
// master of the slot when match has a hash tag.
func (c *ClusterClient) scanMasters(ctx context.Context, match string, args []interface{}) *ScanCmd {
	w := &clusterScanWalker{c: c}
	cmd := NewScanCmd(ctx, w.process, args...)
	state, err := c.state.Get(ctx)
	if err == nil {
		if hashtag.Present(match) {
			var node *clusterNode
			if node, err = state.slotMasterNode(hashtag.Slot(match)); err == nil {
				w.masters = []*clusterNode{node}
			}
		} else {
			w.masters = append([]*clusterNode(nil), state.Masters...)
		}
	}
	if err == nil && len(w.masters) == 0 {
		err = errClusterNoNodes
	}
	if err != nil {
		cmd.SetErr(err)
		return cmd
	}
	_ = w.process(ctx, cmd)
	return cmd
}

// clusterScanWalker is the process of a ScanCmd from scanMasters. It sends each
// page to the current master and moves on to the next one when the current one
// is done. ScanIterator calls it from one goroutine.
type clusterScanWalker struct {
	c       *ClusterClient
	masters []*clusterNode
	idx     int
	restart bool // the next page starts a new master: reset the cursor to 0
}

func (w *clusterScanWalker) process(ctx context.Context, cmd Cmder) error {
	sc := cmd.(*ScanCmd)
	if w.restart {
		sc.args[1] = uint64(0)
		w.restart = false
	}
	node := w.masters[w.idx]
	err := w.c.withProcessHook(ctx, sc, func(ctx context.Context, cmd Cmder) error {
		return node.Client.Process(ctx, cmd)
	})
	sc.SetErr(err)
	if err == nil && sc.cursor == 0 && w.idx+1 < len(w.masters) {
		w.idx++
		w.restart = true
		sc.cursor = 1 // placeholder: more masters to scan
	}
	return err
}

// Scan on an autopipeliner over a ClusterClient runs the cluster's Scan, which
// covers every master (see ClusterClient.Scan). It runs the first page before it
// returns, also on the async face; on a standalone client Scan is unchanged.
func (ap *AutoPipeliner) Scan(ctx context.Context, cursor uint64, match string, count int64) *ScanCmd {
	if cc, ok := ap.pipeliner.(*ClusterClient); ok {
		if ap.isClosed() {
			return closedScanCmd(ctx, ap.cmdable, cursor)
		}
		return cc.Scan(ctx, cursor, match, count)
	}
	return ap.cmdable.Scan(ctx, cursor, match, count)
}

// ScanType on an autopipeliner over a ClusterClient: see AutoPipeliner.Scan.
func (ap *AutoPipeliner) ScanType(ctx context.Context, cursor uint64, match string, count int64, keyType string) *ScanCmd {
	if cc, ok := ap.pipeliner.(*ClusterClient); ok {
		if ap.isClosed() {
			return closedScanCmd(ctx, ap.cmdable, cursor)
		}
		return cc.ScanType(ctx, cursor, match, count, keyType)
	}
	return ap.cmdable.ScanType(ctx, cursor, match, count, keyType)
}

func closedScanCmd(ctx context.Context, process cmdable, cursor uint64) *ScanCmd {
	cmd := NewScanCmd(ctx, process, "scan", cursor)
	cmd.SetErr(ErrClosed)
	return cmd
}
