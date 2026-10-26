package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9/internal/hashtag"
)

// SCAN has request_policy:special and response_policy:special: a cursor belongs
// to the node that returned it, and the keyspace is split over the masters.
//
// ScanIterator re-sends the same *ScanCmd for every page, so the walk is kept on
// the command. The first page (cursor 0) takes a snapshot of the masters, or only
// the master of the slot when MATCH has a hash tag. Pages go to one master until
// it returns cursor 0; the next page starts the next master from cursor 0. Until
// the last master is done the command reports a non-zero cursor, so the iterator
// keeps fetching. Each page is one ClusterClient.Process call, so process hooks
// run once per page.
//
// A SCAN that carries no walk keeps the routed node: a raw command (Do) and a
// loop that passes a cursor back by hand, which cannot say which node the cursor
// belongs to. Keys added, deleted or moved during the walk can be returned twice
// or missed, as SCAN allows on one node.
type clusterScanState struct {
	masters []*clusterNode
	idx     int
	restart bool // the next page starts a new master: reset the cursor to 0
}

func (c *ClusterClient) executeScanCommand(
	ctx context.Context,
	cmd Cmder,
	node *clusterNode,
	decision clusterRoutingDecision,
) error {
	sc, ok := cmd.(*ScanCmd)
	if !ok || (sc.clusterScan == nil && !scanCursorIsZero(sc)) {
		return c.scanRoutedNode(ctx, cmd, node)
	}
	st := sc.clusterScan
	if st == nil {
		var err error
		if st, err = c.newClusterScan(ctx, sc); err != nil {
			return err
		}
		sc.clusterScan = st
	}
	if st.restart {
		sc.args[1] = uint64(0)
		st.restart = false
	}
	if err := st.masters[st.idx].Client.Process(ctx, sc); err != nil {
		return err
	}
	if sc.cursor == 0 && st.idx+1 < len(st.masters) {
		st.idx++
		st.restart = true
		sc.cursor = 1 // placeholder: more masters to scan
	}
	return nil
}

func (c *ClusterClient) newClusterScan(ctx context.Context, sc *ScanCmd) (*clusterScanState, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}
	if match, ok := scanMatch(sc); ok && hashtag.Present(match) {
		node, err := state.slotMasterNode(hashtag.Slot(match))
		if err != nil {
			return nil, err
		}
		return &clusterScanState{masters: []*clusterNode{node}}, nil
	}
	if len(state.Masters) == 0 {
		return nil, errClusterNoNodes
	}
	return &clusterScanState{masters: append([]*clusterNode(nil), state.Masters...)}, nil
}

// scanRoutedNode runs one SCAN page on the node the router picked.
func (c *ClusterClient) scanRoutedNode(ctx context.Context, cmd Cmder, node *clusterNode) error {
	if node == nil {
		state, err := c.state.Get(ctx)
		if err != nil {
			return err
		}
		if node, err = state.slotMasterNode(hashtag.RandomSlot()); err != nil {
			return err
		}
	}
	return node.Client.Process(ctx, cmd)
}

func scanCursorIsZero(sc *ScanCmd) bool {
	if len(sc.args) < 2 {
		return false
	}
	return fmt.Sprint(sc.args[1]) == "0"
}

func scanMatch(sc *ScanCmd) (string, bool) {
	for i := 2; i+1 < len(sc.args); i++ {
		if name, ok := sc.args[i].(string); ok && strings.EqualFold(name, "match") {
			pattern, ok := sc.args[i+1].(string)
			return pattern, ok
		}
	}
	return "", false
}
