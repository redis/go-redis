package redis

import (
	"context"
	"errors"
)

// Head (36b349c8) and head+patch glue: process() + routeAndRun() work before I/O.
func perfRouteOne(c *ClusterClient, ctx context.Context, cmd Cmder) (*clusterNode, error) {
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policyErr != nil {
		return nil, decision.policyErr
	}
	slot := c.cmdSlotWithDecision(cmd, &decision, -1)
	return c.cmdNodeWithDecision(ctx, slot, &decision)
}

// perfAPAdmit mirrors AutoPipeliner.submit's cluster hooks on head:
// routeCommand (validation + divert), shardFn.
func perfAPAdmit(ap *AutoPipeliner, ctx context.Context, cmd Cmder) error {
	if ap.routeCommand != nil {
		divert, err := ap.routeCommand(ctx, cmd)
		if err != nil {
			return err
		}
		if divert {
			return errors.New("diverted")
		}
	}
	_ = ap.shardFn(cmd)
	return nil
}

func perfAPRelease(cmd Cmder) { cmd.setClusterRoutingDecision(nil) }
