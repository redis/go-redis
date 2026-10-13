package redis

import "context"

// Fix glue: process() + routeAndRun() work before I/O.
func perfRouteOne(c *ClusterClient, ctx context.Context, cmd Cmder) (*clusterNode, error) {
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policyErr != nil {
		return nil, decision.policyErr
	}
	slot := c.cmdSlotWithDecision(cmd, &decision, -1)
	return c.cmdNodeWithDecision(ctx, slot, &decision)
}

func perfAPRelease(cmd Cmder) { cmd.setClusterRoutingDecision(nil) }
