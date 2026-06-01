package redis

import "context"

// MultiDBHealthCheck is the interface for health checking databases.
type MultiDBHealthCheck interface {
	// CheckHealth checks if a standalone client is healthy.
	CheckHealth(ctx context.Context, client *Client) bool

	// CheckClusterHealth checks if a cluster client is healthy.
	CheckClusterHealth(ctx context.Context, client *ClusterClient) bool
}
