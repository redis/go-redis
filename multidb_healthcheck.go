package redis

import "context"

// MultiDBHealthCheck is the interface for health checking databases.
//
// Each method reports whether the database is healthy together with the error
// that made it unhealthy (if any). A healthy result is (true, nil). An
// unhealthy result is (false, err) where err explains the failure so callers
// (e.g. health-check metrics) can record why a check failed; err may be nil
// when the check completed and simply determined the database is not healthy.
//
// A check runs on MultiDB's background loop, during construction, and inside
// failover and fallback probes. It must not call the control operations (the
// MultiDBCtrl methods, or Close): see MultiDBCtrl. A check that calls Close,
// for example, blocks forever, because Close waits for the loop the check is
// running on. The same holds for a health-check policy.
type MultiDBHealthCheck interface {
	// CheckHealth checks if a standalone client is healthy.
	CheckHealth(ctx context.Context, client *Client) (bool, error)

	// CheckClusterHealth checks if a cluster client is healthy.
	CheckClusterHealth(ctx context.Context, client *ClusterClient) (bool, error)
}
