package pubsub

// pubSubError is a string-based error type so the pub/sub sentinel
// errors below can be true constants (errors.New values cannot).
// errors.Is still matches them, directly or through %w wrapping.
type pubSubError string

func (e pubSubError) Error() string { return string(e) }

// Every pub/sub sentinel error, in one place.
const (
	// errPubSubNoConn signals that the shared connection is currently
	// gone (e.g. after a failed reconnect); callers retry via the
	// reconnect path.
	errPubSubNoConn pubSubError = "redis: pubsub: connection is not available"

	// errConnUnusable is the reconnect reason when the shared connection
	// was marked for a maintenance-notification handoff (MOVING) or made
	// unusable by a background operation.
	errConnUnusable pubSubError = "redis: pubsub: connection is not usable"

	// errConnExists is connectIdempotentLocked's way of saying the shared
	// connection was already there, so the dial-time subscription replay
	// did NOT run: subscribers must write their commands themselves. It
	// never escapes the manager.
	errConnExists pubSubError = "redis: pubsub: connection already established"

	// Subscribe-argument validation, one per namespace. Exported so the
	// root package's client-level validation (e.g. Ring, which must pick
	// a shard before it can delegate) returns the same error values.
	ErrNoChannels      pubSubError = "redis: pubsub: at least one channel is required"
	ErrNoPatterns      pubSubError = "redis: pubsub: at least one pattern is required"
	ErrNoShardChannels pubSubError = "redis: pubsub: at least one shard channel is required"

	// Frame-parsing sentinels, wrapped with the offending kind/type by
	// parsePubSubMessage.
	errUnsupportedMessage pubSubError = "redis: pubsub: unsupported message"
	errUnsupportedPayload pubSubError = "redis: pubsub: unsupported message payload"
)
