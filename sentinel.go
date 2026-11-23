package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

//------------------------------------------------------------------------------

// FailoverOptions are used to configure a failover client and should
// be passed to NewFailoverClient.
type FailoverOptions struct {
	// The master name.
	MasterName string
	// A seed list of host:port addresses of sentinel nodes.
	SentinelAddrs []string

	// ClientName will execute the `CLIENT SETNAME ClientName` command for each conn.
	ClientName string

	// If specified with SentinelPassword, enables ACL-based authentication (via
	// AUTH <user> <pass>).
	SentinelUsername string
	// Sentinel password from "requirepass <password>" (if enabled) in Sentinel
	// configuration, or, if SentinelUsername is also supplied, used for ACL-based
	// authentication.
	SentinelPassword string

	// Allows routing read-only commands to the closest master or replica node.
	// This option only works with NewFailoverClusterClient.
	RouteByLatency bool
	// RouteByLatencyTolerance is passed through to ClusterOptions; see its documentation.
	RouteByLatencyTolerance time.Duration
	// Allows routing read-only commands to the random master or replica node.
	// This option only works with NewFailoverClusterClient.
	RouteRandomly bool

	// Route all commands to replica read-only nodes.
	ReplicaOnly bool

	// Use replicas disconnected with master when cannot get connected replicas
	// Now, this option only works in RandomReplicaAddr function.
	UseDisconnectedReplicas bool

	// Following options are copied from Options struct.

	Dialer    func(ctx context.Context, network, addr string) (net.Conn, error)
	OnConnect func(ctx context.Context, cn *Conn) error

	Protocol int
	Username string
	Password string

	// AuthEmptyPassword: see Options.AuthEmptyPassword. It applies to the
	// master/replica connections (Username without Password).
	AuthEmptyPassword bool

	// SentinelAuthEmptyPassword is AuthEmptyPassword for the sentinel
	// connections (SentinelUsername without SentinelPassword). The two are
	// independent, like the sentinel and data-node credentials.
	SentinelAuthEmptyPassword bool

	// Push notifications are always enabled for RESP3 connections
	// CredentialsProvider allows the username and password to be updated
	// before reconnecting. It should return the current username and password.
	CredentialsProvider func() (username string, password string)

	// CredentialsProviderContext is an enhanced parameter of CredentialsProvider,
	// done to maintain API compatibility. In the future,
	// there might be a merge between CredentialsProviderContext and CredentialsProvider.
	// There will be a conflict between them; if CredentialsProviderContext exists, we will ignore CredentialsProvider.
	CredentialsProviderContext func(ctx context.Context) (username string, password string, err error)

	// StreamingCredentialsProvider is used to retrieve the credentials
	// for the connection from an external source. Those credentials may change
	// during the connection lifetime. This is useful for managed identity
	// scenarios where the credentials are retrieved from an external source.
	//
	// Currently, this is a placeholder for the future implementation.
	StreamingCredentialsProvider auth.StreamingCredentialsProvider
	DB                           int

	// ClientSideCacheConfig enables an independent cache per node when using
	// NewFailoverClusterClient. NewFailoverClient supports primary-only CSC;
	// ReplicaOnly disables it with a warning. Sentinel discovery stays uncached.
	// As with standalone CSC, caching is disabled when DB is not zero.
	// See ClusterOptions.ClientSideCacheConfig.
	// Experimental: this API may change in a minor release.
	ClientSideCacheConfig *ClientSideCacheConfig

	// ClientSideCache supplies an explicit cache for NewFailoverClient. Storage
	// is scoped to this client and its clones. It is not distributed to nodes
	// of NewFailoverClusterClient, which creates independent local caches.
	// Experimental: this API may change in a minor release.
	ClientSideCache Cache

	// ClientSideCacheTrackingMode selects the tracking mode for each connection.
	// The zero value is TrackingModePlain. An invalid value logs a warning and
	// disables CSC at construction. Supported by primary-only NewFailoverClient and NewFailoverClusterClient.
	// Experimental: this API may change in a minor release.
	ClientSideCacheTrackingMode TrackingMode

	// ClientSideCacheable selects eligible unmarked calls for caching. It also
	// applies with a custom Cache. Supported by primary-only NewFailoverClient and NewFailoverClusterClient.
	// See CacheablePredicate for its callback contract.
	// Experimental: this API may change in a minor release.
	ClientSideCacheable CacheablePredicate
	// ClientSideCacheStrategy selects the node clients' tracking strategy.
	// Supported by primary-only NewFailoverClient and NewFailoverClusterClient.
	// Experimental: this API may change in a minor release.
	ClientSideCacheStrategy CSCStrategy
	// ClientSideCacheRefreshOnInvalidate enables node-local background refresh.
	// Supported by primary-only NewFailoverClient and NewFailoverClusterClient.
	// Experimental: this API may change in a minor release.
	ClientSideCacheRefreshOnInvalidate bool
	// ClientSideCacheCoalesceMisses coalesces concurrent node-local cache misses.
	// Supported by primary-only NewFailoverClient and NewFailoverClusterClient.
	// Experimental: this API may change in a minor release.
	ClientSideCacheCoalesceMisses bool
	// ClientSideCacheInvalidationBatchWindow batches node-local invalidations.
	// Supported by primary-only NewFailoverClient and NewFailoverClusterClient.
	// Experimental: this API may change in a minor release.
	ClientSideCacheInvalidationBatchWindow time.Duration

	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	DialTimeout time.Duration

	// DialerRetries is the maximum number of retry attempts when dialing fails.
	//
	// default: 5
	DialerRetries int

	// DialerRetryTimeout is the backoff duration between retry attempts.
	// Values <= 0 use the default; use DialerRetryBackoff to retry without delay.
	// A URL dialer_retry_timeout=0 sets DialerRetryBackoff, which takes precedence
	// over this field, including when this field is changed after parsing.
	//
	// default: 100 milliseconds
	DialerRetryTimeout time.Duration

	// DialerRetryBackoff controls the delay between dial retry attempts.
	// See Options.DialerRetryBackoff for details.
	DialerRetryBackoff func(attempt int) time.Duration

	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	ContextTimeoutEnabled bool

	// ReadBufferSize is the size of the bufio.Reader buffer for each connection.
	// Larger buffers can improve performance for commands that return large responses.
	// Smaller buffers can improve memory usage for larger pools.
	//
	// default: 32KiB (32768 bytes)
	ReadBufferSize int

	// WriteBufferSize is the size of the bufio.Writer buffer for each connection.
	// Larger buffers can improve performance for large pipelines and commands with many arguments.
	// Smaller buffers can improve memory usage for larger pools.
	//
	// default: 32KiB (32768 bytes)
	WriteBufferSize int

	// PipelineReadBufferSize, PipelineWriteBufferSize, PipelinePoolSize and
	// PipelineMinIdleConns configure the separate connection pool used for
	// pipelining, with its own (typically larger) buffers. See the same-named
	// fields on Options for details. NewFailoverClient creates this pool by default; set
	// PipelinePoolSize < 0 to opt out (pipelines then run on the main pool).
	PipelineReadBufferSize  int
	PipelineWriteBufferSize int
	PipelinePoolSize        int
	PipelineMinIdleConns    int

	// AutoPipelineOptions is the default config for the client's autopipeliner
	// faces. See Options.AutoPipelineOptions.
	AutoPipelineOptions *AutoPipelineOptions

	PoolFIFO bool

	PoolSize int

	// MaxConcurrentDials is the maximum number of concurrent connection creation goroutines.
	// If <= 0, defaults to PoolSize. If > PoolSize, it will be capped at PoolSize.
	MaxConcurrentDials int

	PoolTimeout           time.Duration
	MinIdleConns          int
	MaxIdleConns          int
	MaxActiveConns        int
	ConnMaxIdleTime       time.Duration
	ConnMaxLifetime       time.Duration
	ConnMaxLifetimeJitter time.Duration

	TLSConfig *tls.Config

	// DisableIndentity - Disable set-lib on connect.
	//
	// default: false
	//
	// Deprecated: Use DisableIdentity instead.
	DisableIndentity bool

	// DisableIdentity is used to disable CLIENT SETINFO command on connect.
	//
	// default: false
	DisableIdentity bool

	IdentitySuffix string

	// FailingTimeoutSeconds is the timeout in seconds for marking a cluster node as failing.
	// When a node is marked as failing, it will be avoided for this duration.
	// Only applies to failover cluster clients. Default is 15 seconds.
	FailingTimeoutSeconds int

	// Deprecated: All RediSearch commands now have stable RESP3 parsing and this
	// flag is a no-op. It is kept for backwards compatibility and will be removed
	// in a future release.
	UnstableResp3 bool

	// PushNotificationProcessor is the processor for handling push notifications.
	// If nil, a default processor will be created for RESP3 connections.
	PushNotificationProcessor push.NotificationProcessor

	// CommandMetadata configures shared metadata for failover clients.
	//
	// Experimental: this API may change in a minor release.
	CommandMetadata *CommandMetadataConfig

	// MaintNotificationsConfig is not supported for FailoverClients at the moment
	// MaintNotificationsConfig provides custom configuration for maintnotifications upgrades.
	// When MaintNotificationsConfig.Mode is not "disabled", the client will handle
	// upgrade notifications gracefully and manage connection/pool state transitions
	// seamlessly. Requires Protocol: 3 (RESP3) for push notifications.
	// If nil, maintnotifications upgrades are disabled.
	// (however if Mode is nil, it defaults to "auto" - enable if server supports it)
	// MaintNotificationsConfig *maintnotifications.Config

	// OTelRecorder records this client's metrics instead of the global recorder
	// installed by SetOTelRecorder. The failover client, its sentinel clients and their pools are bound to it
	// for their whole life. nil records to the global recorder.
	OTelRecorder OTelRecorder
}

func (opt *FailoverOptions) clientOptions() *Options {
	return &Options{
		ClientSideCacheConfig:                  opt.ClientSideCacheConfig,
		ClientSideCache:                        opt.ClientSideCache,
		ClientSideCacheTrackingMode:            opt.ClientSideCacheTrackingMode,
		ClientSideCacheable:                    opt.ClientSideCacheable,
		ClientSideCacheStrategy:                opt.ClientSideCacheStrategy,
		ClientSideCacheRefreshOnInvalidate:     opt.ClientSideCacheRefreshOnInvalidate,
		ClientSideCacheCoalesceMisses:          opt.ClientSideCacheCoalesceMisses,
		ClientSideCacheInvalidationBatchWindow: opt.ClientSideCacheInvalidationBatchWindow,
		Addr:                                   "FailoverClient",
		ClientName:                             opt.ClientName,

		Dialer:    opt.Dialer,
		OnConnect: opt.OnConnect,

		DB:                           opt.DB,
		Protocol:                     opt.Protocol,
		Username:                     opt.Username,
		Password:                     opt.Password,
		AuthEmptyPassword:            opt.AuthEmptyPassword,
		CredentialsProvider:          opt.CredentialsProvider,
		CredentialsProviderContext:   opt.CredentialsProviderContext,
		StreamingCredentialsProvider: opt.StreamingCredentialsProvider,

		MaxRetries:      opt.MaxRetries,
		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

		ReadBufferSize:  opt.ReadBufferSize,
		WriteBufferSize: opt.WriteBufferSize,

		PipelineReadBufferSize:  opt.PipelineReadBufferSize,
		PipelineWriteBufferSize: opt.PipelineWriteBufferSize,
		PipelinePoolSize:        opt.PipelinePoolSize,
		PipelineMinIdleConns:    opt.PipelineMinIdleConns,
		AutoPipelineOptions:     opt.AutoPipelineOptions,

		DialTimeout:        opt.DialTimeout,
		DialerRetries:      opt.DialerRetries,
		DialerRetryTimeout: opt.DialerRetryTimeout,
		DialerRetryBackoff: opt.DialerRetryBackoff,
		ReadTimeout:        opt.ReadTimeout,
		WriteTimeout:       opt.WriteTimeout,

		ContextTimeoutEnabled: opt.ContextTimeoutEnabled,

		PoolFIFO:              opt.PoolFIFO,
		PoolSize:              opt.PoolSize,
		MaxConcurrentDials:    opt.MaxConcurrentDials,
		PoolTimeout:           opt.PoolTimeout,
		MinIdleConns:          opt.MinIdleConns,
		MaxIdleConns:          opt.MaxIdleConns,
		MaxActiveConns:        opt.MaxActiveConns,
		ConnMaxIdleTime:       opt.ConnMaxIdleTime,
		ConnMaxLifetime:       opt.ConnMaxLifetime,
		ConnMaxLifetimeJitter: opt.ConnMaxLifetimeJitter,

		TLSConfig: opt.TLSConfig,

		DisableIdentity:  opt.DisableIdentity,
		DisableIndentity: opt.DisableIndentity,

		IdentitySuffix:            opt.IdentitySuffix,
		UnstableResp3:             opt.UnstableResp3,
		PushNotificationProcessor: opt.PushNotificationProcessor,
		CommandMetadata:           opt.CommandMetadata,

		OTelRecorder: opt.OTelRecorder,

		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	}
}

func (opt *FailoverOptions) sentinelOptions(addr string) *Options {
	readTimeout, writeTimeout := opt.ReadTimeout, opt.WriteTimeout
	// Data sockets may skip deadlines entirely. Internal discovery must still
	// apply its context deadline, including the reconciliation task's bound.
	if readTimeout == -2 {
		readTimeout = -1
	}
	if writeTimeout == -2 {
		writeTimeout = -1
	}
	return &Options{
		Addr:       addr,
		ClientName: opt.ClientName,

		Dialer:    opt.Dialer,
		OnConnect: opt.OnConnect,

		DB:                0,
		Username:          opt.SentinelUsername,
		Password:          opt.SentinelPassword,
		AuthEmptyPassword: opt.SentinelAuthEmptyPassword,

		MaxRetries:      opt.MaxRetries,
		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

		// The sentinel client uses a 4KiB read/write buffer size.
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,

		DialTimeout:        opt.DialTimeout,
		DialerRetries:      opt.DialerRetries,
		DialerRetryTimeout: opt.DialerRetryTimeout,
		DialerRetryBackoff: opt.DialerRetryBackoff,
		ReadTimeout:        readTimeout,
		WriteTimeout:       writeTimeout,

		// Internal discovery must honor its bounded contexts even when data
		// commands intentionally ignore context deadlines.
		ContextTimeoutEnabled: true,

		PoolFIFO:              opt.PoolFIFO,
		PoolSize:              opt.PoolSize,
		MaxConcurrentDials:    opt.MaxConcurrentDials,
		PoolTimeout:           opt.PoolTimeout,
		MinIdleConns:          opt.MinIdleConns,
		MaxIdleConns:          opt.MaxIdleConns,
		MaxActiveConns:        opt.MaxActiveConns,
		ConnMaxIdleTime:       opt.ConnMaxIdleTime,
		ConnMaxLifetime:       opt.ConnMaxLifetime,
		ConnMaxLifetimeJitter: opt.ConnMaxLifetimeJitter,

		TLSConfig: opt.TLSConfig,

		DisableIdentity:  opt.DisableIdentity,
		DisableIndentity: opt.DisableIndentity,

		IdentitySuffix:            opt.IdentitySuffix,
		UnstableResp3:             opt.UnstableResp3,
		PushNotificationProcessor: opt.PushNotificationProcessor,

		OTelRecorder: opt.OTelRecorder,

		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	}
}

func (opt *FailoverOptions) clusterOptions() *ClusterOptions {
	return &ClusterOptions{
		ClientSideCacheConfig:                  opt.ClientSideCacheConfig,
		ClientSideCacheTrackingMode:            opt.ClientSideCacheTrackingMode,
		ClientSideCacheable:                    opt.ClientSideCacheable,
		ClientSideCacheStrategy:                opt.ClientSideCacheStrategy,
		ClientSideCacheRefreshOnInvalidate:     opt.ClientSideCacheRefreshOnInvalidate,
		ClientSideCacheCoalesceMisses:          opt.ClientSideCacheCoalesceMisses,
		ClientSideCacheInvalidationBatchWindow: opt.ClientSideCacheInvalidationBatchWindow,
		ClientName:                             opt.ClientName,

		Dialer:    opt.Dialer,
		OnConnect: opt.OnConnect,

		Protocol:                     opt.Protocol,
		Username:                     opt.Username,
		Password:                     opt.Password,
		AuthEmptyPassword:            opt.AuthEmptyPassword,
		CredentialsProvider:          opt.CredentialsProvider,
		CredentialsProviderContext:   opt.CredentialsProviderContext,
		StreamingCredentialsProvider: opt.StreamingCredentialsProvider,

		MaxRedirects: opt.MaxRetries,

		ReadOnly:                opt.ReplicaOnly,
		RouteByLatency:          opt.RouteByLatency,
		RouteByLatencyTolerance: opt.RouteByLatencyTolerance,
		RouteRandomly:           opt.RouteRandomly,

		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

		ReadBufferSize:  opt.ReadBufferSize,
		WriteBufferSize: opt.WriteBufferSize,

		PipelineReadBufferSize:  opt.PipelineReadBufferSize,
		PipelineWriteBufferSize: opt.PipelineWriteBufferSize,
		PipelinePoolSize:        opt.PipelinePoolSize,
		PipelineMinIdleConns:    opt.PipelineMinIdleConns,
		AutoPipelineOptions:     opt.AutoPipelineOptions,

		DialTimeout:        opt.DialTimeout,
		DialerRetries:      opt.DialerRetries,
		DialerRetryTimeout: opt.DialerRetryTimeout,
		DialerRetryBackoff: opt.DialerRetryBackoff,
		ReadTimeout:        opt.ReadTimeout,
		WriteTimeout:       opt.WriteTimeout,

		ContextTimeoutEnabled: opt.ContextTimeoutEnabled,

		PoolFIFO:              opt.PoolFIFO,
		PoolSize:              opt.PoolSize,
		MaxConcurrentDials:    opt.MaxConcurrentDials,
		PoolTimeout:           opt.PoolTimeout,
		MinIdleConns:          opt.MinIdleConns,
		MaxIdleConns:          opt.MaxIdleConns,
		MaxActiveConns:        opt.MaxActiveConns,
		ConnMaxIdleTime:       opt.ConnMaxIdleTime,
		ConnMaxLifetime:       opt.ConnMaxLifetime,
		ConnMaxLifetimeJitter: opt.ConnMaxLifetimeJitter,

		TLSConfig: opt.TLSConfig,

		DisableIdentity:           opt.DisableIdentity,
		DisableIndentity:          opt.DisableIndentity,
		IdentitySuffix:            opt.IdentitySuffix,
		FailingTimeoutSeconds:     opt.FailingTimeoutSeconds,
		PushNotificationProcessor: opt.PushNotificationProcessor,
		CommandMetadata:           opt.CommandMetadata,

		OTelRecorder: opt.OTelRecorder,

		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	}
}

// ParseFailoverURL parses a URL into FailoverOptions that can be used to connect to Redis.
// The URL must be in the form:
//
//	redis://<user>:<password>@<host>:<port>/<db_number>
//	or
//	rediss://<user>:<password>@<host>:<port>/<db_number>
//
// To add additional addresses, specify the query parameter, "addr" one or more times. e.g:
//
//	redis://<user>:<password>@<host>:<port>/<db_number>?addr=<host2>:<port2>&addr=<host3>:<port3>
//	or
//	rediss://<user>:<password>@<host>:<port>/<db_number>?addr=<host2>:<port2>&addr=<host3>:<port3>
//
// Most Option fields can be set using query parameters, with the following restrictions:
//   - field names are mapped using snake-case conversion: to set MaxRetries, use max_retries
//   - only scalar type fields are supported (bool, int, time.Duration)
//   - for time.Duration fields, values must be a valid input for time.ParseDuration();
//     additionally a plain integer as value (i.e. without unit) is interpreted as seconds
//   - to disable a duration field, use value less than or equal to 0; to use the default
//     value, leave the value blank or remove the parameter
//   - dialer_retry_timeout is an exception: 0 means no retry delay, while negative
//     values are rejected. Explicit zero sets DialerRetryBackoff to return 0.
//   - only the last value is interpreted if a parameter is given multiple times
//   - fields "network", "addr", "sentinel_username" and "sentinel_password" can only be set using other
//     URL attributes (scheme, host, userinfo, resp.), query parameters using these
//     names will be treated as unknown parameters
//   - unknown parameter names will result in an error
//   - use "skip_verify=true" to ignore TLS certificate validation
//
// Example:
//
//	redis://user:password@localhost:6789?master_name=mymaster&dial_timeout=3&read_timeout=6s&addr=localhost:6790&addr=localhost:6791
//	is equivalent to:
//	&FailoverOptions{
//		MasterName:  "mymaster",
//		Addr:        ["localhost:6789", "localhost:6790", "localhost:6791"]
//		DialTimeout: 3 * time.Second, // no time unit = seconds
//		ReadTimeout: 6 * time.Second,
//	}
func ParseFailoverURL(redisURL string) (*FailoverOptions, error) {
	u, err := url.Parse(redisURL)
	if err != nil {
		return nil, err
	}
	return setupFailoverConn(u)
}

func setupFailoverConn(u *url.URL) (*FailoverOptions, error) {
	o := &FailoverOptions{}

	o.SentinelUsername, o.SentinelPassword = getUserPassword(u)

	h, p := getHostPortWithDefaults(u)
	o.SentinelAddrs = append(o.SentinelAddrs, net.JoinHostPort(h, p))

	switch u.Scheme {
	case "rediss":
		o.TLSConfig = &tls.Config{ServerName: h, MinVersion: tls.VersionTLS12}
	case "redis":
		o.TLSConfig = nil
	default:
		return nil, fmt.Errorf("redis: invalid URL scheme: %s", u.Scheme)
	}

	f := strings.FieldsFunc(u.Path, func(r rune) bool {
		return r == '/'
	})
	switch len(f) {
	case 0:
		o.DB = 0
	case 1:
		var err error
		if o.DB, err = strconv.Atoi(f[0]); err != nil {
			return nil, fmt.Errorf("redis: invalid database number: %q", f[0])
		}
		// A negative index parses fine but makes every connection fail on
		// SELECT, so reject it when the URL is read.
		if o.DB < 0 {
			return nil, fmt.Errorf("redis: invalid database number: %q", f[0])
		}
	default:
		return nil, fmt.Errorf("redis: invalid URL path: %s", u.Path)
	}

	return setupFailoverConnParams(u, o)
}

func setupFailoverConnParams(u *url.URL, o *FailoverOptions) (*FailoverOptions, error) {
	q := queryOptions{q: u.Query()}

	o.MasterName = q.string("master_name")
	o.ClientName = q.string("client_name")
	o.RouteByLatency = q.bool("route_by_latency")
	o.RouteByLatencyTolerance = q.duration("route_by_latency_tolerance")
	o.RouteRandomly = q.bool("route_randomly")
	o.ReplicaOnly = q.bool("replica_only")
	o.UseDisconnectedReplicas = q.bool("use_disconnected_replicas")
	o.Protocol = q.int("protocol")
	o.Username = q.string("username")
	o.Password = q.string("password")
	o.AuthEmptyPassword = q.bool("auth_empty_password")
	o.SentinelAuthEmptyPassword = q.bool("sentinel_auth_empty_password")
	o.MaxRetries = q.int("max_retries")
	o.MinRetryBackoff = q.duration("min_retry_backoff")
	o.MaxRetryBackoff = q.duration("max_retry_backoff")
	o.DialTimeout = q.duration("dial_timeout")
	o.DialerRetries = q.int("dialer_retries")
	o.DialerRetryTimeout, o.DialerRetryBackoff = q.dialerRetryTimeout()
	o.ReadTimeout = q.duration("read_timeout")
	o.WriteTimeout = q.duration("write_timeout")
	o.ContextTimeoutEnabled = q.bool("context_timeout_enabled")
	o.ReadBufferSize = q.nonNegativeInt("read_buffer_size")
	o.WriteBufferSize = q.nonNegativeInt("write_buffer_size")
	o.PoolFIFO = q.bool("pool_fifo")
	o.PoolSize = q.nonNegativeInt("pool_size")
	o.MaxConcurrentDials = q.int("max_concurrent_dials")
	o.MinIdleConns = q.int("min_idle_conns")
	o.MaxIdleConns = q.int("max_idle_conns")
	o.MaxActiveConns = q.int("max_active_conns")
	// Pipeline pool (created by default): allow URL opt-out
	// (pipeline_pool_size=-1) / tuning, else rejected as unexpected options.
	o.PipelinePoolSize = q.int("pipeline_pool_size")
	o.PipelineMinIdleConns = q.int("pipeline_min_idle_conns")
	o.PipelineReadBufferSize = q.int("pipeline_read_buffer_size")
	o.PipelineWriteBufferSize = q.int("pipeline_write_buffer_size")
	o.ConnMaxLifetime = q.duration("conn_max_lifetime")
	if q.has("conn_max_lifetime_jitter") {
		o.ConnMaxLifetimeJitter = min(q.duration("conn_max_lifetime_jitter"), o.ConnMaxLifetime)
	}
	o.ConnMaxIdleTime = q.duration("conn_max_idle_time")
	o.PoolTimeout = q.duration("pool_timeout")
	// disableIdentity and identitySuffix are the original camelCase spellings;
	// the snake_case names match the other URL parsers and the ParseURL docs.
	if q.has("disable_identity") {
		o.DisableIdentity = q.bool("disable_identity")
	} else {
		o.DisableIdentity = q.bool("disableIdentity")
	}
	if q.has("identity_suffix") {
		o.IdentitySuffix = q.string("identity_suffix")
	} else {
		o.IdentitySuffix = q.string("identitySuffix")
	}
	o.UnstableResp3 = q.bool("unstable_resp3")

	if q.err != nil {
		return nil, q.err
	}

	if tmp := q.string("db"); tmp != "" {
		db, err := strconv.Atoi(tmp)
		if err != nil {
			return nil, fmt.Errorf("redis: invalid database number: %w", err)
		}
		if db < 0 {
			return nil, fmt.Errorf("redis: invalid database number: %q", tmp)
		}
		o.DB = db
	}

	addrs := q.strings("addr")
	for _, addr := range addrs {
		h, p, err := net.SplitHostPort(addr)
		if err != nil || h == "" || p == "" {
			return nil, fmt.Errorf("redis: unable to parse addr param: %s", addr)
		}

		o.SentinelAddrs = append(o.SentinelAddrs, net.JoinHostPort(h, p))
	}

	if o.TLSConfig != nil && q.has("skip_verify") {
		o.TLSConfig.InsecureSkipVerify = q.bool("skip_verify")
	}
	if q.err != nil {
		return nil, q.err
	}

	// any parameters left?
	if r := q.remaining(); len(r) > 0 {
		return nil, fmt.Errorf("redis: unexpected option: %s", strings.Join(r, ", "))
	}

	return o, nil
}

// NewFailoverClient returns a Redis client that uses Redis Sentinel
// for automatic failover. It's safe for concurrent use by multiple
// goroutines.
// Passing nil FailoverOptions will cause a panic.
func NewFailoverClient(failoverOpt *FailoverOptions) *Client {
	if failoverOpt == nil {
		panic("redis: NewFailoverClient nil options")
	}

	if failoverOpt.RouteByLatency {
		panic("to route commands by latency, use NewFailoverClusterClient")
	}
	if failoverOpt.RouteRandomly {
		panic("to route commands randomly, use NewFailoverClusterClient")
	}

	sentinelAddrs := make([]string, len(failoverOpt.SentinelAddrs))
	copy(sentinelAddrs, failoverOpt.SentinelAddrs)

	rand.Shuffle(len(sentinelAddrs), func(i, j int) {
		sentinelAddrs[i], sentinelAddrs[j] = sentinelAddrs[j], sentinelAddrs[i]
	})

	failover := &sentinelFailover{
		opt:           failoverOpt,
		sentinelAddrs: sentinelAddrs,
	}

	opt := failoverOpt.clientOptions().clone()
	opt.Dialer = masterReplicaDialer(failover, opt)
	if !failoverOpt.ReplicaOnly {
		opt.failover = failover
	} else if opt.ClientSideCacheConfig != nil || opt.ClientSideCache != nil {
		internal.Logger.Printf(context.Background(), "redis: client-side caching is disabled with ReplicaOnly")
		opt.ClientSideCacheConfig, opt.ClientSideCache = nil, nil
	}
	if opt.ClientSideCache != nil {
		opt.cscCacheScope = generateUniqueID() + cscNamespaceSep
	}
	opt.init()

	rdb := &Client{
		baseClient: &baseClient{
			cscPolicy: opt.cachePolicy(),
			apClosed:  &atomic.Bool{},
			opt:       opt,
			onClose:   &onCloseHooks{},
			himport:   newHImportRegistry(),
		},
	}
	opt.failoverOwner = rdb.baseClient
	rdb.rec, rdb.poolMetrics = clientRecorder(opt.OTelRecorder)
	rdb.init()

	// Registered first (at construction), so onClose.run's LIFO order invokes it LAST —
	// after any lazily-registered autopipeliner drain hook. The drain needs MasterAddr
	// (hence a live failover client) to dial a replacement conn for accepted-but-unsent
	// work; tearing the failover client down here first would make MasterAddr return
	// pool.ErrClosed and fail those replayable commands. See onCloseHooks.run.
	//
	// Registered BEFORE the pools exist, not after: with MinIdleConns > 0 the main
	// pool starts dialing as soon as it is created, and masterReplicaDialer then
	// builds the failover's Sentinel client and pubsub. If a later construction
	// step panics, the panic-cleanup defer below closes rdb, whose onClose hooks
	// must already include this one — otherwise those discovery resources outlive
	// the client nobody will ever hold (Copilot on #4002).
	rdb.onClose.register(onCloseHookIDSentinelFailover, failover.Close)
	// The listener owns failover, so it cannot be its own GC cleanup target.
	// The shared base stays alive through Conn/EphemeralConn pools as well as
	// Client clones. Keep cleanup non-blocking; Close joins reconciliation.
	runtime.AddCleanup(rdb.baseClient, func(f *sentinelFailover) { go func() { _ = f.Close() }() }, failover)
	owner := weak.Make(rdb.baseClient)
	failover.onFailover = func(ctx context.Context, _ string) {
		if client := owner.Value(); client != nil {
			if client.cmdMeta != nil {
				client.cmdMeta.invalidateLiveAndRequestRefresh()
			}
			if p, ok := client.connPool.(*pool.ConnPool); ok {
				_ = p.Filter(func(cn *pool.Conn) bool { return cn.DialGeneration() < failover.epoch.Load() })
			}
			if ref := client.loadPipelinePool(); ref != nil {
				_ = ref.pool.Filter(func(cn *pool.Conn) bool { return cn.DialGeneration() < failover.epoch.Load() })
			}
		}
	}

	// Close a partially-built client if any construction step below panics before
	// this constructor returns. Mirrors NewClient: the pools (and their MinIdleConns
	// dialing goroutines) are created below, and a later step can panic — a pipeline
	// pool whose PipelinePoolSize overflows int32, otel registration, or a push
	// processor rejecting handler registration. The panic propagates to the caller
	// (which may recover it), but rdb is never returned, so without this its pools
	// would leak with no reference left to Close them. Close is nil-safe for a
	// partially-built client and this defer does not recover, so the panic still
	// surfaces.
	built := false
	defer func() {
		if !built {
			_ = rdb.Close()
		}
	}()

	// Initialize push notification processor using shared helper
	// Use void processor by default for RESP2 connections
	rdb.pushProcessor = initializePushProcessor(opt)
	opt.PushNotificationProcessor = rdb.pushProcessor

	// Generate unique pool names for metrics
	uniqueID := generateUniqueID()
	mainPoolName := opt.Addr + "_" + uniqueID
	pubsubPoolName := opt.Addr + "_" + uniqueID + "_pubsub"

	// Assign the pool fields only AFTER the error check, mirroring NewClient.
	// newConnPool returns a nil *pool.ConnPool on error, and assigning that
	// straight to the pool.Pooler interface field would leave a typed-nil
	// interface that closeResources treats as present (its != nil check passes),
	// so the panic-cleanup defer above would nil-deref inside ConnPool.Close and
	// replace the intended "failed to create connection pool" panic. A local var
	// keeps the field nil on failure. (pubSubPool is a concrete *pool.PubSubPool
	// whose nil is caught correctly by the != nil check, but assign it the same
	// way to keep this constructor identical to NewClient.)
	connPool, err := newConnPool(opt, rdb.dialHook, mainPoolName, rdb.poolMetrics)
	if err != nil {
		panic(fmt.Errorf("redis: failed to create connection pool: %w", err))
	}
	rdb.connPool = connPool
	pubSubPool, err := newPubSubPool(opt, rdb.dialHook, pubsubPoolName, rdb.poolMetrics)
	if err != nil {
		panic(fmt.Errorf("redis: failed to create pubsub pool: %w", err))
	}
	rdb.pubSubPool = pubSubPool

	// Create the dedicated pipeline pool unconditionally, mirroring NewClient
	// via the shared buildPipelinePool helper. PipelinePoolSize < 0 opts out.
	if opt.PipelinePoolSize >= 0 {
		ref, err := rdb.buildPipelinePool(mainPoolName + "_pipeline")
		if err != nil {
			panic(fmt.Errorf("redis: failed to create pipeline connection pool: %w", err))
		}
		rdb.pipelinePool = ref
	}

	rdb.initStreamingCredentials()

	// Register pools for OTel async gauge metrics, matching NewClient (the
	// failover client previously registered none, so pool gauges were silent
	// for the identical standalone setup). The pipeline pool is nil when not
	// configured.
	otel.RegisterPools(rdb.recorder(), rdb.connPool, rdb.pubSubPool, rdb.getPipelinePool(), opt.Addr)

	rdb.initCacheAndMetadata()
	failover.mu.Lock()
	failover.cscActive = rdb.cscActive
	failover.ready = true
	hasSource := failover.masterAddr != ""
	failover.mu.Unlock()
	failover.cleanOldConnections(context.Background())
	// Prewarming may have selected an endpoint before CSC was attached. Only
	// that case needs a baseline here; otherwise preserve lazy discovery.
	if hasSource && rdb.cscTrackingRequested() {
		failover.enqueueReconcile(false)
	}

	built = true
	return rdb
}

func masterReplicaDialer(
	failover *sentinelFailover,
	opt *Options,
) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		var addr string
		var err error
		observedEpoch := failover.epoch.Load()

		if failover.opt.ReplicaOnly {
			addr, err = failover.RandomReplicaAddr(ctx)
		} else {
			addr, err = failover.MasterAddr(ctx)
			if err == nil {
				failover.switchMasterIfCurrent(ctx, addr, observedEpoch)
			}
		}
		if err != nil {
			return nil, err
		}
		if !failover.opt.ReplicaOnly {
			failover.mu.RLock()
			addr = failover.masterAddr
			epoch := failover.epoch.Load()
			failover.mu.RUnlock()
			pool.SetDialGeneration(ctx, epoch)
		}
		if failover.opt.Dialer != nil {
			return failover.opt.Dialer(ctx, network, addr)
		}

		netDialer := &net.Dialer{
			Timeout:         opt.DialTimeout,
			KeepAliveConfig: defaultKeepAliveConfig,
		}
		if failover.opt.TLSConfig == nil {
			return netDialer.DialContext(ctx, network, addr)
		}
		return (&tls.Dialer{NetDialer: netDialer, Config: failover.opt.TLSConfig}).DialContext(ctx, network, addr)
	}
}

//------------------------------------------------------------------------------

// SentinelClient is a client for a Redis Sentinel.
type SentinelClient struct {
	*baseClient
}

// NewSentinelClient returns a Redis Sentinel client.
// Passing nil Options will cause a panic.
func NewSentinelClient(opt *Options) *SentinelClient {
	if opt == nil {
		panic("redis: NewSentinelClient nil options")
	}
	// Clone like NewClient does: init() normalises fields, and construction must
	// not rewrite the caller's *Options. Without the clone, initialising the same
	// *Options twice turns a disabled DialTimeout (0 after the first init) into the
	// 5 second default on the second pass — and that same pointer is captured by
	// the first client's dialer.
	opt = opt.clone()
	fixedDialEndpoint := opt.Dialer == nil
	opt.init()
	c := &SentinelClient{
		baseClient: &baseClient{
			apClosed:          &atomic.Bool{},
			opt:               opt,
			fixedDialEndpoint: fixedDialEndpoint,
			onClose:           &onCloseHooks{},
		},
	}
	c.rec, c.poolMetrics = clientRecorder(opt.OTelRecorder)

	// Initialize push notification processor using shared helper
	// Use void processor for Sentinel clients
	c.pushProcessor = NewVoidPushNotificationProcessor()

	c.initHooks(hooks{
		dial:    c.baseClient.dial,
		process: c.baseClient.process,
	})

	// Generate unique pool names for metrics
	uniqueID := generateUniqueID()
	mainPoolName := opt.Addr + "_" + uniqueID
	pubsubPoolName := opt.Addr + "_" + uniqueID + "_pubsub"

	var err error
	c.connPool, err = newConnPool(opt, c.dialHook, mainPoolName, c.poolMetrics)
	if err != nil {
		panic(fmt.Errorf("redis: failed to create connection pool: %w", err))
	}
	c.pubSubPool, err = newPubSubPool(opt, c.dialHook, pubsubPoolName, c.poolMetrics)
	if err != nil {
		panic(fmt.Errorf("redis: failed to create pubsub pool: %w", err))
	}

	// Close unregisters them, so register here as NewClient does.
	otel.RegisterPools(c.recorder(), c.connPool, c.pubSubPool, nil, opt.Addr)

	return c
}

// GetPushNotificationHandler returns the handler for a specific push notification name.
// Returns nil if no handler is registered for the given name.
func (c *SentinelClient) GetPushNotificationHandler(pushNotificationName string) push.NotificationHandler {
	return c.pushProcessor.GetHandler(pushNotificationName)
}

// RegisterPushNotificationHandler registers a handler for a specific push notification name.
// Returns an error if a handler is already registered for this push notification name.
// If protected is true, the handler cannot be unregistered.
func (c *SentinelClient) RegisterPushNotificationHandler(pushNotificationName string, handler push.NotificationHandler, protected bool) error {
	return c.pushProcessor.RegisterHandler(pushNotificationName, handler, protected)
}

func (c *SentinelClient) Process(ctx context.Context, cmd Cmder) error {
	err := c.processHook(ctx, cmd)
	cmd.SetErr(err)
	return err
}

func (c *SentinelClient) pubSub() *PubSub {
	pubsub := &PubSub{
		opt: c.cloneOpt(),
		rec: c.rec,
		newConn: func(ctx context.Context, addr string, channels []string, onInit ...func(*pool.Conn)) (*pool.Conn, error) {
			return c.newPubSubConn(ctx, addr, channels, onInit...)
		},
		closeConn: func(cn *pool.Conn) error {
			// Untrack connection from PubSubPool
			c.pubSubPool.UntrackConn(cn)
			_ = cn.Close()
			return nil
		},
		pushProcessor: c.pushProcessor,
	}
	pubsub.init()

	return pubsub
}

// Ping is used to test if a connection is still alive, or to
// measure latency.
func (c *SentinelClient) Ping(ctx context.Context) *StringCmd {
	cmd := NewStringCmd(ctx, "ping")
	_ = c.Process(ctx, cmd)
	return cmd
}

// Subscribe subscribes the client to the specified channels.
// Channels can be omitted to create empty subscription.
func (c *SentinelClient) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe subscribes the client to the given patterns.
// Patterns can be omitted to create empty subscription.
func (c *SentinelClient) PSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.PSubscribe(ctx, channels...)
	}
	return pubsub
}

func (c *SentinelClient) GetMasterAddrByName(ctx context.Context, name string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "sentinel", "get-master-addr-by-name", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *SentinelClient) Sentinels(ctx context.Context, name string) *MapStringStringSliceCmd {
	cmd := NewMapStringStringSliceCmd(ctx, "sentinel", "sentinels", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Failover forces a failover as if the master was not reachable, and without
// asking for agreement to other Sentinels.
func (c *SentinelClient) Failover(ctx context.Context, name string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "sentinel", "failover", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Reset resets all the masters with matching name. The pattern argument is a
// glob-style pattern. The reset process clears any previous state in a master
// (including a failover in progress), and removes every replica and sentinel
// already discovered and associated with the master.
func (c *SentinelClient) Reset(ctx context.Context, pattern string) *IntCmd {
	cmd := NewIntCmd(ctx, "sentinel", "reset", pattern)
	_ = c.Process(ctx, cmd)
	return cmd
}

// FlushConfig forces Sentinel to rewrite its configuration on disk, including
// the current Sentinel state.
func (c *SentinelClient) FlushConfig(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "sentinel", "flushconfig")
	_ = c.Process(ctx, cmd)
	return cmd
}

// Master shows the state and info of the specified master.
func (c *SentinelClient) Master(ctx context.Context, name string) *MapStringStringCmd {
	cmd := NewMapStringStringCmd(ctx, "sentinel", "master", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Masters shows a list of monitored masters and their state.
func (c *SentinelClient) Masters(ctx context.Context) *SliceCmd {
	cmd := NewSliceCmd(ctx, "sentinel", "masters")
	_ = c.Process(ctx, cmd)
	return cmd
}

// Replicas shows a list of replicas for the specified master and their state.
func (c *SentinelClient) Replicas(ctx context.Context, name string) *MapStringStringSliceCmd {
	cmd := NewMapStringStringSliceCmd(ctx, "sentinel", "replicas", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// CkQuorum checks if the current Sentinel configuration is able to reach the
// quorum needed to failover a master, and the majority needed to authorize the
// failover. This command should be used in monitoring systems to check if a
// Sentinel deployment is ok.
func (c *SentinelClient) CkQuorum(ctx context.Context, name string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "ckquorum", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Monitor tells the Sentinel to start monitoring a new master with the specified
// name, ip, port, and quorum.
func (c *SentinelClient) Monitor(ctx context.Context, name, ip, port, quorum string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "monitor", name, ip, port, quorum)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Set is used in order to change configuration parameters of a specific master.
func (c *SentinelClient) Set(ctx context.Context, name, option, value string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "set", name, option, value)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Remove is used in order to remove the specified master: the master will no
// longer be monitored, and will totally be removed from the internal state of
// the Sentinel.
func (c *SentinelClient) Remove(ctx context.Context, name string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "remove", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

type sentinelFailover struct {
	opt *FailoverOptions

	sentinelAddrs []string

	onFailover func(ctx context.Context, addr string)
	onUpdate   func(ctx context.Context)
	// epoch is published with masterAddr under mu; pool admission only loads it.
	epoch atomic.Uint64
	// Fields below are protected by mu. ready publishes fully built pools.
	ready              bool
	configEpoch        uint64
	primaryRunID       string
	configKnown        bool
	cscActive          *atomic.Bool
	rediscover         string // Discredited Sentinel address; empty for ordinary discovery.
	roleWarned         bool
	reconcileWarned    bool
	reconcileRunning   bool
	reconcileAgain     bool
	reconcileUncertain bool
	subscribed         bool
	reconcileCancel    context.CancelFunc
	reconcileDone      chan struct{}

	mu         sync.RWMutex
	masterAddr string
	sentinel   *SentinelClient
	pubsub     *PubSub
	// closed is set by Close (under mu). Once set, MasterAddr and replicaAddrs
	// refuse to create a new SentinelClient/pubsub and return pool.ErrClosed.
	// Close runs as an onClose hook, which fires BEFORE a pool-sharing wrapper's
	// autopipeliner drain hook (registration order); that drain may dial through
	// masterReplicaDialer, and without this flag the dial would rebuild the
	// sentinel client + pubsub after the only cleanup had already run, leaking
	// them past the pool teardown.
	closed bool
}

func (c *sentinelFailover) Close() error {
	c.mu.Lock()
	c.closed = true
	cancel, done := c.reconcileCancel, c.reconcileDone
	if cancel != nil {
		cancel()
	}
	var err error
	if c.sentinel != nil {
		err = c.closeSentinel()
	}
	c.mu.Unlock()
	if done != nil {
		<-done
	}
	return err
}

func (c *sentinelFailover) closeSentinel() error {
	firstErr := c.pubsub.Close()
	c.pubsub = nil

	err := c.sentinel.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	c.sentinel = nil

	return firstErr
}

func (c *sentinelFailover) RandomReplicaAddr(ctx context.Context) (string, error) {
	if c.opt == nil {
		return "", errors.New("opt is nil")
	}

	addresses, err := c.replicaAddrs(ctx, false)
	if err != nil {
		return "", err
	}

	if len(addresses) == 0 && c.opt.UseDisconnectedReplicas {
		addresses, err = c.replicaAddrs(ctx, true)
		if err != nil {
			return "", err
		}
	}

	if len(addresses) == 0 {
		return c.MasterAddr(ctx)
	}
	return addresses[rand.Intn(len(addresses))], nil
}

func (c *sentinelFailover) MasterAddr(ctx context.Context) (string, error) {
	c.mu.RLock()
	sentinel := c.sentinel
	rediscover := c.rediscover
	c.mu.RUnlock()

	if sentinel != nil && rediscover == "" {
		addr, err := c.getMasterAddr(ctx, sentinel)
		if err != nil {
			if isContextError(ctx.Err()) {
				return "", err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: GetMasterAddrByName name=%q failed: %s",
				c.opt.MasterName, err)
		} else {
			return addr, nil
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	rediscover = c.rediscover
	if rediscover != "" && c.sentinel != nil {
		_ = c.closeSentinel()
	}
	if c.sentinel != nil {
		addr, err := c.getMasterAddr(ctx, c.sentinel)
		if err != nil {
			_ = c.closeSentinel()
			if isContextError(ctx.Err()) {
				return "", err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: GetMasterAddrByName name=%q failed: %s",
				c.opt.MasterName, err)
		} else {
			return addr, nil
		}
	}

	// Closed: do not rebuild the sentinel client (see sentinelFailover.closed).
	if c.closed {
		return "", pool.ErrClosed
	}

	// short circuit if no sentinels configured
	if len(c.sentinelAddrs) == 0 {
		return "", errors.New("redis: no sentinels configured")
	}

	type result struct {
		index int
		addr  string
		err   error
	}
	clients := make([]*SentinelClient, len(c.sentinelAddrs))
	results := make(chan result, len(clients))
	var wg sync.WaitGroup
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i, sentinelAddr := range c.sentinelAddrs {
		clients[i] = NewSentinelClient(c.opt.sentinelOptions(sentinelAddr))
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := c.getMasterAddr(queryCtx, clients[i])
			if err != nil && queryCtx.Err() == nil {
				internal.Logger.Printf(queryCtx, "sentinel: GetMasterAddrByName addr=%s, master=%q failed: %s", sentinelAddr, c.opt.MasterName, err)
			}
			results <- result{index: i, addr: addr, err: err}
		}()
	}

	selected, fallback := -1, result{index: -1}
	var masterAddr string
	var preferOther <-chan time.Time
	errs := make([]error, 0, len(clients))
collect:
	for received := range clients {
		var r result
		select {
		case r = <-results:
		case <-ctx.Done():
			break collect
		case <-preferOther:
			break collect
		}
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		if c.sentinelAddrs[r.index] == rediscover {
			// Give peers a bounded preference once a usable fallback exists.
			// Reserve time for selection and subscription within this budget.
			fallback = r
			if preferOther == nil && received+1 < len(clients) {
				wait := 500 * time.Millisecond
				if deadline, ok := ctx.Deadline(); ok {
					wait = min(wait, time.Until(deadline)/2)
				}
				timer := time.NewTimer(wait)
				defer timer.Stop()
				preferOther = timer.C
			}
			continue
		}
		selected, masterAddr = r.index, r.addr
		break
	}
	if selected == -1 {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
		} else if fallback.index != -1 {
			selected, masterAddr = fallback.index, fallback.addr
		}
	}
	cancel()
	for i, client := range clients {
		if i != selected {
			// Cancellation alone does not interrupt an established socket read.
			// Close losing clients before joining their discovery goroutines.
			_ = client.Close()
		}
	}
	wg.Wait()
	if selected != -1 {
		c.rediscover = ""
		c.sentinelAddrs[0], c.sentinelAddrs[selected] = c.sentinelAddrs[selected], c.sentinelAddrs[0]
		c.setSentinel(ctx, clients[selected])
		internal.Logger.Printf(ctx, "sentinel: selected addr=%s masterAddr=%s", c.sentinelAddrs[0], masterAddr)
		return masterAddr, nil
	}
	return "", fmt.Errorf("redis: all sentinels specified in configuration are unreachable: %w", errors.Join(errs...))
}

func (c *sentinelFailover) replicaAddrs(ctx context.Context, useDisconnected bool) ([]string, error) {
	c.mu.RLock()
	sentinel := c.sentinel
	c.mu.RUnlock()

	if sentinel != nil {
		addrs, err := c.getReplicaAddrs(ctx, sentinel)
		if err != nil {
			if isContextError(ctx.Err()) {
				return nil, err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: Replicas name=%q failed: %s",
				c.opt.MasterName, err)
		} else if len(addrs) > 0 {
			return addrs, nil
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sentinel != nil {
		addrs, err := c.getReplicaAddrs(ctx, c.sentinel)
		if err != nil {
			_ = c.closeSentinel()
			if isContextError(ctx.Err()) {
				return nil, err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: Replicas name=%q failed: %s",
				c.opt.MasterName, err)
		} else if len(addrs) > 0 {
			return addrs, nil
		} else if !useDisconnected {
			// No error and no replicas — valid steady state for master-only setups.
			// Preserve the sentinel connection for master discovery and failover
			// pub/sub monitoring. Only return early when useDisconnected is false;
			// when true, fall through to the discovery loop which passes
			// useDisconnected to parseReplicaAddrs (getReplicaAddrs hardcodes false).
			return []string{}, nil
		} else {
			// useDisconnected=true: close sentinel so the discovery loop can call
			// setSentinel if it finds disconnected replicas.
			_ = c.closeSentinel()
		}
	}

	// Closed: do not rebuild the sentinel client (see sentinelFailover.closed).
	if c.closed {
		return nil, pool.ErrClosed
	}

	var sentinelReachable bool

	for i, sentinelAddr := range c.sentinelAddrs {
		sentinel := NewSentinelClient(c.opt.sentinelOptions(sentinelAddr))

		replicas, err := sentinel.Replicas(ctx, c.opt.MasterName).Result()
		if err != nil {
			_ = sentinel.Close()
			if isContextError(ctx.Err()) {
				return nil, err
			}
			internal.Logger.Printf(ctx, "sentinel: Replicas master=%q failed: %s",
				c.opt.MasterName, err)
			continue
		}
		sentinelReachable = true
		addrs := parseReplicaAddrs(replicas, useDisconnected)
		if len(addrs) == 0 {
			_ = sentinel.Close()
			continue
		}
		// Push working sentinel to the top.
		c.sentinelAddrs[0], c.sentinelAddrs[i] = c.sentinelAddrs[i], c.sentinelAddrs[0]
		c.setSentinel(ctx, sentinel)

		return addrs, nil
	}

	if sentinelReachable {
		return nil, nil
	}
	return nil, errors.New("redis: all sentinels specified in configuration are unreachable")
}

func (c *sentinelFailover) getMasterAddr(ctx context.Context, sentinel *SentinelClient) (string, error) {
	addr, err := sentinel.GetMasterAddrByName(ctx, c.opt.MasterName).Result()
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(addr[0], addr[1]), nil
}

func (c *sentinelFailover) getReplicaAddrs(ctx context.Context, sentinel *SentinelClient) ([]string, error) {
	addrs, err := sentinel.Replicas(ctx, c.opt.MasterName).Result()
	if err != nil {
		internal.Logger.Printf(ctx, "sentinel: Replicas name=%q failed: %s",
			c.opt.MasterName, err)
		return nil, err
	}
	return parseReplicaAddrs(addrs, false), nil
}

func parseReplicaAddrs(addrs []map[string]string, keepDisconnected bool) []string {
	nodes := make([]string, 0, len(addrs))
	for _, node := range addrs {
		isDown := false
		if flags, ok := node["flags"]; ok {
			for _, flag := range strings.Split(flags, ",") {
				switch flag {
				case "s_down", "o_down":
					isDown = true
				case "disconnected":
					if !keepDisconnected {
						isDown = true
					}
				}
			}
		}
		if !isDown && node["ip"] != "" && node["port"] != "" {
			nodes = append(nodes, net.JoinHostPort(node["ip"], node["port"]))
		}
	}

	return nodes
}

func (c *sentinelFailover) trySwitchMaster(ctx context.Context, addr string) bool {
	return c.switchMasterIfCurrent(ctx, addr, c.epoch.Load())
}

func (c *sentinelFailover) switchMasterIfCurrent(ctx context.Context, addr string, observed uint64) bool {
	c.mu.RLock()
	unchanged := c.closed || c.epoch.Load() != observed || addr == c.masterAddr
	c.mu.RUnlock()
	if unchanged {
		return false
	}
	c.mu.Lock()
	if c.closed || c.epoch.Load() != observed || addr == c.masterAddr {
		c.mu.Unlock()
		return false
	}
	c.masterAddr = addr
	c.primaryRunID = ""
	// The version still fences older observers, but it describes the previous
	// endpoint. Establish the new baseline without retiring its fresh sockets.
	c.configKnown = false
	c.roleWarned = false
	c.epoch.Add(1)
	c.mu.Unlock()
	internal.Logger.Printf(ctx, "sentinel: new master=%q addr=%q", c.opt.MasterName, addr)
	c.cleanOldConnections(ctx)
	if c.trackingActive() {
		c.enqueueReconcile(false)
	}
	return true
}

// trackingActive borrows the successful CSC-attachment signal published after
// construction. Configured-but-disabled caches must not retire data sockets.
func (c *sentinelFailover) trackingActive() bool {
	c.mu.RLock()
	active := c.cscActive
	c.mu.RUnlock()
	return active != nil && active.Load()
}

// rejectRole makes the next discovery prefer a different observer. Repeated
// replies from an already retired source cannot disturb its replacement.
func (c *sentinelFailover) rejectRole(ctx context.Context, generation uint64, role, addr string) {
	c.mu.Lock()
	if c.closed || c.epoch.Load() != generation {
		c.mu.Unlock()
		return
	}
	if c.sentinel != nil {
		selected := c.sentinel.opt.Addr
		c.rediscover = selected
		if i := slices.Index(c.sentinelAddrs, selected); i >= 0 {
			copy(c.sentinelAddrs[i:], c.sentinelAddrs[i+1:])
			c.sentinelAddrs[len(c.sentinelAddrs)-1] = selected
		}
	}
	warn := !c.roleWarned
	c.roleWarned = true
	c.configKnown = false
	c.epoch.Add(1)
	c.mu.Unlock()
	if warn {
		internal.Logger.Printf(ctx, "sentinel: expected primary at %s, got role %q; rediscovering through Sentinels", addr, role)
	}
	c.cleanOldConnections(ctx)
	c.enqueueReconcile(false)
}

func (c *sentinelFailover) cleanOldConnections(ctx context.Context) {
	c.mu.RLock()
	callback, ready, addr := c.onFailover, c.ready, c.masterAddr
	c.mu.RUnlock()
	if ready && callback != nil {
		callback(ctx, addr)
	}
}

func (c *sentinelFailover) setSentinel(ctx context.Context, sentinel *SentinelClient) {
	if c.sentinel != nil {
		panic("not reached")
	}
	c.sentinel = sentinel
	c.discoverSentinels(ctx)

	c.pubsub = sentinel.Subscribe(ctx, "+switch-master", "+replica-reconf-done")
	go c.listen(c.pubsub)
}

func (c *sentinelFailover) discoverSentinels(ctx context.Context) {
	sentinels, err := c.sentinel.Sentinels(ctx, c.opt.MasterName).Result()
	if err != nil {
		internal.Logger.Printf(ctx, "sentinel: Sentinels master=%q failed: %s", c.opt.MasterName, err)
		return
	}
	for _, sentinel := range sentinels {
		ip, ok := sentinel["ip"]
		if !ok {
			continue
		}
		port, ok := sentinel["port"]
		if !ok {
			continue
		}
		if ip != "" && port != "" {
			sentinelAddr := net.JoinHostPort(ip, port)
			if !slices.Contains(c.sentinelAddrs, sentinelAddr) {
				internal.Logger.Printf(ctx, "sentinel: discovered new sentinel=%q for master=%q",
					sentinelAddr, c.opt.MasterName)
				c.sentinelAddrs = append(c.sentinelAddrs, sentinelAddr)
			}
		}
	}
}

func (c *sentinelFailover) listen(pubsub *PubSub) {
	ctx := context.Background()
	if c.onUpdate != nil {
		c.onUpdate(ctx)
	}
	for event := range pubsub.ChannelWithSubscriptions() {
		ctx = pubsub.getContext()
		switch msg := event.(type) {
		case *Subscription:
			// Exactly one channel's ACK schedules reconciliation, including on
			// resubscribe. The other channel's ACK must not duplicate the work.
			if msg.Kind == "subscribe" && msg.Channel == "+switch-master" && !c.opt.ReplicaOnly && c.onFailover != nil {
				c.mu.Lock()
				uncertain := c.subscribed
				c.subscribed = true
				c.mu.Unlock()
				if c.trackingActive() {
					c.enqueueReconcile(uncertain)
				}
			}
			continue
		case *Message:
			if msg.Channel == "+switch-master" {
				parts := strings.Fields(msg.Payload)
				if len(parts) != 5 || parts[0] != c.opt.MasterName {
					continue
				}
				// Discovery can select the promoted primary before MASTER has
				// caught up. Even an already-processed switch needs a new baseline.
				if !c.trySwitchMaster(ctx, net.JoinHostPort(parts[3], parts[4])) && c.trackingActive() {
					c.enqueueReconcile(false)
				}
			}
		}
		if c.onUpdate != nil {
			c.onUpdate(ctx)
		}
	}
}

func (c *sentinelFailover) enqueueReconcile(uncertain bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.reconcileUncertain = c.reconcileUncertain || uncertain
	if c.reconcileRunning {
		c.reconcileAgain = true
		return
	}
	c.reconcileRunning = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.reconcileCancel, c.reconcileDone = cancel, done
	go func() {
		defer close(done)
		defer cancel()
		for {
			// Internal discovery is bounded even when application socket
			// timeouts are disabled. Every reconnect shares this one task.
			c.mu.Lock()
			uncertain := c.reconcileUncertain
			c.reconcileUncertain = false
			c.mu.Unlock()
			attemptCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			c.reconcilePrimary(attemptCtx, uncertain)
			stop()
			c.mu.Lock()
			if c.closed || !c.reconcileAgain {
				c.reconcileRunning = false
				c.mu.Unlock()
				return
			}
			c.reconcileAgain = false
			c.mu.Unlock()
		}
	}()
}

func (c *sentinelFailover) reconcilePrimary(ctx context.Context, uncertain bool) {
	observed := c.epoch.Load()
	addr, err := c.MasterAddr(ctx)
	if err != nil {
		// A failed reconnect verification cannot establish that old tracking
		// still covers the current primary. Retire it only while tracking is active.
		if uncertain && c.trackingActive() {
			c.mu.Lock()
			changed := !c.closed && c.epoch.Load() == observed
			if changed {
				c.epoch.Add(1)
			}
			c.mu.Unlock()
			if changed {
				c.cleanOldConnections(ctx)
			}
		}
		return
	}
	c.mu.RLock()
	sentinel := c.sentinel
	c.mu.RUnlock()
	if sentinel == nil {
		return
	}
	// Unlike GET-MASTER-ADDR-BY-NAME, MASTER may still describe the old
	// address during promotion. Its runid is usable only for the same target.
	info, infoErr := sentinel.Master(ctx, c.opt.MasterName).Result()
	config, parseErr := strconv.ParseUint(info["config-epoch"], 10, 64)
	runID := ""
	if net.JoinHostPort(info["ip"], info["port"]) == addr {
		runID = info["runid"]
	}
	known := infoErr == nil && parseErr == nil && net.JoinHostPort(info["ip"], info["port"]) == addr
	active := c.trackingActive()
	c.mu.Lock()
	// A reconnect accepted during this baseline's I/O must be evaluated against
	// the previous baseline, not hidden by this task's newer observation.
	if c.closed || c.epoch.Load() != observed || !uncertain && c.reconcileUncertain {
		c.mu.Unlock()
		return
	}
	// Keep the high-water version when an observer is behind or its monitor
	// was reset. Such an observer cannot roll back the selected endpoint or
	// prove coverage across a subscription gap. A baseline refresh without a
	// gap does not, by itself, justify retiring any connection.
	older := known && config < c.configEpoch
	coverageChanged := uncertain && (!known || older || !c.configKnown) ||
		known && !older && c.configKnown && config != c.configEpoch ||
		!older && addr == c.masterAddr && runID != "" && c.primaryRunID != "" && runID != c.primaryRunID
	addressChanged := addr != c.masterAddr && !older
	changed := addressChanged || active && coverageChanged
	if addressChanged {
		c.masterAddr = addr
		c.primaryRunID = ""
		c.configKnown = false
		c.roleWarned = false
	}
	if known && !older {
		c.configEpoch, c.configKnown = config, true
		if runID != "" {
			c.primaryRunID = runID
		}
	}
	warn := active && uncertain && (!known || older) && !c.reconcileWarned
	if warn {
		c.reconcileWarned = true
	}
	if changed {
		c.epoch.Add(1)
	}
	c.mu.Unlock()
	if warn {
		internal.Logger.Printf(ctx, "sentinel: cannot verify configuration epoch; retiring existing data connections after resubscribe")
	}
	if changed {
		c.cleanOldConnections(ctx)
	}
}

//------------------------------------------------------------------------------

// NewFailoverClusterClient returns a client that supports routing read-only commands
// to a replica node.
// Passing nil FailoverOptions will cause a panic.
func NewFailoverClusterClient(failoverOpt *FailoverOptions) *ClusterClient {
	if failoverOpt == nil {
		panic("redis: NewFailoverClusterClient nil options")
	}

	sentinelAddrs := make([]string, len(failoverOpt.SentinelAddrs))
	copy(sentinelAddrs, failoverOpt.SentinelAddrs)

	failover := &sentinelFailover{
		opt:           failoverOpt,
		sentinelAddrs: sentinelAddrs,
	}

	opt := failoverOpt.clusterOptions()
	if failoverOpt.DB != 0 && failoverOpt.ClientSideCacheConfig != nil {
		// Set DB before construction so both connection initialization and the
		// CSC namespace agree. A SELECT in OnConnect is rejected by CSC.
		opt.NewClient = func(nodeOpt *Options) *Client {
			nodeOpt.DB = failoverOpt.DB
			return NewClient(nodeOpt)
		}
	} else if failoverOpt.DB != 0 {
		onConnect := opt.OnConnect
		opt.OnConnect = func(ctx context.Context, cn *Conn) error {
			if err := cn.Select(ctx, failoverOpt.DB).Err(); err != nil {
				return err
			}
			if onConnect != nil {
				return onConnect(ctx, cn)
			}
			return nil
		}
	}

	opt.ClusterSlots = func(ctx context.Context) ([]ClusterSlot, error) {
		masterAddr, err := failover.MasterAddr(ctx)
		if err != nil {
			return nil, err
		}

		nodes := []ClusterNode{{
			Addr: masterAddr,
		}}

		replicaAddrs, err := failover.replicaAddrs(ctx, false)
		if err != nil {
			return nil, err
		}

		for _, replicaAddr := range replicaAddrs {
			nodes = append(nodes, ClusterNode{
				Addr: replicaAddr,
			})
		}

		slots := []ClusterSlot{
			{
				Start: 0,
				End:   16383,
				Nodes: nodes,
			},
		}
		return slots, nil
	}

	c := NewClusterClient(opt)

	c.onClose.register(onCloseHookIDSentinelFailover, failover.Close)

	failover.mu.Lock()
	failover.onUpdate = func(ctx context.Context) {
		c.ReloadState(ctx)
	}
	failover.mu.Unlock()

	return c
}
