package redis

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/routing"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

const (
	minLatencyMeasurementInterval = 10 * time.Second
)

var (
	errClusterNoNodes                    = errors.New("redis: cluster has no nodes")
	errClusterMetadataMissingFingerprint = errors.New("redis: cluster command metadata has no server identity")
	errClusterCommandMetadataUnusable    = errors.New("redis: cluster command metadata is unusable")
	errClusterTopologyUnhealthy          = errors.New("redis: cluster topology contains a node that is not online")
	errNoWatchKeys                       = errors.New("redis: Watch requires at least one key")
	errWatchCrosslot                     = errors.New("redis: Watch requires all keys to be in the same slot")
)

// ClusterOptions are used to configure a cluster client and should be
// passed to NewClusterClient.
type ClusterOptions struct {
	// A seed list of host:port addresses of cluster nodes.
	Addrs []string

	// ClientName will execute the `CLIENT SETNAME ClientName` command for each conn.
	ClientName string

	// NewClient creates a cluster node client with provided name and options.
	// If NewClient is set by the user, the user is responsible for handling maintnotifications upgrades and push notifications.
	NewClient func(opt *Options) *Client

	// The maximum number of retries before giving up. Command is retried
	// on network errors and MOVED/ASK redirects.
	// Default is 3 retries.
	MaxRedirects int

	// Enables read-only commands on slave nodes.
	ReadOnly bool
	// Allows routing read-only commands to the closest master or slave node.
	// It automatically enables ReadOnly.
	RouteByLatency bool
	// Allows routing read-only commands to the random master or slave node.
	// It automatically enables ReadOnly.
	RouteRandomly bool

	// Optional function that returns cluster slots information.
	// It is useful to manually create cluster of standalone Redis servers
	// and load-balance read/write operations between master and slaves.
	// It can use service like ZooKeeper to maintain configuration information
	// and Cluster.ReloadState to manually trigger state reloading.
	// The returned topology is authoritative for all-shards and all-nodes
	// routing, including Sentinel-backed cluster clients.
	ClusterSlots func(context.Context) ([]ClusterSlot, error)

	// Following options are copied from Options struct.

	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)

	OnConnect func(ctx context.Context, cn *Conn) error

	Protocol                     int
	Username                     string
	Password                     string
	CredentialsProvider          func() (username string, password string)
	CredentialsProviderContext   func(ctx context.Context) (username string, password string, err error)
	StreamingCredentialsProvider auth.StreamingCredentialsProvider

	// MaxRetries is the maximum number of retries before giving up.
	// For ClusterClient, retries are disabled by default (set to -1),
	// because the cluster client handles all kinds of retries internally.
	// This is intentional and differs from the standalone Options default.
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	DialTimeout time.Duration

	// DialerRetries is the maximum number of retry attempts when dialing fails.
	//
	// default: 5
	DialerRetries int

	// DialerRetryTimeout is the backoff duration between retry attempts.
	//
	// default: 100 milliseconds
	DialerRetryTimeout time.Duration

	// DialerRetryBackoff controls the delay between dial retry attempts.
	// See Options.DialerRetryBackoff for details.
	DialerRetryBackoff func(attempt int) time.Duration

	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	ContextTimeoutEnabled bool

	// MaxConcurrentDials is the maximum number of concurrent connection creation goroutines.
	// If <= 0, defaults to PoolSize. If > PoolSize, it will be capped at PoolSize.
	MaxConcurrentDials int

	PoolFIFO              bool
	PoolSize              int // applies per cluster node and not for the whole cluster
	PoolTimeout           time.Duration
	MinIdleConns          int
	MaxIdleConns          int
	MaxActiveConns        int // applies per cluster node and not for the whole cluster
	ConnMaxIdleTime       time.Duration
	ConnMaxLifetime       time.Duration
	ConnMaxLifetimeJitter time.Duration

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

	// PipelineReadBufferSize, PipelineWriteBufferSize and PipelinePoolSize
	// configure an optional separate connection pool used for pipelining on
	// each node, with its own (typically larger) buffers. See the same-named
	// fields on Options for details. The pool is created only when PipelineReadBufferSize or PipelineWriteBufferSize is set (PipelinePoolSize alone does not enable it).
	PipelineReadBufferSize  int
	PipelineWriteBufferSize int
	PipelinePoolSize        int

	// AutoPipelineOptions is the default config for BOTH autopipeliner faces
	// (AutoPipeline and AsyncAutoPipeline), applied when they are called
	// without explicit options. See Options.AutoPipelineOptions.
	AutoPipelineOptions *AutoPipelineOptions

	TLSConfig *tls.Config

	// DisableRoutingPolicies disables the request/response policy routing system.
	// When disabled, all commands use the legacy routing behavior.
	// Experimental. Will be removed when shard picker is fully implemented.
	DisableRoutingPolicies bool

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

	IdentitySuffix string // Add suffix to client name. Default is empty.

	// Deprecated: All RediSearch commands now have stable RESP3 parsing and this
	// flag is a no-op. It is kept for backwards compatibility and will be removed
	// in a future release.
	UnstableResp3 bool

	// PushNotificationProcessor is the processor for handling push notifications.
	// If nil, a default processor will be created for RESP3 connections.
	PushNotificationProcessor push.NotificationProcessor

	// FailingTimeoutSeconds is the timeout in seconds for marking a cluster node as failing.
	// When a node is marked as failing, it will be avoided for this duration.
	// Default is 15 seconds.
	FailingTimeoutSeconds int

	// MaintNotificationsConfig provides custom configuration for maintnotifications upgrades.
	// When MaintNotificationsConfig.Mode is not "disabled", the client will handle
	// cluster upgrade notifications gracefully and manage connection/pool state
	// transitions seamlessly. Requires Protocol: 3 (RESP3) for push notifications.
	// If nil, maintnotifications upgrades are in "auto" mode and will be enabled if the server supports it.
	// The ClusterClient supports SMIGRATING and SMIGRATED notifications for cluster state management.
	// Individual node clients handle other maintenance notifications (MOVING, MIGRATING, etc.).
	MaintNotificationsConfig *maintnotifications.Config

	// CommandMetadata configures cluster-owned metadata for routing.
	//
	// Experimental: this API may change in a minor release.
	CommandMetadata *CommandMetadataConfig

	// ShardPicker is used to pick a shard when the request_policy is
	// ReqDefault and the command has no keys.
	ShardPicker routing.ShardPicker

	// ClusterStateReloadInterval is the interval for reloading the cluster state.
	// MOVED/ASK redirects still trigger an immediate reactive reload, so this
	// only bounds how stale a topology can get without traffic errors.
	// Default is 60 seconds.
	ClusterStateReloadInterval time.Duration
}

func (opt *ClusterOptions) init() {
	switch opt.MaxRedirects {
	case -1:
		opt.MaxRedirects = 0
	case 0:
		opt.MaxRedirects = 3
	}

	if opt.RouteByLatency || opt.RouteRandomly {
		opt.ReadOnly = true
	}

	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}
	if opt.DialerRetries == 0 {
		opt.DialerRetries = 5
	}
	if opt.DialerRetryTimeout == 0 {
		opt.DialerRetryTimeout = 100 * time.Millisecond
	}

	if opt.PoolSize == 0 {
		opt.PoolSize = 5 * runtime.GOMAXPROCS(0)
	}
	if opt.MaxConcurrentDials <= 0 {
		opt.MaxConcurrentDials = opt.PoolSize
	} else if opt.MaxConcurrentDials > opt.PoolSize {
		opt.MaxConcurrentDials = opt.PoolSize
	}
	if opt.ReadBufferSize == 0 {
		opt.ReadBufferSize = proto.DefaultBufferSize
	}
	if opt.WriteBufferSize == 0 {
		opt.WriteBufferSize = proto.DefaultBufferSize
	}

	switch opt.ReadTimeout {
	case -1:
		opt.ReadTimeout = 0
	case 0:
		opt.ReadTimeout = 5 * time.Second
	}
	switch opt.WriteTimeout {
	case -1:
		opt.WriteTimeout = 0
	case 0:
		opt.WriteTimeout = opt.ReadTimeout
	}

	if opt.MaxRetries == 0 {
		opt.MaxRetries = -1
	}
	switch opt.MinRetryBackoff {
	case -1:
		opt.MinRetryBackoff = 0
	case 0:
		opt.MinRetryBackoff = 10 * time.Millisecond
	}
	switch opt.MaxRetryBackoff {
	case -1:
		opt.MaxRetryBackoff = 0
	case 0:
		opt.MaxRetryBackoff = time.Second
	}

	if opt.NewClient == nil {
		opt.NewClient = NewClient
	}

	if opt.FailingTimeoutSeconds == 0 {
		opt.FailingTimeoutSeconds = 15
	}

	if opt.ShardPicker == nil {
		opt.ShardPicker = &routing.RoundRobinPicker{}
	}

	if opt.ClusterStateReloadInterval == 0 {
		opt.ClusterStateReloadInterval = 60 * time.Second
	}
}

// ParseClusterURL parses a URL into ClusterOptions that can be used to connect to Redis.
// The URL must be in the form:
//
//	redis://<user>:<password>@<host>:<port>
//	or
//	rediss://<user>:<password>@<host>:<port>
//
// To add additional addresses, specify the query parameter, "addr" one or more times. e.g:
//
//	redis://<user>:<password>@<host>:<port>?addr=<host2>:<port2>&addr=<host3>:<port3>
//	or
//	rediss://<user>:<password>@<host>:<port>?addr=<host2>:<port2>&addr=<host3>:<port3>
//
// Most Option fields can be set using query parameters, with the following restrictions:
//   - field names are mapped using snake-case conversion: to set MaxRetries, use max_retries
//   - only scalar type fields are supported (bool, int, time.Duration)
//   - for time.Duration fields, values must be a valid input for time.ParseDuration();
//     additionally a plain integer as value (i.e. without unit) is interpreted as seconds
//   - to disable a duration field, use value less than or equal to 0; to use the default
//     value, leave the value blank or remove the parameter
//   - only the last value is interpreted if a parameter is given multiple times
//   - fields "network", "addr", "username" and "password" can only be set using other
//     URL attributes (scheme, host, userinfo, resp.), query parameters using these
//     names will be treated as unknown parameters
//   - unknown parameter names will result in an error
//
// Example:
//
//	redis://user:password@localhost:6789?dial_timeout=3&read_timeout=6s&addr=localhost:6790&addr=localhost:6791
//	is equivalent to:
//	&ClusterOptions{
//		Addr:        ["localhost:6789", "localhost:6790", "localhost:6791"]
//		DialTimeout: 3 * time.Second, // no time unit = seconds
//		ReadTimeout: 6 * time.Second,
//	}
func ParseClusterURL(redisURL string) (*ClusterOptions, error) {
	o := &ClusterOptions{}

	u, err := url.Parse(redisURL)
	if err != nil {
		return nil, err
	}

	// add base URL to the array of addresses
	// more addresses may be added through the URL params
	h, p := getHostPortWithDefaults(u)
	o.Addrs = append(o.Addrs, net.JoinHostPort(h, p))

	// setup username, password, and other configurations
	o, err = setupClusterConn(u, h, o)
	if err != nil {
		return nil, err
	}

	return o, nil
}

// setupClusterConn gets the username and password from the URL and the query parameters.
func setupClusterConn(u *url.URL, host string, o *ClusterOptions) (*ClusterOptions, error) {
	switch u.Scheme {
	case "rediss":
		o.TLSConfig = &tls.Config{ServerName: host}
		fallthrough
	case "redis":
		o.Username, o.Password = getUserPassword(u)
	default:
		return nil, fmt.Errorf("redis: invalid URL scheme: %s", u.Scheme)
	}

	// retrieve the configuration from the query parameters
	o, err := setupClusterQueryParams(u, o)
	if err != nil {
		return nil, err
	}

	return o, nil
}

// setupClusterQueryParams converts query parameters in u to option value in o.
func setupClusterQueryParams(u *url.URL, o *ClusterOptions) (*ClusterOptions, error) {
	q := queryOptions{q: u.Query()}

	o.Protocol = q.int("protocol")
	o.ClientName = q.string("client_name")
	o.MaxRedirects = q.int("max_redirects")
	o.ReadOnly = q.bool("read_only")
	o.RouteByLatency = q.bool("route_by_latency")
	o.RouteRandomly = q.bool("route_randomly")
	o.MaxRetries = q.int("max_retries")
	o.MinRetryBackoff = q.duration("min_retry_backoff")
	o.MaxRetryBackoff = q.duration("max_retry_backoff")
	o.DialTimeout = q.duration("dial_timeout")
	o.DialerRetries = q.int("dialer_retries")
	o.DialerRetryTimeout = q.duration("dialer_retry_timeout")
	o.ReadTimeout = q.duration("read_timeout")
	o.WriteTimeout = q.duration("write_timeout")
	o.PoolFIFO = q.bool("pool_fifo")
	o.PoolSize = q.int("pool_size")
	o.MaxConcurrentDials = q.int("max_concurrent_dials")
	o.MinIdleConns = q.int("min_idle_conns")
	o.MaxIdleConns = q.int("max_idle_conns")
	o.MaxActiveConns = q.int("max_active_conns")
	// Pipeline pool (per node, created by default): allow URL opt-out
	// (pipeline_pool_size=-1) / tuning, else rejected as unexpected options.
	o.PipelinePoolSize = q.int("pipeline_pool_size")
	o.PipelineReadBufferSize = q.int("pipeline_read_buffer_size")
	o.PipelineWriteBufferSize = q.int("pipeline_write_buffer_size")
	o.PoolTimeout = q.duration("pool_timeout")
	o.ConnMaxLifetime = q.duration("conn_max_lifetime")
	if q.has("conn_max_lifetime_jitter") {
		o.ConnMaxLifetimeJitter = min(q.duration("conn_max_lifetime_jitter"), o.ConnMaxLifetime)
	}
	o.ConnMaxIdleTime = q.duration("conn_max_idle_time")
	o.FailingTimeoutSeconds = q.int("failing_timeout_seconds")

	if q.err != nil {
		return nil, q.err
	}

	// addr can be specified as many times as needed
	addrs := q.strings("addr")
	for _, addr := range addrs {
		h, p, err := net.SplitHostPort(addr)
		if err != nil || h == "" || p == "" {
			return nil, fmt.Errorf("redis: unable to parse addr param: %s", addr)
		}

		o.Addrs = append(o.Addrs, net.JoinHostPort(h, p))
	}

	// any parameters left?
	if r := q.remaining(); len(r) > 0 {
		return nil, fmt.Errorf("redis: unexpected option: %s", strings.Join(r, ", "))
	}

	return o, nil
}

func (opt *ClusterOptions) clientOptions() *Options {
	// Clone MaintNotificationsConfig to avoid sharing between cluster node clients
	var maintNotificationsConfig *maintnotifications.Config
	if opt.MaintNotificationsConfig != nil {
		configClone := *opt.MaintNotificationsConfig
		maintNotificationsConfig = &configClone
	}

	return &Options{
		ClientName: opt.ClientName,
		Dialer:     opt.Dialer,
		OnConnect:  opt.OnConnect,

		Protocol:                     opt.Protocol,
		Username:                     opt.Username,
		Password:                     opt.Password,
		CredentialsProvider:          opt.CredentialsProvider,
		CredentialsProviderContext:   opt.CredentialsProviderContext,
		StreamingCredentialsProvider: opt.StreamingCredentialsProvider,

		MaxRetries:      opt.MaxRetries,
		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

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
		ReadBufferSize:        opt.ReadBufferSize,
		WriteBufferSize:       opt.WriteBufferSize,

		PipelineReadBufferSize:  opt.PipelineReadBufferSize,
		PipelineWriteBufferSize: opt.PipelineWriteBufferSize,
		PipelinePoolSize:        opt.PipelinePoolSize,
		DisableIdentity:         opt.DisableIdentity,
		DisableIndentity:        opt.DisableIndentity,
		IdentitySuffix:          opt.IdentitySuffix,
		FailingTimeoutSeconds:   opt.FailingTimeoutSeconds,
		TLSConfig:               opt.TLSConfig,
		// If ClusterSlots is populated, then we probably have an artificial
		// cluster whose nodes are not in clustering mode (otherwise there isn't
		// much use for ClusterSlots config).  This means we cannot execute the
		// READONLY command against that node -- setting readOnly to false in such
		// situations in the options below will prevent that from happening.
		readOnly:                  opt.ReadOnly && opt.ClusterSlots == nil,
		UnstableResp3:             opt.UnstableResp3,
		MaintNotificationsConfig:  maintNotificationsConfig,
		PushNotificationProcessor: opt.PushNotificationProcessor,
	}
}

//------------------------------------------------------------------------------

type clusterNode struct {
	Client *Client

	latency    atomic.Uint32
	generation atomic.Uint32
	failing    atomic.Uint32
	loaded     atomic.Uint32

	// last time the latency measurement was performed for the node, stored in nanoseconds from epoch
	lastLatencyMeasurement atomic.Int64
}

func newClusterNodeWithNodeAddress(clOpt *ClusterOptions, addr, nodeAddress string) *clusterNode {
	opt := clOpt.clientOptions()
	opt.Addr = addr
	opt.NodeAddress = nodeAddress
	node := clusterNode{
		Client: clOpt.NewClient(opt),
	}

	node.latency.Store(math.MaxUint32)
	if clOpt.RouteByLatency {
		go node.updateLatency()
	}

	return &node
}

func (n *clusterNode) String() string {
	return n.Client.String()
}

func (n *clusterNode) Close() error {
	return n.Client.Close()
}

const maximumNodeLatency = 1 * time.Minute

func (n *clusterNode) updateLatency() {
	const numProbe = 10
	var dur uint64

	successes := 0
	for i := 0; i < numProbe; i++ {
		time.Sleep(time.Duration(10+rand.Intn(10)) * time.Millisecond)

		start := time.Now()
		err := n.Client.Ping(context.TODO()).Err()
		if err == nil {
			dur += uint64(time.Since(start) / time.Microsecond)
			successes++
		}
	}

	var latency float64
	if successes == 0 {
		// If none of the pings worked, set latency to some arbitrarily high value so this node gets
		// least priority.
		latency = float64(maximumNodeLatency / time.Microsecond)
	} else {
		latency = float64(dur) / float64(successes)
	}
	n.latency.Store(uint32(latency + 0.5))
	n.SetLastLatencyMeasurement(time.Now())
}

func (n *clusterNode) Latency() time.Duration {
	latency := n.latency.Load()
	return time.Duration(latency) * time.Microsecond
}

func (n *clusterNode) MarkAsFailing() {
	n.failing.Store(uint32(time.Now().Unix()))
	n.loaded.Store(0)
}

func (n *clusterNode) Failing() bool {
	timeout := int64(n.Client.opt.FailingTimeoutSeconds)

	failing := n.failing.Load()
	if failing == 0 {
		return false
	}
	if time.Now().Unix()-int64(failing) < timeout {
		return true
	}
	n.failing.Store(0)
	return false
}

func (n *clusterNode) Generation() uint32 {
	return n.generation.Load()
}

func (n *clusterNode) LastLatencyMeasurement() int64 {
	return n.lastLatencyMeasurement.Load()
}

func (n *clusterNode) SetGeneration(gen uint32) {
	for {
		v := n.generation.Load()
		if gen < v || n.generation.CompareAndSwap(v, gen) {
			break
		}
	}
}

func (n *clusterNode) SetLastLatencyMeasurement(t time.Time) {
	for {
		v := n.lastLatencyMeasurement.Load()
		if t.UnixNano() < v || n.lastLatencyMeasurement.CompareAndSwap(v, t.UnixNano()) {
			break
		}
	}
}

func (n *clusterNode) Loading() bool {
	loaded := n.loaded.Load()
	if loaded == 1 {
		return false
	}

	// check if the node is loading
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := n.Client.Ping(ctx).Err()
	loading := err != nil && isLoadingError(err)
	if !loading {
		n.loaded.Store(1)
	}
	return loading
}

//------------------------------------------------------------------------------

type clusterNodes struct {
	opt *ClusterOptions

	mu          sync.RWMutex
	addrs       []string
	nodes       map[string]*clusterNode
	activeAddrs []string
	closed      bool
	onNewNode   []func(rdb *Client)

	generation atomic.Uint32
}

func newClusterNodes(opt *ClusterOptions) *clusterNodes {
	return &clusterNodes{
		opt:   opt,
		addrs: opt.Addrs,
		nodes: make(map[string]*clusterNode),
	}
}

func (c *clusterNodes) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var firstErr error
	for _, node := range c.nodes {
		if err := node.Client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	c.nodes = nil
	c.activeAddrs = nil

	return firstErr
}

func (c *clusterNodes) OnNewNode(fn func(rdb *Client)) {
	c.mu.Lock()
	c.onNewNode = append(c.onNewNode, fn)
	c.mu.Unlock()
}

func (c *clusterNodes) Addrs() ([]string, error) {
	var addrs []string

	c.mu.RLock()
	closed := c.closed //nolint:ifshort
	if !closed {
		if len(c.activeAddrs) > 0 {
			addrs = make([]string, len(c.activeAddrs))
			copy(addrs, c.activeAddrs)
		} else {
			addrs = make([]string, len(c.addrs))
			copy(addrs, c.addrs)
		}
	}
	c.mu.RUnlock()

	if closed {
		return nil, pool.ErrClosed
	}
	if len(addrs) == 0 {
		return nil, errClusterNoNodes
	}
	return addrs, nil
}

func (c *clusterNodes) NextGeneration() uint32 {
	return c.generation.Add(1)
}

// GC removes unused nodes.
func (c *clusterNodes) GC(generation uint32) {
	var collected []*clusterNode

	c.mu.Lock()

	c.activeAddrs = c.activeAddrs[:0]
	now := time.Now()
	for addr, node := range c.nodes {
		if node.Generation() >= generation {
			c.activeAddrs = append(c.activeAddrs, addr)
			if c.opt.RouteByLatency && node.LastLatencyMeasurement() < now.Add(-minLatencyMeasurementInterval).UnixNano() {
				go node.updateLatency()
			}
			continue
		}

		delete(c.nodes, addr)
		collected = append(collected, node)
	}

	c.mu.Unlock()

	for _, node := range collected {
		_ = node.Client.Close()
	}
}

func (c *clusterNodes) GetOrCreate(addr string) (*clusterNode, error) {
	return c.GetOrCreateWithNodeAddress(addr, "")
}

func (c *clusterNodes) GetOrCreateWithNodeAddress(addr, nodeAddress string) (*clusterNode, error) {
	node, err := c.get(addr)
	if err != nil {
		return nil, err
	}
	if node != nil {
		return node, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	node, ok := c.nodes[addr]
	if ok {
		return node, nil
	}

	node = newClusterNodeWithNodeAddress(c.opt, addr, nodeAddress)
	for _, fn := range c.onNewNode {
		fn(node.Client)
	}

	c.addrs = appendIfNotExist(c.addrs, addr)
	c.nodes[addr] = node

	return node, nil
}

func (c *clusterNodes) get(addr string) (*clusterNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, pool.ErrClosed
	}
	return c.nodes[addr], nil
}

func (c *clusterNodes) All() ([]*clusterNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	cp := make([]*clusterNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		cp = append(cp, node)
	}
	return cp, nil
}

func (c *clusterNodes) Random() (*clusterNode, error) {
	addrs, err := c.Addrs()
	if err != nil {
		return nil, err
	}

	n := rand.Intn(len(addrs))
	return c.GetOrCreate(addrs[n])
}

//------------------------------------------------------------------------------

type clusterSlot struct {
	start int
	end   int
	nodes []*clusterNode
}

type clusterState struct {
	nodes   *clusterNodes
	Masters []*clusterNode
	Slaves  []*clusterNode

	// allMasters/allSlaves include zero-slot and unhealthy SHARDS nodes.
	// Masters/Slaves contain only online routing candidates.
	allMasters []*clusterNode
	allSlaves  []*clusterNode
	health     map[*clusterNode]string

	slots []*clusterSlot

	generation uint32
	createdAt  time.Time
}

func (c *clusterState) declaredMasters() []*clusterNode {
	if c.allMasters != nil {
		return c.allMasters
	}
	return c.Masters
}

func (c *clusterState) declaredSlaves() []*clusterNode {
	if c.allSlaves != nil {
		return c.allSlaves
	}
	return c.Slaves
}

func (c *clusterState) nodeOnline(node *clusterNode) bool {
	health, known := c.health[node]
	return !known || health == "online"
}

func (c *clusterState) requireOnline(nodes []*clusterNode) error {
	for _, node := range nodes {
		if !c.nodeOnline(node) {
			return fmt.Errorf("%w: %s is %s", errClusterTopologyUnhealthy, node, c.health[node])
		}
	}
	return nil
}

// sameClusterTopology compares slot-serving endpoints only. Transient health,
// zero-slot nodes, and SHARDS/SLOTS representation changes do not alter COMMAND
// metadata and must not trigger a live-metadata refresh loop.
func sameClusterTopology(a, b *clusterState) bool {
	if a == nil || b == nil || len(a.slots) != len(b.slots) {
		return false
	}
	for i, aSlot := range a.slots {
		bSlot := b.slots[i]
		if aSlot == nil || bSlot == nil || aSlot.start != bSlot.start || aSlot.end != bSlot.end ||
			len(aSlot.nodes) != len(bSlot.nodes) {
			return false
		}
		for j, aNode := range aSlot.nodes {
			bNode := bSlot.nodes[j]
			if aNode == nil || bNode == nil || aNode.Client == nil || bNode.Client == nil ||
				aNode.Client.opt.Addr != bNode.Client.opt.Addr ||
				aNode.Client.opt.NodeAddress != bNode.Client.opt.NodeAddress {
				return false
			}
		}
	}
	return true
}

func newClusterState(
	nodes *clusterNodes, slots []ClusterSlot, origin string,
) (*clusterState, error) {
	c := clusterState{
		nodes: nodes,

		slots: make([]*clusterSlot, 0, len(slots)),

		generation: nodes.NextGeneration(),
		createdAt:  time.Now(),
	}

	originHost, originPort, _ := net.SplitHostPort(origin)
	isLoopbackOrigin := isLoopback(originHost)

	for _, slot := range slots {
		var nodes []*clusterNode
		for i, slotNode := range slot.Nodes {
			// slotNode.Addr is the node address from CLUSTER SLOTS
			nodeAddress := slotNode.Addr
			addr := nodeAddress
			if !isLoopbackOrigin {
				addr = replaceLoopbackHost(addr, originHost)
			}
			// TLS-only clusters (`--port 0 --tls-port 6379`) report port 0
			// in CLUSTER SLOTS. Fall back to the origin port — by definition
			// reachable, since it is the port that returned this slot map.
			// See https://github.com/redis/go-redis/issues/3726.
			addr = replaceZeroPort(addr, originPort)

			node, err := c.nodes.GetOrCreateWithNodeAddress(addr, nodeAddress)
			if err != nil {
				return nil, err
			}

			node.SetGeneration(c.generation)
			nodes = append(nodes, node)

			if i == 0 {
				c.Masters = appendIfNotExist(c.Masters, node)
			} else {
				c.Slaves = appendIfNotExist(c.Slaves, node)
			}
		}
		if len(nodes) > 2 {
			sortClusterNodes(nodes[1:])
		}

		c.slots = append(c.slots, &clusterSlot{
			start: slot.Start,
			end:   slot.End,
			nodes: nodes,
		})
	}

	slices.SortFunc(c.slots, func(a, b *clusterSlot) int {
		return cmp.Compare(a.start, b.start)
	})
	sortClusterNodes(c.Masters)
	sortClusterNodes(c.Slaves)

	time.AfterFunc(time.Minute, func() {
		nodes.GC(c.generation)
	})

	return &c, nil
}

// newClusterStateFromShards converts CLUSTER SHARDS into clusterState.
func newClusterStateFromShards(
	nodes *clusterNodes,
	shards []ClusterShard,
	origin string,
	tlsEnabled bool,
) (*clusterState, error) {
	slots, err := clusterSlotsFromShards(shards, origin, tlsEnabled)
	if err != nil {
		return nil, err
	}
	state, err := newClusterState(nodes, slots, origin)
	if err != nil {
		return nil, err
	}
	state.Masters = nil
	state.Slaves = nil
	state.allMasters = make([]*clusterNode, 0, len(shards))
	state.allSlaves = make([]*clusterNode, 0)
	state.health = make(map[*clusterNode]string)
	originHost, originPort, _ := net.SplitHostPort(origin)
	for shardIndex, shard := range shards {
		for _, shardNode := range shard.Nodes {
			nodeAddress, addrErr := clusterShardNodeAddr(shardNode, origin, tlsEnabled)
			if addrErr != nil {
				return nil, fmt.Errorf("redis: invalid CLUSTER SHARDS node in shard %d: %w", shardIndex, addrErr)
			}
			addr := nodeAddress
			if !isLoopback(originHost) {
				addr = replaceLoopbackHost(addr, originHost)
			}
			addr = replaceZeroPort(addr, originPort)
			node, nodeErr := nodes.GetOrCreateWithNodeAddress(addr, nodeAddress)
			if nodeErr != nil {
				return nil, nodeErr
			}
			node.SetGeneration(state.generation)
			health := clusterShardNodeHealth(shardNode.Health)
			if health != "online" {
				state.health[node] = health
			}
			switch strings.ToLower(shardNode.Role) {
			case "master":
				state.allMasters = appendIfNotExist(state.allMasters, node)
				if health == "online" {
					state.Masters = appendIfNotExist(state.Masters, node)
				}
			case "replica", "slave":
				state.allSlaves = appendIfNotExist(state.allSlaves, node)
				if health == "online" {
					state.Slaves = appendIfNotExist(state.Slaves, node)
				}
			}
		}
	}
	sortClusterNodes(state.Masters)
	sortClusterNodes(state.Slaves)
	sortClusterNodes(state.allMasters)
	sortClusterNodes(state.allSlaves)
	return state, nil
}

func sortClusterNodes(nodes []*clusterNode) {
	slices.SortFunc(nodes, func(a, b *clusterNode) int {
		if byAddr := cmp.Compare(a.Client.opt.Addr, b.Client.opt.Addr); byAddr != 0 {
			return byAddr
		}
		return cmp.Compare(a.Client.opt.NodeAddress, b.Client.opt.NodeAddress)
	})
}

func clusterShardNodeHealth(raw string) string {
	health := strings.ToLower(raw)
	if health == "failed" {
		// Redis currently emits "fail"; accept the documented spelling too.
		return "fail"
	}
	return health
}

func clusterSlotsFromShards(shards []ClusterShard, origin string, tlsEnabled bool) ([]ClusterSlot, error) {
	slots := make([]ClusterSlot, 0, len(shards))
	for shardIndex, shard := range shards {
		var master *ClusterNode
		replicas := make([]ClusterNode, 0, len(shard.Nodes))
		for _, shardNode := range shard.Nodes {
			addr, err := clusterShardNodeAddr(shardNode, origin, tlsEnabled)
			if err != nil {
				return nil, fmt.Errorf("redis: invalid CLUSTER SHARDS node in shard %d: %w", shardIndex, err)
			}
			health := clusterShardNodeHealth(shardNode.Health)
			node := ClusterNode{ID: shardNode.ID, Addr: addr}
			switch strings.ToLower(shardNode.Role) {
			case "master":
				if master != nil {
					return nil, fmt.Errorf("redis: CLUSTER SHARDS shard %d has multiple masters", shardIndex)
				}
				master = &node
			case "replica", "slave":
				if health == "online" {
					replicas = append(replicas, node)
				}
			default:
				return nil, fmt.Errorf(
					"redis: CLUSTER SHARDS shard %d has unknown node role %q",
					shardIndex,
					shardNode.Role,
				)
			}
		}
		if master == nil {
			return nil, fmt.Errorf("redis: CLUSTER SHARDS shard %d has no master", shardIndex)
		}
		slices.SortFunc(replicas, func(a, b ClusterNode) int {
			if byAddr := cmp.Compare(a.Addr, b.Addr); byAddr != 0 {
				return byAddr
			}
			return cmp.Compare(a.ID, b.ID)
		})
		orderedNodes := make([]ClusterNode, 0, 1+len(replicas))
		orderedNodes = append(orderedNodes, *master)
		orderedNodes = append(orderedNodes, replicas...)
		for _, slotRange := range shard.Slots {
			if slotRange.Start < 0 || slotRange.End < slotRange.Start || slotRange.End >= 16384 {
				return nil, fmt.Errorf(
					"redis: CLUSTER SHARDS shard %d has invalid slot range %d-%d",
					shardIndex,
					slotRange.Start,
					slotRange.End,
				)
			}
			slots = append(slots, ClusterSlot{
				Start: int(slotRange.Start),
				End:   int(slotRange.End),
				Nodes: orderedNodes,
			})
		}
	}
	if len(slots) == 0 {
		return nil, errors.New("redis: CLUSTER SHARDS returned no slot ranges")
	}
	return slots, nil
}

func clusterShardNodeAddr(node Node, origin string, tlsEnabled bool) (string, error) {
	host := node.Endpoint
	if host == "?" {
		return "", errors.New("unknown endpoint")
	}
	if host == "" {
		var err error
		host, _, err = net.SplitHostPort(origin)
		if err != nil || host == "" {
			return "", errors.New("null endpoint without a usable origin address")
		}
	}
	port := node.Port
	if tlsEnabled && node.TLSPort > 0 {
		port = node.TLSPort
	}
	if port == 0 {
		_, originPort, err := net.SplitHostPort(origin)
		if err != nil {
			return "", errors.New("zero port without a usable origin address")
		}
		parsedPort, err := strconv.ParseInt(originPort, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid origin port %q", originPort)
		}
		port = parsedPort
	}
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid port %d", port)
	}
	return net.JoinHostPort(host, strconv.FormatInt(port, 10)), nil
}

func replaceLoopbackHost(nodeAddr, originHost string) string {
	nodeHost, nodePort, err := net.SplitHostPort(nodeAddr)
	if err != nil {
		return nodeAddr
	}

	nodeIP := net.ParseIP(nodeHost)
	if nodeIP == nil {
		return nodeAddr
	}

	if !nodeIP.IsLoopback() {
		return nodeAddr
	}

	// Use origin host which is not loopback and node port.
	return net.JoinHostPort(originHost, nodePort)
}

// replaceZeroPort substitutes originPort for a node port of "0", which is
// what CLUSTER SLOTS reports for TLS-only clusters started with
// `--port 0 --tls-port <port>`. Non-zero ports and addresses without a
// recoverable origin port are returned unchanged.
func replaceZeroPort(nodeAddr, originPort string) string {
	if originPort == "" || originPort == "0" {
		return nodeAddr
	}
	nodeHost, nodePort, err := net.SplitHostPort(nodeAddr)
	if err != nil || nodePort != "0" {
		return nodeAddr
	}
	return net.JoinHostPort(nodeHost, originPort)
}

// isLoopback returns true if the host is a loopback address.
// For IP addresses, it uses net.IP.IsLoopback().
// For hostnames, it recognizes well-known loopback hostnames like "localhost"
// and Docker-specific loopback patterns like "*.docker.internal".
func isLoopback(host string) bool {
	ip := net.ParseIP(host)
	if ip != nil {
		return ip.IsLoopback()
	}

	if strings.ToLower(host) == "localhost" {
		return true
	}

	if strings.HasSuffix(strings.ToLower(host), ".docker.internal") {
		return true
	}

	return false
}

func (c *clusterState) slotMasterNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) > 0 {
		if !c.nodeOnline(nodes[0]) {
			return nil, fmt.Errorf("%w: slot %d master %s is %s", errClusterTopologyUnhealthy, slot, nodes[0], c.health[nodes[0]])
		}
		return nodes[0], nil
	}
	return c.nodes.Random()
}

func (c *clusterState) slotSlaveNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	switch len(nodes) {
	case 0:
		return c.nodes.Random()
	case 1:
		return c.slotMasterNode(slot)
	case 2:
		slave := nodes[1]
		if c.nodeOnline(slave) && !slave.Failing() && !slave.Loading() {
			return slave, nil
		}
		return c.slotMasterNode(slot)
	default:
		var slave *clusterNode
		for i := 0; i < 10; i++ {
			n := rand.Intn(len(nodes)-1) + 1
			slave = nodes[n]
			if c.nodeOnline(slave) && !slave.Failing() && !slave.Loading() {
				return slave, nil
			}
		}

		// All slaves are loading - use master.
		return c.slotMasterNode(slot)
	}
}

func (c *clusterState) slotClosestNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) == 0 {
		return c.nodes.Random()
	}

	allNodesFailing := true
	var (
		closestNonFailingNode *clusterNode
		closestNode           *clusterNode
		minLatency            time.Duration
		minHealthyLatency     time.Duration
	)

	// setting the max possible duration as zerovalue for minlatency
	minLatency = time.Duration(math.MaxInt64)
	minHealthyLatency = time.Duration(math.MaxInt64)

	for _, n := range nodes {
		if !c.nodeOnline(n) {
			continue
		}
		latency := n.Latency()
		if closestNode == nil || latency < minLatency {
			closestNode = n
			minLatency = latency
		}
		if !n.Failing() && (closestNonFailingNode == nil || latency < minHealthyLatency) {
			closestNonFailingNode = n
			minHealthyLatency = latency
			allNodesFailing = false
		}
	}

	// pick the healthly node with the lowest latency
	if !allNodesFailing && closestNonFailingNode != nil {
		return closestNonFailingNode, nil
	}

	// if all nodes are failing, we will pick the temporarily failing node with lowest latency
	if minLatency < maximumNodeLatency && closestNode != nil {
		internal.Logger.Printf(context.TODO(), "redis: all nodes are marked as failed, picking the temporarily failing node with lowest latency")
		return closestNode, nil
	}

	if closestNode != nil {
		internal.Logger.Printf(context.TODO(), "redis: pings to all online shard nodes are failing, picking the closest candidate")
		return closestNode, nil
	}
	return nil, errClusterTopologyUnhealthy
}

func (c *clusterState) slotRandomNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) == 0 {
		return c.nodes.Random()
	}
	if len(nodes) == 1 {
		if !c.nodeOnline(nodes[0]) {
			return nil, errClusterTopologyUnhealthy
		}
		return nodes[0], nil
	}
	randomNodes := rand.Perm(len(nodes))
	for _, idx := range randomNodes {
		if node := nodes[idx]; c.nodeOnline(node) && !node.Failing() {
			return node, nil
		}
	}
	for _, idx := range randomNodes {
		if node := nodes[idx]; c.nodeOnline(node) {
			return node, nil
		}
	}
	return nil, errClusterTopologyUnhealthy
}

func (c *clusterState) slotShardPickerSlaveNode(slot int, shardPicker routing.ShardPicker) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) == 0 {
		return c.nodes.Random()
	}

	// nodes[0] is master, nodes[1:] are slaves
	// First, try all slave nodes for this slot using ShardPicker order
	slaves := nodes[1:]
	if len(slaves) > 0 {
		for i := 0; i < len(slaves); i++ {
			idx := shardPicker.Next(len(slaves))
			slave := slaves[idx]
			if c.nodeOnline(slave) && !slave.Failing() && !slave.Loading() {
				return slave, nil
			}
		}
	}

	// All slaves are failing or loading - return master
	return c.slotMasterNode(slot)
}

func (c *clusterState) slotNodes(slot int) []*clusterNode {
	i := sort.Search(len(c.slots), func(i int) bool {
		return c.slots[i].end >= slot
	})
	if i >= len(c.slots) {
		return nil
	}
	x := c.slots[i]
	if slot >= x.start && slot <= x.end {
		return x.nodes
	}
	return nil
}

//------------------------------------------------------------------------------

type clusterStateHolder struct {
	load func(ctx context.Context) (*clusterState, error)
	// beforeReload invalidates metadata before publishing a changed topology.
	beforeReload func(current, previous *clusterState)
	// onReload runs after publishing a loaded topology.
	onReload func(current, previous *clusterState)

	reloadInterval time.Duration
	state          atomic.Value
	publishMu      sync.Mutex
	reloading      atomic.Uint32
	reloadPending  atomic.Uint32 // set to 1 when reload is requested during active reload
}

func newClusterStateHolder(load func(ctx context.Context) (*clusterState, error), reloadInterval time.Duration) *clusterStateHolder {
	return &clusterStateHolder{
		load:           load,
		reloadInterval: reloadInterval,
	}
}

func (c *clusterStateHolder) Reload(ctx context.Context) (*clusterState, error) {
	state, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	c.publishMu.Lock()
	defer c.publishMu.Unlock()
	previous := c.state.Load()
	var previousState *clusterState
	if previous != nil {
		previousState = previous.(*clusterState)
	}
	if c.beforeReload != nil && previousState != nil && !sameClusterTopology(state, previousState) {
		c.beforeReload(state, previousState)
	}
	c.state.Store(state)
	if c.onReload != nil {
		c.onReload(state, previousState)
	}
	return state, nil
}

func (c *clusterStateHolder) LazyReload() {
	// If already reloading, mark that another reload is pending
	if !c.reloading.CompareAndSwap(0, 1) {
		c.reloadPending.Store(1)
		return
	}

	go func() {
		for {
			_, err := c.Reload(context.Background())
			if err != nil {
				c.reloadPending.Store(0)
				c.reloading.Store(0)
				return
			}

			// Clear pending flag after reload completes, before cooldown
			// This captures notifications that arrived during the reload
			c.reloadPending.Store(0)

			// Wait cooldown period
			time.Sleep(200 * time.Millisecond)

			// Check if another reload was requested during cooldown
			if c.reloadPending.Load() == 0 {
				// No pending reload, we're done
				c.reloading.Store(0)
				return
			}

			// Pending reload requested, loop to reload again
		}
	}()
}

func (c *clusterStateHolder) Get(ctx context.Context) (*clusterState, error) {
	v := c.state.Load()
	if v == nil {
		return c.Reload(ctx)
	}

	state := v.(*clusterState)
	if time.Since(state.createdAt) > c.reloadInterval {
		c.LazyReload()
	}
	return state, nil
}

func (c *clusterStateHolder) ReloadOrGet(ctx context.Context) (*clusterState, error) {
	state, err := c.Reload(ctx)
	if err == nil {
		return state, nil
	}
	return c.Get(ctx)
}

//------------------------------------------------------------------------------

// ClusterClient is a Redis Cluster client representing a pool of zero
// or more underlying connections. It's safe for concurrent use by
// multiple goroutines.
type ClusterClient struct {
	opt             *ClusterOptions
	nodes           *clusterNodes
	state           *clusterStateHolder
	cmdMeta         *commandMetadataStore
	cmdInfoResolver *commandInfoResolver
	// autoPipelineRouting preserves admission decisions through dispatch.
	// The completion callback removes entries from all exit paths.
	autoPipelineRouting sync.Map // map[Cmder]clusterRoutingDecision
	cmdable
	hooksMixin

	// himport is the cluster-wide HIMPORT fieldset registry, shared with
	// every node client (masters and replicas alike — roles change with the
	// topology) so any connection serving an HIMPORT SET can lazily replay
	// the PREPARE (see himport.go, himport_cluster.go).
	himport *himportRegistry

	autopipelinerMu     *sync.Mutex    // guards the autopipeliner fields against concurrent first-call creation
	autopipeliner       *AutoPipeliner // blocking face (ClusterClient.AutoPipeline)
	asyncAutopipeliner  *AutoPipeliner // deferred face (ClusterClient.AsyncAutoPipeline)
	autopipelinerClosed bool           // set by Close: refuse to resurrect a pipeliner on a closed client
}

// NewClusterClient returns a Redis Cluster client as described in
// https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec.
// Passing nil ClusterOptions will cause a panic.
func NewClusterClient(opt *ClusterOptions) *ClusterClient {
	if opt == nil {
		panic("redis: NewClusterClient nil options")
	}
	opt.init()

	c := &ClusterClient{
		opt:             opt,
		nodes:           newClusterNodes(opt),
		himport:         newHImportRegistry(),
		autopipelinerMu: &sync.Mutex{},
	}

	// Every node client shares the cluster-wide fieldset registry, replicas
	// included: a promoted replica's connections carry no prepared flags, so
	// the first HIMPORT SET routed to it replays the PREPARE lazily.
	c.nodes.OnNewNode(func(nodeClient *Client) {
		nodeClient.himport = c.himport
	})

	c.state = newClusterStateHolder(c.loadState, opt.ClusterStateReloadInterval)
	// The cluster owns one metadata store; node clients do not start workers.
	weakClient := weak.Make(c)
	metadataConfig := opt.CommandMetadata
	if opt.DisableRoutingPolicies {
		// Disabled policy routing must not use live metadata or overrides.
		metadataConfig = nil
	}
	c.cmdMeta = newCommandMetadataStoreForLive(metadataConfig, func(ctx context.Context) (commandMetadataFetchResult, error) {
		owner := weakClient.Value()
		if owner == nil {
			return commandMetadataFetchResult{}, pool.ErrClosed
		}
		metadata, err := owner.fetchCommandMetadata(ctx)
		runtime.KeepAlive(owner)
		return metadata, err
	})
	runtime.AddCleanup(c, func(store *commandMetadataStore) { store.signalStop() }, c.cmdMeta)
	c.state.beforeReload = func(_, _ *clusterState) {
		c.cmdMeta.beginParentSourceChange()
	}
	c.state.onReload = func(current, previous *clusterState) {
		if previous != nil && !sameClusterTopology(current, previous) {
			c.cmdMeta.finishParentSourceChange()
			return
		}
		if previous == nil {
			// The initial topology enables live refresh without invalidating its fetch.
			c.cmdMeta.requestRefresh()
		}
	}
	c.SetCommandInfoResolver(newCommandMetadataPolicyResolver(c.metadataView))

	c.cmdable = c.Process
	c.initHooks(hooks{
		dial:       nil,
		process:    c.process,
		pipeline:   c.processPipeline,
		txPipeline: c.processTxPipeline,
	})

	// Set up SMIGRATED notification handling for cluster state reload
	// When a node client receives a SMIGRATED notification, it should trigger
	// cluster state reload on the parent ClusterClient
	if opt.MaintNotificationsConfig != nil {
		c.nodes.OnNewNode(func(nodeClient *Client) {
			manager := nodeClient.GetMaintNotificationsManager()
			if manager != nil {
				manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
					// Log the migration details for now
					if internal.LogLevel.InfoOrAbove() {
						internal.Logger.Printf(ctx, "cluster: slots %v migrated to %s, reloading cluster state", slotRanges, hostPort)
					}
					// Currently we reload the entire cluster state
					// In the future, this could be optimized to reload only the specific slots
					c.state.LazyReload()
				})
			}
		})
	}

	return c
}

// Options returns read-only *ClusterOptions that were used to create the client.
// Any alteration of the returned *ClusterOptions may result in undefined behaviour.
func (c *ClusterClient) Options() *ClusterOptions {
	return c.opt
}

// ReloadState reloads cluster state. If available it calls ClusterSlots func
// to get cluster slots information.
func (c *ClusterClient) ReloadState(ctx context.Context) {
	c.state.LazyReload()
}

// Close closes the cluster client, releasing any open resources.
//
// It is rare to Close a ClusterClient, as the ClusterClient is meant
// to be long-lived and shared between many goroutines.
func (c *ClusterClient) Close() error {
	// Stop both cached autopipeliners (blocking and async faces) before
	// closing nodes, so its background flusher goroutines don't outlive the
	// client. AutoPipeliner.Close is idempotent and nil-safe here.
	c.autopipelinerMu.Lock()
	ap, async := c.autopipeliner, c.asyncAutopipeliner
	c.autopipeliner, c.asyncAutopipeliner = nil, nil
	c.autopipelinerClosed = true // getters refuse to resurrect on a closed client
	c.autopipelinerMu.Unlock()
	var firstErr error
	for _, p := range []*AutoPipeliner{ap, async} {
		if p != nil {
			if err := p.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	c.autoPipelineRouting.Range(func(key, _ interface{}) bool {
		c.autoPipelineRouting.Delete(key)
		return true
	})
	// Stop metadata before closing the pools it uses.
	c.cmdMeta.stopAndJoin()
	if err := c.nodes.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (c *ClusterClient) Process(ctx context.Context, cmd Cmder) error {
	err := c.processHook(ctx, cmd)
	cmd.SetErr(err)
	return err
}

func (c *ClusterClient) process(ctx context.Context, cmd Cmder) error {
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policyErr != nil && !c.opt.DisableRoutingPolicies {
		return decision.policyErr
	}
	slot := c.cmdSlotWithDecision(cmd, decision, -1)
	var node *clusterNode
	var moved bool
	var ask bool
	var lastErr error
	needsInitialNode := c.opt.DisableRoutingPolicies || decision.policy == nil ||
		decision.policy.Request == routing.ReqDefault
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		// MOVED and ASK responses are not transient errors that require retry delay; they
		// should be attempted immediately.
		if attempt > 0 && !moved && !ask {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		if node == nil && needsInitialNode {
			var err error
			if !c.opt.DisableRoutingPolicies && c.opt.ShardPicker != nil {
				node, err = c.cmdNodeWithShardPickerAndDecision(ctx, slot, c.opt.ShardPicker, decision)
			} else {
				node, err = c.cmdNodeWithDecision(ctx, slot, decision)
			}
			if err != nil {
				lastErr = err
				if c.reloadTopologyForRetry(ctx, err, attempt) {
					continue
				}
				return err
			}
		}

		if ask {
			ask = false
			pipe := node.Client.Pipeline()
			_ = pipe.Process(ctx, NewCmd(ctx, "asking"))
			_ = pipe.Process(ctx, cmd)
			_, lastErr = pipe.Exec(ctx)
		} else {
			if !c.opt.DisableRoutingPolicies {
				lastErr = c.routeAndRun(ctx, cmd, node, decision)
			} else {
				lastErr = node.Client.Process(ctx, cmd)
			}
		}

		// If there is no error - we are done.
		if lastErr == nil {
			return nil
		}
		var fanoutErr *clusterFanoutExecutionError
		if errors.As(lastErr, &fanoutErr) {
			return fanoutErr.err
		}
		if isReadOnly := isReadOnlyError(lastErr); isReadOnly || lastErr == pool.ErrClosed {
			if isReadOnly {
				c.state.LazyReload()
			}
			node = nil
			continue
		}

		// If slave is loading - pick another node.
		if c.opt.ReadOnly && isLoadingError(lastErr) {
			if node != nil {
				node.MarkAsFailing()
			}
			node = nil
			continue
		}

		var addr string
		moved, ask, addr = isMovedError(lastErr)
		if moved || ask {
			c.state.LazyReload()

			recordClusterRedirectMetric(ctx, ask)

			var err error
			node, err = c.nodes.GetOrCreate(addr)
			if err != nil {
				return err
			}
			continue
		}

		if shouldRetry(lastErr, cmd.readTimeout() == nil) && !cmd.NoRetry() {
			// First retry the same node.
			if attempt == 0 {
				continue
			}

			// Second try another node.
			if node != nil {
				node.MarkAsFailing()
			}
			node = nil
			continue
		}

		return lastErr
	}
	return lastErr
}

func (c *ClusterClient) OnNewNode(fn func(rdb *Client)) {
	c.nodes.OnNewNode(fn)
}

// ForEachMaster concurrently calls the fn on each master node in the cluster.
// It returns the first error if any.
func (c *ClusterClient) ForEachMaster(
	ctx context.Context,
	fn func(ctx context.Context, client *Client) error,
) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for _, master := range state.Masters {
		wg.Add(1)
		go func(node *clusterNode) {
			defer wg.Done()
			err := fn(ctx, node.Client)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(master)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// ForEachSlave concurrently calls the fn on each slave node in the cluster.
// It returns the first error if any.
func (c *ClusterClient) ForEachSlave(
	ctx context.Context,
	fn func(ctx context.Context, client *Client) error,
) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for _, slave := range state.Slaves {
		wg.Add(1)
		go func(node *clusterNode) {
			defer wg.Done()
			err := fn(ctx, node.Client)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(slave)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// ForEachShard concurrently calls the fn on each known node in the cluster.
// It returns the first error if any.
func (c *ClusterClient) ForEachShard(
	ctx context.Context,
	fn func(ctx context.Context, client *Client) error,
) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	worker := func(node *clusterNode) {
		defer wg.Done()
		err := fn(ctx, node.Client)
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}

	for _, node := range state.Masters {
		wg.Add(1)
		go worker(node)
	}
	for _, node := range state.Slaves {
		wg.Add(1)
		go worker(node)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// PoolStats returns accumulated connection pool stats.
func (c *ClusterClient) PoolStats() *PoolStats {
	var acc PoolStats
	var pipe pool.Stats
	havePipe := false

	state, _ := c.state.Get(context.TODO())
	if state == nil {
		return &acc
	}

	foldNode := func(client *Client) {
		s := client.connPool.Stats()
		acc.Hits += s.Hits
		acc.Misses += s.Misses
		acc.Timeouts += s.Timeouts
		acc.WaitCount += s.WaitCount
		acc.WaitDurationNs += s.WaitDurationNs

		acc.TotalConns += s.TotalConns
		acc.IdleConns += s.IdleConns
		acc.StaleConns += s.StaleConns

		// The dedicated pipeline pool is now created per node by default; fold its
		// stats into acc.PipelineStats so cluster monitoring reflects it too.
		if pp := client.getPipelinePool(); pp != nil {
			ps := pp.Stats()
			pipe.Hits += ps.Hits
			pipe.Misses += ps.Misses
			pipe.Timeouts += ps.Timeouts
			pipe.WaitCount += ps.WaitCount
			pipe.WaitDurationNs += ps.WaitDurationNs
			pipe.TotalConns += ps.TotalConns
			pipe.IdleConns += ps.IdleConns
			pipe.StaleConns += ps.StaleConns
			havePipe = true
		}
	}

	for _, node := range state.Masters {
		foldNode(node.Client)
	}
	for _, node := range state.Slaves {
		foldNode(node.Client)
	}

	if havePipe {
		acc.PipelineStats = &pipe
	}
	return &acc
}

func (c *ClusterClient) loadState(ctx context.Context) (*clusterState, error) {
	if c.opt.ClusterSlots != nil {
		slots, err := c.opt.ClusterSlots(ctx)
		if err != nil {
			return nil, err
		}
		return newClusterState(c.nodes, slots, "")
	}

	addrs, err := c.nodes.Addrs()
	if err != nil {
		return nil, err
	}

	var firstErr error

	for _, idx := range rand.Perm(len(addrs)) {
		addr := addrs[idx]

		node, err := c.nodes.GetOrCreate(addr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		shards, shardsErr := node.Client.ClusterShards(ctx).Result()
		if shardsErr == nil {
			state, stateErr := newClusterStateFromShards(
				c.nodes,
				shards,
				addr,
				c.opt.TLSConfig != nil,
			)
			if stateErr == nil {
				return state, nil
			}
			shardsErr = stateErr
		}

		// CLUSTER SLOTS remains a fallback but cannot authorize all_shards.
		slots, slotsErr := node.Client.ClusterSlots(ctx).Result()
		if slotsErr == nil {
			return newClusterState(c.nodes, slots, addr)
		}
		if firstErr == nil {
			if shardsErr != nil {
				firstErr = shardsErr
			} else {
				firstErr = slotsErr
			}
		}
	}

	/*
	 * No node is connectable. It's possible that all nodes' IP has changed.
	 * Clear activeAddrs to let client be able to re-connect using the initial
	 * setting of the addresses (e.g. [redis-cluster-0:6379, redis-cluster-1:6379]),
	 * which might have chance to resolve domain name and get updated IP address.
	 */
	c.nodes.mu.Lock()
	c.nodes.activeAddrs = nil
	c.nodes.mu.Unlock()

	return nil, firstErr
}

func (c *ClusterClient) Pipeline() Pipeliner {
	pipe := Pipeline{
		exec: pipelineExecer(c.processPipelineHook),
	}
	pipe.init()
	return &pipe
}

// clusterAutoPipelineOptions applies the cluster shard-count default: commands
// are routed to shards by slot (see installAutoPipelineSharding), so unlike a
// standalone client — which defaults to a single deep queue — a cluster client
// wants several shards to keep concurrent nodes' batches separate. The caller's
// config is copied before the default is filled in, never mutated.
func clusterAutoPipelineOptions(cfg *AutoPipelineOptions) *AutoPipelineOptions {
	c2 := *cfg
	if c2.NumShards == 0 {
		c2.NumShards = numAutoPipelineShards()
	}
	// A cluster always routes by slot, so per-key order holds regardless of shard
	// count; mark it so construction's NumShards ordering check (which targets
	// round-robin sharding) does not reject the cluster default or an explicit
	// NumShards on the deferred (async) face.
	c2.contentSharded = true
	return &c2
}

// AutoPipeline returns the blocking autopipeliner for this cluster client: each
// command call blocks until executed (drop-in shape) while the engine batches
// concurrent callers into pipelines. Commands keep per-goroutine order; across
// nodes, ordering is per key (slot routing keeps a key on one shard and node
// sub-pipelines execute concurrently). Use AutoPipelineWithOptions to override
// DefaultBlockingAutoPipelineOptions. Cached/shared; first call's config wins.
// Close it (or the client) to release its goroutines.
//
// It returns an error if the supplied config is invalid (e.g. MaxConcurrentBatches>1
// without Unordered, or a negative size); on error no instance is cached.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *ClusterClient) AutoPipeline() (*AutoPipeliner, error) {
	return c.AutoPipelineWithOptions(nil)
}

// AutoPipelineWithOptions is AutoPipeline with explicit options instead of
// ClusterOptions.AutoPipelineOptions / the default. Cached/shared; first call wins.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *ClusterClient) AutoPipelineWithOptions(config *AutoPipelineOptions) (*AutoPipeliner, error) {
	return getOrCreateAutoPipeliner(c.autopipelinerMu, &c.autopipeliner, &c.autopipelinerClosed, nil, config,
		func() *AutoPipelineOptions {
			if c.opt.AutoPipelineOptions != nil {
				return c.opt.AutoPipelineOptions
			}
			return DefaultBlockingAutoPipelineOptions()
		},
		func(cfg *AutoPipelineOptions) (*AutoPipeliner, error) {
			ap, err := newAutoPipeliner(c, clusterAutoPipelineOptions(cfg), true)
			if err != nil {
				return nil, err
			}
			c.installAutoPipelineSharding(ap)
			return ap, nil
		})
}

// installAutoPipelineSharding buckets single-node commands by admitted slot.
func (c *ClusterClient) installAutoPipelineSharding(ap *AutoPipeliner) {
	// Reject non-pipelineable commands before they can poison a shared batch.
	ap.setPreflight(func(ctx context.Context, cmd Cmder) error {
		decision := c.autoPipelineRoutingDecision(ctx, cmd)
		if c.opt.DisableRoutingPolicies {
			return nil
		}
		if err := c.pipelineRoutingError(cmd, decision); err != nil {
			c.autoPipelineRouting.Delete(cmd)
			return err
		}
		return nil
	})
	// Run invocation-specific routing outside merged batches.
	ap.setMustDivert(func(ctx context.Context, cmd Cmder) bool {
		decision := c.autoPipelineRoutingDecision(ctx, cmd)
		if c.opt.DisableRoutingPolicies {
			return false
		}
		return decision.policyErr == nil && decision.policy != nil &&
			decision.policy.Request == routing.ReqSpecial
	})
	ap.setCommandDone(func(cmd Cmder) {
		c.autoPipelineRouting.Delete(cmd)
	})

	const slots = 16384
	n := ap.numShards()
	ap.setShardFn(func(cmd Cmder) int {
		// Reuse the admission slot so refreshes cannot change the flush shard.
		var slot int
		if decision, ok := c.peekAutoPipelineRoutingDecision(cmd); ok {
			slot = c.cmdSlotWithDecision(cmd, decision, -1)
		} else {
			slot = c.cmdSlot(cmd, -1)
		}
		if slot < 0 {
			return 0
		}
		return slot * n / slots
	})
}

// AsyncAutoPipeline returns the deferred autopipeliner: command calls return
// immediately and the result accessors block. Submit a window then read results
// for the highest throughput. By default,
// ClusterOptions.AutoPipelineOptions is used if set, otherwise
// DefaultAutoPipelineOptions. Ordering across nodes is per key: slot routing
// keeps a key on one shard, and node sub-pipelines execute concurrently. Use
// AsyncAutoPipelineWithOptions to override. Cached/shared; first call's config wins.
//
// It returns an error if the supplied config is invalid (e.g. MaxConcurrentBatches>1
// without Unordered, or a negative size); on error no instance is cached.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *ClusterClient) AsyncAutoPipeline() (*AutoPipeliner, error) {
	return c.AsyncAutoPipelineWithOptions(nil)
}

// AsyncAutoPipelineWithOptions is AsyncAutoPipeline with an explicit config
// instead of ClusterOptions.AutoPipelineOptions / the default. Cached/shared.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *ClusterClient) AsyncAutoPipelineWithOptions(config *AutoPipelineOptions) (*AutoPipeliner, error) {
	return getOrCreateAutoPipeliner(c.autopipelinerMu, &c.asyncAutopipeliner, &c.autopipelinerClosed, nil, config,
		func() *AutoPipelineOptions {
			if c.opt.AutoPipelineOptions != nil {
				return c.opt.AutoPipelineOptions
			}
			return DefaultAutoPipelineOptions()
		},
		func(cfg *AutoPipelineOptions) (*AutoPipeliner, error) {
			ap, err := newAutoPipeliner(c, clusterAutoPipelineOptions(cfg), false)
			if err != nil {
				return nil, err
			}
			c.installAutoPipelineSharding(ap)
			return ap, nil
		})
}

func (c *ClusterClient) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

type clusterPipelineRouting struct {
	view      *commandMetadataView
	decisions map[Cmder]clusterRoutingDecision
}

func (c *ClusterClient) autoPipelineRoutingDecision(
	ctx context.Context,
	cmd Cmder,
) clusterRoutingDecision {
	if c.opt.DisableRoutingPolicies {
		// Disabled policy routing must not invoke custom or dynamic resolvers.
		c.autoPipelineRouting.Delete(cmd)
		return c.legacyRoutingDecision(cmd)
	}
	if decision, ok := c.peekAutoPipelineRoutingDecision(cmd); ok {
		return decision
	}
	decision := c.resolveCommandRoutingDecision(ctx, cmd)
	actual, _ := c.autoPipelineRouting.LoadOrStore(cmd, decision)
	return actual.(clusterRoutingDecision)
}

func (c *ClusterClient) peekAutoPipelineRoutingDecision(cmd Cmder) (clusterRoutingDecision, bool) {
	value, ok := c.autoPipelineRouting.Load(cmd)
	if !ok {
		return clusterRoutingDecision{}, false
	}
	return value.(clusterRoutingDecision), true
}

func (c *ClusterClient) takeAutoPipelineRoutingDecision(cmd Cmder) (clusterRoutingDecision, bool) {
	value, ok := c.autoPipelineRouting.LoadAndDelete(cmd)
	if !ok {
		return clusterRoutingDecision{}, false
	}
	return value.(clusterRoutingDecision), true
}

func (c *ClusterClient) resolvePipelineRouting(
	ctx context.Context,
	cmds []Cmder,
) *clusterPipelineRouting {
	route := &clusterPipelineRouting{
		view:      c.metadataView(),
		decisions: make(map[Cmder]clusterRoutingDecision, len(cmds)),
	}
	if c.opt.DisableRoutingPolicies {
		// Legacy routing does not consult policies or configured metadata.
		for _, cmd := range cmds {
			if _, duplicate := route.decisions[cmd]; !duplicate {
				route.decisions[cmd] = c.legacyRoutingDecision(cmd)
			}
		}
		return route
	}

	// Reuse AutoPipeline admission decisions through flush.
	unresolved := make([]Cmder, 0, len(cmds))
	for _, cmd := range cmds {
		if _, duplicate := route.decisions[cmd]; duplicate {
			continue
		}
		if decision, ok := c.takeAutoPipelineRoutingDecision(cmd); ok {
			route.decisions[cmd] = decision
			continue
		}
		unresolved = append(unresolved, cmd)
	}
	if len(unresolved) == 0 {
		return route
	}

	view := route.view
	var resolutions []commandRoutingResolution
	if c.cmdInfoResolver != nil {
		var err error
		resolutions, view, err = c.cmdInfoResolver.resolveCommandRoutingsWithView(ctx, unresolved, c.metadataView)
		if err != nil {
			internal.Logger.Printf(ctx, "getting live command metadata: %s", err)
		}
	}
	route.view = view
	for i, cmd := range unresolved {
		if i < len(resolutions) {
			resolution := resolutions[i]
			route.decisions[cmd] = c.routingDecisionWithMeta(
				cmd, view, resolution.policy, resolution.policyFromMetadata,
				resolution.meta, resolution.metaOK,
			)
			continue
		}
		route.decisions[cmd] = c.routingDecisionInView(ctx, cmd, view, nil)
	}
	return route
}

func (c *ClusterClient) pipelineDecision(
	ctx context.Context,
	cmd Cmder,
	route *clusterPipelineRouting,
) clusterRoutingDecision {
	if decision, ok := route.decisions[cmd]; ok {
		return decision
	}
	// Handle internal commands added after batch resolution.
	var resolution commandRoutingResolution
	if c.cmdInfoResolver != nil {
		resolution = c.cmdInfoResolver.getCommandRoutingInView(ctx, cmd, route.view)
		return c.routingDecisionWithMeta(
			cmd, route.view, resolution.policy, resolution.policyFromMetadata,
			resolution.meta, resolution.metaOK,
		)
	}
	return c.routingDecisionInView(ctx, cmd, route.view, nil)
}

func (c *ClusterClient) processPipeline(ctx context.Context, cmds []Cmder) error {
	// Only call time.Now() if pipeline operation duration callback is set to avoid overhead
	var operationStart time.Time
	pipelineOpDurationCallback := otel.GetPipelineOperationDurationCallback()
	if pipelineOpDurationCallback != nil {
		operationStart = time.Now()
	}
	totalAttempts := 0
	route := c.resolvePipelineRouting(ctx, cmds)

	cmdsMap := newCmdsMap()
	var mapErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		cmdsMap = newCmdsMap()
		mapErr = c.mapCmdsByNodeInView(ctx, cmdsMap, cmds, route)
		if mapErr == nil || !c.reloadTopologyForRetry(ctx, mapErr, attempt) {
			break
		}
		if err := internal.Sleep(ctx, c.retryBackoff(attempt+1)); err != nil {
			mapErr = err
			break
		}
	}
	if mapErr != nil {
		err := mapErr
		setCmdsErr(cmds, err)
		if pipelineOpDurationCallback != nil {
			operationDuration := time.Since(operationStart)
			pipelineOpDurationCallback(ctx, operationDuration, "PIPELINE", len(cmds), 1, err, nil, 0)
		}
		return err
	}

	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		totalAttempts++
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				setCmdsErr(cmds, err)
				if pipelineOpDurationCallback != nil {
					operationDuration := time.Since(operationStart)
					pipelineOpDurationCallback(ctx, operationDuration, "PIPELINE", len(cmds), totalAttempts, err, nil, 0)
				}
				return err
			}
		}

		failedCmds := newCmdsMap()
		var wg sync.WaitGroup

		for node, cmds := range cmdsMap.m {
			wg.Add(1)
			go func(node *clusterNode, cmds []Cmder) {
				defer wg.Done()
				c.processPipelineNode(ctx, node, cmds, failedCmds, route)
			}(node, cmds)
		}

		wg.Wait()
		if len(failedCmds.m) == 0 {
			break
		}
		cmdsMap = failedCmds
		lastErr = cmdsFirstErr(cmds)
	}

	// Record pipeline operation duration
	if pipelineOpDurationCallback != nil {
		operationDuration := time.Since(operationStart)
		finalErr := cmdsFirstErr(cmds)
		if finalErr == nil {
			finalErr = lastErr
		}
		pipelineOpDurationCallback(ctx, operationDuration, "PIPELINE", len(cmds), totalAttempts, finalErr, nil, 0)
	}

	return cmdsFirstErr(cmds)
}

func (c *ClusterClient) mapCmdsByNode(
	ctx context.Context,
	cmdsMap *cmdsMap,
	cmds []Cmder,
) error {
	return c.mapCmdsByNodeInView(ctx, cmdsMap, cmds, c.resolvePipelineRouting(ctx, cmds))
}

func (c *ClusterClient) mapCmdsByNodeInView(
	ctx context.Context,
	cmdsMap *cmdsMap,
	cmds []Cmder,
	route *clusterPipelineRouting,
) error {
	state, err := c.state.Get(ctx)
	if err != nil {
		return err
	}

	decisions := make([]clusterRoutingDecision, len(cmds))
	allReadOnly := c.opt.ReadOnly
	for i, cmd := range cmds {
		decisions[i] = c.pipelineDecision(ctx, cmd, route)
		if decisions[i].policyErr != nil && !c.opt.DisableRoutingPolicies {
			setCmdsErr(cmds, decisions[i].policyErr)
			return decisions[i].policyErr
		}
		if !decisions[i].readOnly {
			allReadOnly = false
		}
	}

	if allReadOnly {
		for i, cmd := range cmds {
			decision := decisions[i]
			if err := c.pipelineRoutingError(cmd, decision); err != nil {
				// All-or-nothing: a user Pipeline() relies on the whole batch
				// either dispatching or failing before anything executes, so a
				// non-pipelineable command fails the entire mapping pre-dispatch.
				// Autopipeline batches never reach here with such a command: the
				// cluster face rejects them at submit (see the preflight installed
				// by installAutoPipelineSharding), so one caller's bad command
				// cannot poison a merged batch.
				setCmdsErr(cmds, err)
				return err
			}
			slot := c.cmdSlotWithDecision(cmd, decision, -1)
			var node *clusterNode
			// For keyless commands (slot == -1), use ShardPicker if routing policies are enabled
			if slot == -1 && !c.opt.DisableRoutingPolicies && c.opt.ShardPicker != nil {
				if len(state.Masters) == 0 {
					return errClusterNoNodes
				}
				// For read-only keyless commands, pick from all nodes (masters + slaves).
				// Index directly instead of building a combined slice, which would
				// append into the shared snapshot's spare capacity and race.
				idx := c.opt.ShardPicker.Next(len(state.Masters) + len(state.Slaves))
				if idx < len(state.Masters) {
					node = state.Masters[idx]
				} else {
					node = state.Slaves[idx-len(state.Masters)]
				}
			} else {
				node, err = c.slotReadOnlyNode(state, slot)
				if err != nil {
					return err
				}
			}
			cmdsMap.Add(node, cmd)
		}
		return nil
	}

	for i, cmd := range cmds {
		decision := decisions[i]
		if err := c.pipelineRoutingError(cmd, decision); err != nil {
			// All-or-nothing: a user Pipeline() relies on the whole batch
			// either dispatching or failing before anything executes, so a
			// non-pipelineable command fails the entire mapping pre-dispatch.
			// Autopipeline batches never reach here with such a command: the
			// cluster face rejects them at submit (see the preflight installed
			// by installAutoPipelineSharding), so one caller's bad command
			// cannot poison a merged batch.
			setCmdsErr(cmds, err)
			return err
		}
		slot := c.cmdSlotWithDecision(cmd, decision, -1)
		var node *clusterNode
		// For keyless commands (slot == -1), use ShardPicker if routing policies are enabled
		if slot == -1 && !c.opt.DisableRoutingPolicies && c.opt.ShardPicker != nil {
			if len(state.Masters) == 0 {
				return errClusterNoNodes
			}
			idx := c.opt.ShardPicker.Next(len(state.Masters))
			node = state.Masters[idx]
		} else {
			node, err = state.slotMasterNode(slot)
			if err != nil {
				return c.noteTopologySelectionError(err)
			}
		}
		cmdsMap.Add(node, cmd)
	}
	return nil
}

func (c *ClusterClient) pipelineRoutingError(cmd Cmder, decision clusterRoutingDecision) error {
	if c.opt.DisableRoutingPolicies {
		return nil
	}
	if decision.policyErr != nil {
		return decision.policyErr
	}
	if decision.policy == nil || decision.policy.Request == routing.ReqDefault {
		return nil
	}
	if decision.policy.Request == routing.ReqMultiShard &&
		decision.metaOK && decision.meta.policy != nil &&
		decision.policy == decision.meta.policy &&
		pipelineMultiShardFitsOneSlot(cmd, decision) {
		return nil
	}
	if decision.policy.Request != routing.ReqDefault {
		return fmt.Errorf(
			"redis: cannot pipeline command %q with request policy ReqAllNodes/ReqAllShards/ReqMultiShard/ReqSpecial; Note: This behavior is subject to change in the future",
			cmd.Name(),
		)
	}
	return nil
}

// pipelineMultiShardFitsOneSlot allows an unchanged request when all keys share its slot.
func pipelineMultiShardFitsOneSlot(cmd Cmder, decision clusterRoutingDecision) bool {
	if !decision.planOK || !decision.plan.splittable || len(decision.plan.positions) == 0 {
		return false
	}
	slot := -1
	for _, pos := range decision.plan.positions {
		key, ok := routingArgText(cmd, pos)
		if !ok {
			return false
		}
		keySlot := clusterKeySlot(key)
		if slot == -1 {
			slot = keySlot
			continue
		}
		if keySlot != slot {
			return false
		}
	}
	// Require a complete plan that matches the admitted slot.
	return slot >= 0 && slot == decision.naturalSlot
}

func (c *ClusterClient) processPipelineNode(
	ctx context.Context,
	node *clusterNode,
	cmds []Cmder,
	failedCmds *cmdsMap,
	route *clusterPipelineRouting,
) {
	// This call runs on a per-node fan-out goroutine, so register it as an
	// executor of every deferred-face batch among cmds: a NODE-level hook
	// (OnNewNode — redisotel's tracing) reading a result before next() must
	// get the not-yet-executed view from the accessor guards instead of
	// blocking on a batch only this call chain completes (reproduced as a
	// permanent wedge with a rediscmd-shaped Err() peek).
	unregister := registerBatchExecutors(cmds)
	defer unregister()

	// executed guards against a node-level hook short-circuiting (returning
	// without calling next): the inner callback then never runs, and without
	// surfacing the chain's error the cluster pipeline would report success
	// for commands that were never sent.
	executed := false
	err := node.Client.withProcessPipelineHook(ctx, cmds, func(ctx context.Context, cmds []Cmder) error {
		executed = true
		// Acquire through the node's dedicated pipeline pool when one is
		// configured (Pipeline*BufferSize propagate to node clients via
		// clientOptions); withPipelineConn falls back to the main pool
		// otherwise, preserving the previous behavior. entered distinguishes
		// an acquisition failure (fn never ran) from an execution error.
		entered := false
		err := node.Client.withPipelineConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			entered = true
			return c.processPipelineNodeConn(ctx, node, cn, cmds, failedCmds, route)
		})
		if err != nil && !entered {
			if !isContextError(err) {
				node.MarkAsFailing()
			}
			_ = c.mapCmdsByNodeInView(ctx, failedCmds, cmds, route)
			setCmdsErr(cmds, err)
		}
		return err
	})
	if !executed {
		// A hook returned without calling next. If it supplied an error that is
		// a deliberate abort: set it and do not remap for retry (a retry would
		// re-run the same hook). If it returned nil it short-circuited
		// SUCCESSFULLY, having served the batch itself — the same thing a plain
		// Pipeline hook may do — so setCmdsErr(nil) leaves the values it set
		// intact (review finding by codex on #3942).
		setCmdsErr(cmds, err)
		return
	}
	if err != nil && cmdsFirstErr(cmds) == nil {
		// Post-next verdict from a node-level hook on an all-clean sub-batch:
		// the exec fully succeeded, so the error can only be the hook's own —
		// apply it, mirroring AutoPipeliner.dispatchCmds. On a mixed batch the
		// exec-recorded outcomes win (hooks conventionally echo next's error,
		// and stamping the echo would overwrite successful replies). No remap:
		// retrying would re-run the same hook.
		setCmdsErr(cmds, err)
	}
}

func (c *ClusterClient) processPipelineNodeConn(
	ctx context.Context,
	node *clusterNode,
	cn *pool.Conn,
	cmds []Cmder,
	failedCmds *cmdsMap,
	route *clusterPipelineRouting,
) error {
	// HIMPORT bookkeeping: pending discards for this session and PREPAREs
	// for registered fieldsets the batch references get written ahead of
	// the batch (see himport.go).
	injected := node.Client.himportInjectedCmds(ctx, cn, cmds)

	if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, cmds)
	}); err != nil {
		if isBadConn(err, false, node.Client.getAddr()) {
			node.MarkAsFailing()
		}
		if shouldRetry(err, true) && !cmdsContainNoRetry(cmds) {
			_ = c.mapCmdsByNodeInView(ctx, failedCmds, cmds, route)
		}
		setCmdsErr(cmds, err)
		return err
	}

	return cn.WithReader(c.context(ctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
		if err := node.Client.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			// Transport error with the batch replies unread: same handling
			// as a write error — the batch may be retried on a fresh
			// connection.
			if isBadConn(err, false, node.Client.getAddr()) {
				node.MarkAsFailing()
			}
			if shouldRetry(err, true) && !cmdsContainNoRetry(cmds) {
				_ = c.mapCmdsByNodeInView(ctx, failedCmds, cmds, route)
			}
			setCmdsErr(cmds, err)
			return err
		}
		err := c.pipelineReadCmdsInView(ctx, node, cn, rd, cmds, failedCmds, route)
		if err == nil || isRedisError(err) {
			node.Client.himportAfterBatch(cn, injected, cmds)
			// SETs of registered fieldsets that lost their session state
			// re-queue for the next attempt, which re-prepares lazily —
			// the cluster equivalent of himportRetryFailedSets, bounded by
			// the pipeline's attempt budget. A non-nil redis error here
			// means pipelineReadCmds already re-queued the whole batch
			// (retryable first-command error); adding the SETs again would
			// duplicate them in the next attempt.
			if err == nil {
				c.himportRequeueFailedSets(ctx, cmds, failedCmds)
			}
		}
		return err
	})
}

func (c *ClusterClient) pipelineReadCmdsInView(
	ctx context.Context,
	node *clusterNode,
	cn *pool.Conn,
	rd *proto.Reader,
	cmds []Cmder,
	failedCmds *cmdsMap,
	route *clusterPipelineRouting,
) error {
	for i, cmd := range cmds {
		// Drain any buffered RESP3 push notifications before reading each
		// reply — otherwise a push frame (e.g. a maintnotifications MOVING
		// notification) is consumed AS the command's reply and every
		// subsequent reply in the pipeline shifts by one command. The
		// standalone pipeline and the cluster TxPipeline read loops already
		// do this; this loop was the only push-blind reader, and the
		// autopipeliner routes all cluster traffic through it.
		if err := node.Client.processPendingPushNotificationWithReader(ctx, cn, rd); err != nil {
			internal.Logger.Printf(ctx, "push: error processing pending notifications before reading reply: %v", err)
		}
		err := cmd.readReply(rd)
		cmd.SetErr(err)

		if err == nil {
			continue
		}

		if c.checkMovedErr(ctx, cmd, err, failedCmds) {
			continue
		}

		if c.opt.ReadOnly && isBadConn(err, false, node.Client.getAddr()) {
			node.MarkAsFailing()
		}

		if !isRedisError(err) {
			if shouldRetry(err, true) && !cmdsContainNoRetry(cmds) {
				_ = c.mapCmdsByNodeInView(ctx, failedCmds, cmds, route)
			}
			setCmdsErr(cmds[i+1:], err)
			return err
		}
	}

	// rawErr: execution path; never await an async command's batch here.
	if err := cmds[0].rawErr(); err != nil && shouldRetry(err, true) && !cmdsContainNoRetry(cmds) {
		_ = c.mapCmdsByNodeInView(ctx, failedCmds, cmds, route)
		return err
	}

	return nil
}

func (c *ClusterClient) checkMovedErr(
	ctx context.Context, cmd Cmder, err error, failedCmds *cmdsMap,
) bool {
	moved, ask, addr := isMovedError(err)
	if !moved && !ask {
		return false
	}

	node, err := c.nodes.GetOrCreate(addr)
	if err != nil {
		return false
	}

	if moved {
		c.state.LazyReload()
		failedCmds.Add(node, cmd)
		return true
	}

	if ask {
		failedCmds.Add(node, NewCmd(ctx, "asking"), cmd)
		return true
	}

	panic("not reached")
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (c *ClusterClient) TxPipeline() Pipeliner {
	pipe := Pipeline{
		exec: func(ctx context.Context, cmds []Cmder) error {
			cmds = wrapMultiExec(ctx, cmds)
			return c.processTxPipelineHook(ctx, cmds)
		},
	}
	pipe.init()
	return &pipe
}

func (c *ClusterClient) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

// A cluster tx pipeline sends MULTI, c1..cN, EXEC — N+2 commands, or N+3 with a
// leading ASKING — and always receives exactly that many replies, so every
// redirect/abort path leaves the connection clean.
//
// Possible reply sequences:
//	1. Slot owned here, no migration:
//	     +OK,  +QUEUED x N,  *N (array of N results)   -> success
//	2. Slot already migrated away:
//	     +OK,  -MOVED x N,  -EXECABORT                 -> re-route whole tx
//	3. Slot in migrating state (still owned here, keys draining out). Per
//	   cmd, the queue reply is +QUEUED / -ASK / -TRYAGAIN (keys present /
//	   all gone / some gone); any -ASK or -TRYAGAIN dirties the tx, so
//	   EXEC is -EXECABORT. Still N+2 replies, like the cases above:
//	     +OK,  (+QUEUED|-ASK|-TRYAGAIN) x N,  -EXECABORT  -> follow first redirect
//	4. Narrow race (all +QUEUED, slot moves before EXEC):
//	     +OK,  +QUEUED x N,  -MOVED <slot> <addr>         -> re-route whole tx
//	5. Non-cluster command error (arity / ACL / unknown):
//	     +OK,  +QUEUED..., -ERR...,  -EXECABORT           -> surface, not retryable
//	6. Narrow race (all +QUEUED, slot still migrating, keys drain before EXEC):
//	     +OK,  +QUEUED x N,  -ASK / -TRYAGAIN             -> re-route on -ASK, back off on -TRYAGAIN
//
// EXEC reply — the reply that decides the outcome:
//
//	*N                    success; read N per-command results
//	-EXECABORT            a queue-stage command failed; follow the first queue
//	                      redirect (MOVED/ASK/TRYAGAIN), else surface the trigger
//	-MOVED <slot> <addr>  case 4; re-route whole tx to addr, reload topology
//	-ASK <slot> <addr>    race: slot entered migrating state; re-route to addr
//	                      with a top-level ASKING before MULTI
//	-TRYAGAIN             race: migrating with split keys, or slot being trimmed
//	                      (CLUSTER_REDIR_TRIMMING on a write); back off and retry
//	                      the whole tx (same node still owns it)
//	-CLUSTERDOWN          cluster degraded; back off and retry whole tx
//
// ASK retry: the ASKING flag is NOT cleared between commands inside a MULTI
// so one top-level ASKING before MULTI covers the whole tx and lets the importing
// slot serve at EXEC. ASKING placed inside the MULTI would be queued and leave
// the flag unset during queueing, so the keyed commands would still get MOVED.
//
// Out of scope: WATCH's null-array EXEC and -CROSSSLOT;
// cluster TxPipeline is not used with WATCH and cross-slot is rejected client-side.

type txOutcomeKind int

const (
	txSuccess       txOutcomeKind = iota // transaction executed; per-command results are set
	txRetryMoved                         // MOVED: reload topology and re-route the whole tx
	txRetryAsk                           // ASK: re-route to the target with a top-level ASKING
	txRetryTryAgain                      // TRYAGAIN: back off and re-route the whole tx
	txRetryConn                          // connection/write/read failure: re-route the whole tx
	txFatal                              // non-retryable error; surface to the caller
)

// txOutcome is the result of a single tx attempt. err is the error to report
// when the redirect/retry loop is exhausted (or the fatal error to surface);
// addr is the ASK target; execErr is the EXEC reply error used to mark
// aborted commands; unreadReplies forces the connection to be discarded
// when the read loop exited before consuming all N+2 replies, leaving bytes
// on the wire.
type txOutcome struct {
	kind          txOutcomeKind
	err           error
	addr          string
	execErr       error
	unreadReplies bool
}

// txRedirect records the first queue-stage redirect (MOVED/ASK/TRYAGAIN) seen
// while reading +QUEUED replies. Redis dirties and aborts the transaction on
// any such reply, so the EXEC reply will be EXECABORT and the client must
// follow the recorded redirect with the whole transaction.
type txRedirect struct {
	moved    bool
	ask      bool
	tryAgain bool
	addr     string
	err      error
}

// errTxDirtyConn forces releaseConn to discard a connection that may still have
// unread transaction replies on it (an early exit before consuming all N+2).
var errTxDirtyConn = errors.New("redis: connection has unread transaction replies")

func (c *ClusterClient) processTxPipeline(ctx context.Context, cmds []Cmder) (retErr error) {
	var operationStart time.Time
	pipelineOpDurationCallback := otel.GetPipelineOperationDurationCallback()
	if pipelineOpDurationCallback != nil {
		operationStart = time.Now()
	}
	totalAttempts := 0
	var lastErr error

	defer func() {
		if pipelineOpDurationCallback == nil {
			return
		}
		finalErr := cmp.Or(retErr, cmdsFirstErr(cmds), lastErr)
		pipelineOpDurationCallback(ctx, time.Since(operationStart), "MULTI", len(cmds), totalAttempts, finalErr, nil, 0)
	}()

	// Trim multi .. exec.
	cmds = cmds[1 : len(cmds)-1]
	if len(cmds) == 0 {
		return nil
	}

	// Resolve once so the transaction uses one metadata view.
	route := c.resolvePipelineRouting(ctx, cmds)
	keyedCmdsBySlot, err := c.slottedKeyedCommandsInRouting(ctx, cmds, route)
	if err != nil {
		setCmdsErr(cmds, err)
		return err
	}

	slot := -1
	switch len(keyedCmdsBySlot) {
	case 0:
		slot = hashtag.RandomSlot()
	case 1:
		for sl := range keyedCmdsBySlot {
			slot = sl
		}
	default:
		// TxPipeline does not support cross slot transaction.
		setCmdsErr(cmds, ErrCrossSlot)
		return ErrCrossSlot
	}

	var node *clusterNode
	asking := false
	// MOVED/ASK are routing changes, not transient failures: follow them immediately.
	redirected := false
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		totalAttempts++
		if attempt > 0 && !redirected {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				setCmdsErr(cmds, err)
				return err
			}
		}
		if node == nil {
			state, err := c.state.Get(ctx)
			if err == nil {
				node, err = state.slotMasterNode(slot)
			}
			if err != nil {
				err = c.noteTopologySelectionError(err)
				lastErr = err
				if c.reloadTopologyForRetry(ctx, err, attempt) {
					continue
				}
				setCmdsErr(cmds, err)
				return err
			}
		}

		outcome := c.processTxPipelineNode(ctx, node, cmds, asking)
		lastErr = outcome.err
		redirected = false
		switch outcome.kind {
		case txSuccess:
			return cmdsFirstErr(cmds)
		case txRetryMoved:
			// Route directly to the authoritative addr from the MOVED; the
			// cached slot state may be stale until LazyReload lands.
			redirected = true
			asking = false
			c.state.LazyReload()
			if node, err = c.nodes.GetOrCreate(outcome.addr); err != nil {
				setCmdsErr(cmds, err)
				return err
			}
		case txRetryAsk:
			redirected = true
			asking = true
			if node, err = c.nodes.GetOrCreate(outcome.addr); err != nil {
				setCmdsErr(cmds, err)
				return err
			}
		case txRetryTryAgain, txRetryConn:
			// Same node, fresh connection: TRYAGAIN comes from the migrating
			// source (still the owner), and a conn failure only needs a new
			// connection. Preserve a prior ASKING flag: if we followed an ASK
			// to the importing target, the retry must still send ASKING (the
			// slot is still importing). ASKING is harmless if the migration
			// has since completed, since the flag is only consulted for
			// importing slots.
		case txFatal:
			// Mark every queued-but-never-executed command with the abort
			// error; the command that triggered EXECABORT already has its
			// own error and keeps it, so callers can tell what went wrong.
			abortErr := cmp.Or(outcome.execErr, outcome.err)
			for _, cmd := range cmds {
				if cmd.Err() == nil {
					cmd.SetErr(abortErr)
				}
			}
			return lastErr
		}
	}

	if lastErr != nil {
		setCmdsErr(cmds, lastErr)
	}
	return cmdsFirstErr(cmds)
}

func (c *ClusterClient) slottedKeyedCommandsInRouting(
	ctx context.Context,
	cmds []Cmder,
	route *clusterPipelineRouting,
) (map[int][]Cmder, error) {
	if c.opt.DisableRoutingPolicies {
		return c.legacySlottedKeyedCommands(cmds), nil
	}
	cmdsSlots := map[int][]Cmder{}

	prefferedRandomSlot := -1
	for _, cmd := range cmds {
		decision := c.pipelineDecision(ctx, cmd, route)
		if err := c.txRoutingError(cmd, decision); err != nil {
			return nil, err
		}
		var positions []int
		if decision.metaOK && decision.meta.keyState == routingKeysKnown {
			plan, ok := routingResolveKeyPlan(decision.meta, cmd)
			if !ok {
				return nil, fmt.Errorf(
					"redis: cannot determine all key arguments for transaction command %s", cmd.Name(),
				)
			}
			positions = plan.positions
		} else if !decision.keyless && decision.firstKey > 0 {
			// An incomplete spec may still prove one routing key.
			positions = []int{decision.firstKey}
		}
		if len(positions) == 0 {
			continue
		}
		seenSlots := make(map[int]struct{}, len(positions))
		for _, pos := range positions {
			slot := c.cmdSlotWithPos(cmd, pos, prefferedRandomSlot)
			if slot < 0 {
				return nil, fmt.Errorf(
					"redis: cannot encode key argument at position %d for transaction command %s", pos, cmd.Name(),
				)
			}
			if prefferedRandomSlot == -1 {
				prefferedRandomSlot = slot
			}
			if _, seen := seenSlots[slot]; seen {
				continue
			}
			seenSlots[slot] = struct{}{}
			cmdsSlots[slot] = append(cmdsSlots[slot], cmd)
		}
	}

	return cmdsSlots, nil
}

func (c *ClusterClient) legacySlottedKeyedCommands(cmds []Cmder) map[int][]Cmder {
	cmdsSlots := map[int][]Cmder{}
	prefferedRandomSlot := -1
	for _, cmd := range cmds {
		pos := cmdFirstKeyPosWithInfo(cmd, nil)
		if pos == 0 {
			continue
		}
		slot := c.legacyCmdSlotWithPos(cmd, pos, prefferedRandomSlot)
		if prefferedRandomSlot == -1 {
			prefferedRandomSlot = slot
		}
		cmdsSlots[slot] = append(cmdsSlots[slot], cmd)
	}
	return cmdsSlots
}

func (c *ClusterClient) txRoutingError(cmd Cmder, decision clusterRoutingDecision) error {
	if c.opt.DisableRoutingPolicies {
		return nil
	}
	if decision.policyErr != nil {
		return decision.policyErr
	}
	if decision.policy == nil {
		return nil
	}
	switch decision.policy.Request {
	case routing.ReqDefault, routing.ReqMultiShard:
		return nil
	case routing.ReqAllNodes, routing.ReqAllShards:
		// PING remains connection-local inside MULTI.
		if decision.metaOK && decision.meta.tx&routingTransactionSingleNode != 0 {
			return nil
		}
	default:
	}
	return fmt.Errorf(
		"redis: cannot execute transaction command %q with request policy %s",
		cmd.Name(), decision.policy.Request,
	)
}

func (c *ClusterClient) processTxPipelineNode(
	ctx context.Context, node *clusterNode, cmds []Cmder, asking bool,
) *txOutcome {
	wire := wrapMultiExec(ctx, cmds)
	if asking {
		// ASKING must precede MULTI so the flag stays set for the whole tx.
		wire = append([]Cmder{NewCmd(ctx, "asking")}, wire...)
	}

	var outcome *txOutcome
	// executed guards against a node-level hook short-circuiting (returning
	// without calling next) — same treatment as processPipelineNode.
	executed := false
	chainErr := node.Client.withProcessPipelineHook(ctx, wire, func(ctx context.Context, wire []Cmder) error {
		executed = true
		// Acquire through the node's dedicated pipeline pool when configured
		// (same routing as processPipelineNode); withPipelineConn falls back
		// to the main pool otherwise. The inner fn's return value drives the
		// connection release exactly like the explicit releaseConn did:
		// redis errors keep the conn poolable, unread replies poison it.
		entered := false
		err := node.Client.withPipelineConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			entered = true
			outcome = c.processTxPipelineNodeConn(ctx, node, cn, wire, cmds, asking)
			connErr := outcome.err
			if isRedisError(outcome.err) {
				connErr = nil
			}
			if outcome.unreadReplies {
				connErr = errTxDirtyConn
			}
			return connErr
		})
		if !entered && err != nil {
			// Connection acquisition failed — fn never ran.
			if shouldRetry(err, true) && !cmdsContainNoRetry(cmds) {
				outcome = &txOutcome{kind: txRetryConn, err: err}
			} else {
				outcome = &txOutcome{kind: txFatal, err: err}
			}
		}
		return err
	})

	if !executed && chainErr != nil {
		// A node-level hook aborted with an error: surface its verdict. A hook
		// that returned nil short-circuited successfully (it served the batch),
		// which is legal for plain pipelines too, so it is not turned into a
		// fatal outcome (review finding by codex on #3942).
		outcome = &txOutcome{kind: txFatal, err: chainErr}
	}
	if outcome == nil {
		outcome = &txOutcome{kind: txFatal, err: fmt.Errorf("redis: tx pipeline produced no outcome")}
	}
	return outcome
}

func (c *ClusterClient) processTxPipelineNodeConn(
	ctx context.Context, node *clusterNode, cn *pool.Conn, wire []Cmder, cmds []Cmder, asking bool,
) *txOutcome {
	// HIMPORT bookkeeping: pending discards and PREPAREs for registered
	// fieldsets the transaction references get written ahead of the wire
	// batch (before ASKING/MULTI; the session state is visible at EXEC).
	injected := node.Client.himportInjectedCmds(ctx, cn, cmds)

	if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wire)
	}); err != nil {
		// Write failure: re-route the whole tx on a fresh connection.
		if shouldRetry(err, true) && !cmdsContainNoRetry(cmds) {
			return &txOutcome{kind: txRetryConn, err: err}
		}
		return &txOutcome{kind: txFatal, err: err}
	}

	var outcome *txOutcome
	readErr := cn.WithReader(c.context(ctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
		if err := node.Client.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			// Transport error with the tx replies unread; the batch was
			// written and may have committed — fatal, discard the conn.
			outcome = c.txReadFatal(err)
			return nil
		}
		outcome = c.readTxPipelineReplies(ctx, node, cn, rd, cmds, asking)
		if outcome != nil && outcome.kind == txSuccess {
			node.Client.himportAfterBatch(cn, injected, cmds)
		}
		return nil
	})

	if readErr != nil {
		// Reader-level failure (deadline setup, nil conn) around the read loop.
		// The batch was already written, so the server may have committed;
		// surface the error as fatal and discard the suspect connection rather
		// than re-executing the transaction.
		return c.txReadFatal(readErr)
	}
	return outcome
}

// readTxPipelineReplies reads the replies of one MULTI..EXEC unit and
// classifies the outcome. The reply count always matches the number of sent
// commands, so success/redirect paths leave the connection clean; only an early
// MULTI read failure can leave unread replies.
func (c *ClusterClient) readTxPipelineReplies(
	ctx context.Context, node *clusterNode, cn *pool.Conn, rd *proto.Reader, cmds []Cmder, asking bool,
) *txOutcome {
	scratch := NewStatusCmd(ctx)

	readStatus := func() error {
		c.txProcessPush(ctx, node, cn, rd)
		return scratch.readReply(rd)
	}

	// Optional top-level ASKING reply (+OK, or a retryable error such as -LOADING).
	if asking {
		if err := readStatus(); err != nil {
			return c.txPreQueueErrorOutcome(err, cmds)
		}
	}

	// MULTI reply (+OK, or an error such as -LOADING during failover).
	if err := readStatus(); err != nil {
		return c.txPreQueueErrorOutcome(err, cmds)
	}

	// Queue replies: +QUEUED, or a redirect / command error that dirties the tx.
	var firstRedirect *txRedirect
	var firstFatal error
	for _, cmd := range cmds {
		err := readStatus()
		if err == nil {
			continue // +QUEUED
		}
		if !isRedisError(err) {
			return c.txReadFatal(err) // IO error
		}
		if moved, ask, addr := isMovedError(err); moved || ask {
			if firstRedirect == nil {
				firstRedirect = &txRedirect{moved: moved, ask: ask, addr: addr, err: err}
			}
			continue
		}
		if proto.IsTryAgainError(err) {
			if firstRedirect == nil {
				firstRedirect = &txRedirect{tryAgain: true, err: err}
			}
			continue
		}
		// Non-redirect command error (e.g. wrong arity) dirties the tx.
		cmd.SetErr(err)
		if firstFatal == nil {
			firstFatal = err
		}
	}

	// EXEC reply. ReadLine parses error lines into typed errors, so a non-nil
	// err means EXEC returned an error rather than the result array.
	c.txProcessPush(ctx, node, cn, rd)
	line, err := rd.ReadLine()
	if err != nil {
		if !isRedisError(err) {
			return c.txReadFatal(err) // IO error
		}
		return c.classifyExecError(err, firstRedirect, firstFatal)
	}

	if line[0] != proto.RespArray {
		err := fmt.Errorf("redis: unexpected EXEC reply %q", line)
		setCmdsErr(cmds, err)
		// A non-array aggregate reply may carry an unread payload.
		return &txOutcome{kind: txFatal, err: err, unreadReplies: true}
	}

	// Success: read the N command results.
	if err := node.Client.pipelineReadCmds(ctx, cn, rd, cmds); err != nil && !isRedisError(err) {
		return c.txReadFatal(err) // IO error mid-results
	}
	return &txOutcome{kind: txSuccess}
}

func (c *ClusterClient) txProcessPush(ctx context.Context, node *clusterNode, cn *pool.Conn, rd *proto.Reader) {
	if err := node.Client.processPendingPushNotificationWithReader(ctx, cn, rd); err != nil {
		internal.Logger.Printf(ctx, "push: error processing pending notifications before reading reply: %v", err)
	}
}

// txReadFatal classifies a read-phase IO error. The MULTI..EXEC batch was
// already written, so the server may have committed the transaction; retrying
// would re-execute it, double-applying non-idempotent commands (INCR/APPEND,
// which are not NoRetry). Surface the error as fatal and discard the
// connection, since replies may still be unread on the wire.
func (c *ClusterClient) txReadFatal(err error) *txOutcome {
	return &txOutcome{kind: txFatal, err: err, unreadReplies: true}
}

// txPreQueueErrorOutcome classifies a setup-phase reply error: the top-level
// ASKING reply or the MULTI reply. The transaction body never executes (EXEC
// returns -EXECABORT), so retryable errors such as -LOADING are safe to retry
// on a fresh connection. A failed setup reply still leaves the remaining
// replies on the wire -- the server replies to each following command and to
// EXEC regardless -- so the connection is always discarded.
func (c *ClusterClient) txPreQueueErrorOutcome(err error, cmds []Cmder) *txOutcome {
	if !isRedisError(err) {
		return c.txReadFatal(err)
	}
	if shouldRetry(err, true) && !cmdsContainNoRetry(cmds) {
		return &txOutcome{kind: txRetryConn, err: err, unreadReplies: true}
	}
	return &txOutcome{kind: txFatal, err: err, unreadReplies: true}
}

// classifyExecError turns an EXEC reply error into a retry/fatal outcome.
func (c *ClusterClient) classifyExecError(execErr error, firstRedirect *txRedirect, firstFatal error) *txOutcome {
	if moved, ask, addr := isMovedError(execErr); moved || ask {
		// Narrow race: the slot moved after every command was queued.
		if ask {
			return &txOutcome{kind: txRetryAsk, err: execErr, addr: addr}
		}
		return &txOutcome{kind: txRetryMoved, err: execErr, addr: addr}
	}
	if proto.IsTryAgainError(execErr) {
		return &txOutcome{kind: txRetryTryAgain, err: execErr}
	}
	if proto.IsClusterDownError(execErr) {
		// Cluster degraded: back off and retry. Replies were fully consumed.
		return &txOutcome{kind: txRetryConn, err: execErr}
	}
	if proto.IsExecAbortError(execErr) {
		if firstFatal != nil {
			return &txOutcome{kind: txFatal, err: firstFatal, execErr: execErr}
		}
		if firstRedirect != nil {
			switch {
			case firstRedirect.moved:
				return &txOutcome{kind: txRetryMoved, err: firstRedirect.err, addr: firstRedirect.addr}
			case firstRedirect.ask:
				return &txOutcome{kind: txRetryAsk, err: firstRedirect.err, addr: firstRedirect.addr}
			case firstRedirect.tryAgain:
				return &txOutcome{kind: txRetryTryAgain, err: firstRedirect.err}
			}
		}
		return &txOutcome{kind: txFatal, err: execErr, execErr: execErr}
	}
	return &txOutcome{kind: txFatal, err: execErr}
}

func (c *ClusterClient) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	if len(keys) == 0 {
		return errNoWatchKeys
	}

	slot := clusterKeySlot(keys[0])
	for _, key := range keys[1:] {
		if clusterKeySlot(key) != slot {
			return errWatchCrosslot
		}
	}

	node, err := c.slotMasterNode(ctx, slot)
	if err != nil {
		return err
	}

	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		// Track callback errors separately to avoid retrying user failures through cluster retry classification.
		var fnErr error
		err = node.Client.Watch(ctx, func(tx *Tx) error {
			fnErr = fn(tx)
			return fnErr
		}, keys...)
		if err == nil {
			break
		}
		if fnErr != nil {
			return fnErr
		}

		moved, ask, addr := isMovedError(err)
		if moved || ask {
			node, err = c.nodes.GetOrCreate(addr)
			if err != nil {
				return err
			}
			continue
		}

		if isReadOnly := isReadOnlyError(err); isReadOnly || err == pool.ErrClosed {
			if isReadOnly {
				c.state.LazyReload()
			}
			node, err = c.slotMasterNode(ctx, slot)
			if err != nil {
				return err
			}
			continue
		}

		if shouldRetry(err, true) {
			continue
		}

		return err
	}

	return err
}

// maintenance notifications won't work here for now
func (c *ClusterClient) pubSub() *PubSub {
	var node *clusterNode
	pubsub := &PubSub{
		opt: c.opt.clientOptions(),
		newConn: func(ctx context.Context, addr string, channels []string) (*pool.Conn, error) {
			if node != nil {
				panic("node != nil")
			}

			var err error

			if len(channels) > 0 {
				slot := clusterKeySlot(channels[0])

				// newConn in PubSub is only used for subscription connections, so it is safe to
				// assume that a slave node can always be used when client options specify ReadOnly.
				if c.opt.ReadOnly {
					state, err := c.state.Get(ctx)
					if err != nil {
						return nil, err
					}

					node, err = c.slotReadOnlyNode(state, slot)
					if err != nil {
						return nil, err
					}
				} else {
					node, err = c.slotMasterNode(ctx, slot)
					if err != nil {
						return nil, err
					}
				}
			} else {
				node, err = c.nodes.Random()
				if err != nil {
					return nil, err
				}
			}
			cn, err := node.Client.pubSubPool.NewConn(ctx, node.Client.opt.Network, node.Client.opt.Addr, channels)
			if err != nil {
				node = nil
				return nil, err
			}
			// will return nil if already initialized
			err = node.Client.initConn(ctx, cn)
			if err != nil {
				_ = cn.Close()
				node = nil
				return nil, err
			}
			node.Client.pubSubPool.TrackConn(cn)
			return cn, nil
		},
		closeConn: func(cn *pool.Conn) error {
			// Untrack connection from PubSubPool
			node.Client.pubSubPool.UntrackConn(cn)
			err := cn.Close()
			node = nil
			return err
		},
	}
	pubsub.init()

	return pubsub
}

// Subscribe subscribes the client to the specified channels.
// Channels can be omitted to create empty subscription.
func (c *ClusterClient) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe subscribes the client to the given patterns.
// Patterns can be omitted to create empty subscription.
func (c *ClusterClient) PSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.PSubscribe(ctx, channels...)
	}
	return pubsub
}

// SSubscribe Subscribes the client to the specified shard channels.
func (c *ClusterClient) SSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.SSubscribe(ctx, channels...)
	}
	return pubsub
}

func (c *ClusterClient) retryBackoff(attempt int) time.Duration {
	return internal.RetryBackoff(attempt, c.opt.MinRetryBackoff, c.opt.MaxRetryBackoff)
}

// metadataView returns the current immutable metadata view.
func (c *ClusterClient) metadataView() *commandMetadataView {
	if c.cmdMeta != nil {
		return c.cmdMeta.view()
	}
	return defaultCommandMetadataView
}

// fetchCommandMetadata reads HELLO and COMMAND on one connection.
// It rejects concurrent topology changes.
func (c *ClusterClient) fetchCommandMetadata(ctx context.Context) (commandMetadataFetchResult, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return commandMetadataFetchResult{}, err
	}
	generation := state.generation
	nodes := make([]*clusterNode, 0, len(state.Masters)+len(state.Slaves))
	nodes = append(nodes, state.Masters...)
	nodes = append(nodes, state.Slaves...)
	if len(nodes) == 0 {
		return commandMetadataFetchResult{}, errClusterNoNodes
	}

	expected := c.cmdMeta.serverFingerprint()
	var firstErr error
	var changedFingerprint string
	for _, idx := range rand.Perm(len(nodes)) {
		metadata, fetchErr := nodes[idx].Client.baseClient.fetchCommandMetadata(ctx)
		if !c.isTopologyGeneration(generation) {
			return commandMetadataFetchResult{}, fmt.Errorf(
				"redis: cluster topology changed during command metadata fetch",
			)
		}
		if fetchErr == nil {
			matches, identityErr := clusterMetadataFingerprintMatches(expected, metadata.serverFingerprint)
			if identityErr != nil {
				// Skip nodes whose HELLO identity cannot be verified.
				if firstErr == nil {
					firstErr = identityErr
				}
				continue
			}
			if !matches {
				// Try every node during rolling upgrades before retiring the view.
				changedFingerprint = metadata.serverFingerprint
				continue
			}
			return metadata, nil
		}
		if firstErr == nil {
			firstErr = fetchErr
		}
	}
	if changedFingerprint != "" {
		c.cmdMeta.invalidateLiveAndRequestRefresh()
		return commandMetadataFetchResult{}, fmt.Errorf(
			"redis: cluster command metadata server changed: got %q, want %q",
			changedFingerprint, expected,
		)
	}
	return commandMetadataFetchResult{}, firstErr
}

func clusterMetadataFingerprintMatches(expected, actual string) (bool, error) {
	if actual == "" {
		return false, errClusterMetadataMissingFingerprint
	}
	return expected == "" || expected == actual, nil
}

func (c *ClusterClient) isTopologyGeneration(generation uint32) bool {
	current := c.state.state.Load()
	return current != nil && current.(*clusterState).generation == generation
}

// clusterRoutingDecision is one invocation's immutable routing result.
type clusterRoutingDecision struct {
	view          *commandMetadataView
	name          string
	meta          routingCommandMeta
	metadataState routingMetadataState
	metaOK        bool
	plan          routingKeyPlan
	planOK        bool
	firstKey      int
	naturalSlot   int
	keyless       bool
	readOnly      bool
	policy        *routing.CommandPolicy
	policyErr     error
}

func (c *ClusterClient) routingDecisionInView(
	ctx context.Context,
	cmd Cmder,
	view *commandMetadataView,
	policy *routing.CommandPolicy,
) clusterRoutingDecision {
	meta, metaOK := routingLookupMeta(view, cmd)
	return c.routingDecisionWithMeta(cmd, view, policy, policy != nil, meta, metaOK)
}

func (c *ClusterClient) routingDecisionWithMeta(
	cmd Cmder,
	view *commandMetadataView,
	policy *routing.CommandPolicy,
	policyFromMetadata bool,
	meta routingCommandMeta,
	metaOK bool,
) clusterRoutingDecision {
	metadataState := routingMetadataUsable
	if !metaOK {
		_, metadataState = routingLookupMetaState(view, cmd)
	}
	d := clusterRoutingDecision{
		view: view, name: cmd.Name(), meta: meta, metaOK: metaOK,
		metadataState: metadataState,
		firstKey:      -1, policy: policy,
	}
	if policy != nil && !policyFromMetadata {
		d.readOnly = policy.IsReadOnly()
	}
	if d.metaOK {
		if (policy == nil || policyFromMetadata) && d.meta.policy != nil {
			d.readOnly = d.meta.readOnly
		}
		if pos, ok := routingFirstKeyPos(d.meta, cmd); ok {
			d.firstKey = pos
			d.keyless = pos == 0
		}
		needsFullPlan := policy != nil && policy.Request == routing.ReqMultiShard
		needsFullPlan = needsFullPlan || d.meta.policy != nil && d.meta.policy.Request == routing.ReqMultiShard
		if needsFullPlan {
			d.plan, d.planOK = routingResolveKeyPlan(d.meta, cmd)
		}
	} else if policy != nil && policy.Request == routing.ReqDefault &&
		policy.Response == routing.RespDefaultKeyless {
		// A custom policy may explicitly mark an unknown command keyless.
		d.firstKey = 0
		d.keyless = true
	} else if d.metadataState == routingMetadataMissing && policy == nil {
		// Preserve raw module/proxy commands and explicit SetFirstKeyPos hints.
		// A malformed known record remains unusable and never takes this path.
		d.firstKey = cmdFirstKeyPosWithInfo(cmd, nil)
		d.keyless = d.firstKey == 0
	} else if d.metadataState == routingMetadataMissing && policy.Request == routing.ReqDefault &&
		cmd.firstKeyPos() != 0 {
		// A custom keyed policy still needs an explicit key-position hint.
		d.firstKey = int(cmd.firstKeyPos())
	}

	// Metadata routing never falls back to constructor key hints.
	d.naturalSlot = c.cmdSlotWithPos(cmd, d.firstKey, -1)

	d.policyErr = clusterRoutingPolicyError(d)
	if d.policyErr == nil && clusterPolicyFansOut(d.policy) && unsafeClusterFanoutCommand(cmd, d) {
		d.policyErr = fmt.Errorf(
			"redis: cannot fan out streaming, raw, or non-repeatable command %q safely",
			cmd.Name(),
		)
	}
	return d
}

func clusterPolicyFansOut(policy *routing.CommandPolicy) bool {
	if policy == nil {
		return false
	}
	switch policy.Request {
	case routing.ReqAllNodes, routing.ReqAllShards, routing.ReqMultiShard:
		return true
	default:
		return false
	}
}

func unsafeClusterFanoutCommand(cmd Cmder, d clusterRoutingDecision) bool {
	if cmd.NoRetry() {
		return true
	}
	switch cmd.(type) {
	case *RawCmd, *RawWriteToCmd:
		return true
	}
	// Multi-shard commands reproduce only their proven key groups. Key
	// positions were already checked by routingResolveKeyPlan; values are sent
	// to exactly one shard and have the same retry semantics as ordinary writes.
	if d.policy != nil && d.policy.Request == routing.ReqMultiShard {
		return false
	}
	return !commandArgsRepeatable(cmd)
}

func clusterRoutingPolicyError(d clusterRoutingDecision) error {
	// Custom policies may route without metadata; default metadata routing may not.
	if !d.metaOK && d.policy == nil {
		switch d.metadataState {
		case routingMetadataUnusable:
			return fmt.Errorf("%w for command %s", errClusterCommandMetadataUnusable, d.name)
		default:
			return nil
		}
	}
	effectivePolicy := d.policy
	if effectivePolicy == nil && d.metaOK {
		effectivePolicy = d.meta.policy
	}
	if effectivePolicy != nil && effectivePolicy.Request == routing.ReqDefault && d.firstKey < 0 {
		return fmt.Errorf("redis: cannot determine the routing key for command %s", d.name)
	}
	if effectivePolicy != nil && effectivePolicy.Request == routing.ReqDefault &&
		d.firstKey > 0 && d.naturalSlot < 0 {
		return fmt.Errorf("redis: cannot reproduce the routing key for command %s", d.name)
	}
	if d.policy != nil && d.policy.Request == routing.ReqMultiShard &&
		(!d.planOK || !d.plan.splittable || len(d.plan.positions) == 0) {
		return fmt.Errorf("redis: cannot determine all key arguments for multi-shard command %s", d.name)
	}
	if d.policy == nil {
		if d.metaOK {
			if d.meta.policy != nil && d.meta.policy.Request == routing.ReqMultiShard &&
				(!d.planOK || !d.plan.splittable || len(d.plan.positions) == 0) {
				return fmt.Errorf("redis: cannot determine all key arguments for multi-shard command %s", d.name)
			}
			return routingSpecialPolicyError(d.meta)
		}
		return nil
	}
	if d.policy.Request == routing.ReqSpecial &&
		(!d.metaOK || d.meta.special&(routingSpecialRequestDeclared|routingSpecialRequestSupported) !=
			routingSpecialRequestDeclared|routingSpecialRequestSupported) {
		return errUnsupportedRoutingPolicy
	}
	if d.policy.Response == routing.RespSpecial &&
		(!d.metaOK || d.meta.special&(routingSpecialResponseDeclared|routingSpecialResponseSupported) !=
			routingSpecialResponseDeclared|routingSpecialResponseSupported) {
		return errUnsupportedRoutingPolicy
	}
	return nil
}

func (c *ClusterClient) commandRoutingDecision(ctx context.Context, cmd Cmder) clusterRoutingDecision {
	if c.opt.DisableRoutingPolicies {
		c.autoPipelineRouting.Delete(cmd)
		return c.legacyRoutingDecision(cmd)
	}
	if decision, ok := c.takeAutoPipelineRoutingDecision(cmd); ok {
		return decision
	}
	return c.resolveCommandRoutingDecision(ctx, cmd)
}

func (c *ClusterClient) legacyRoutingDecision(cmd Cmder) clusterRoutingDecision {
	firstKey := cmdFirstKeyPosWithInfo(cmd, nil)
	d := clusterRoutingDecision{
		name:     cmd.Name(),
		firstKey: firstKey,
		keyless:  firstKey == 0,
	}
	// Disabled policy routing uses only the static snapshot for replica selection.
	if meta, ok := routingLookupMeta(defaultCommandMetadataView, cmd); ok && meta.policy != nil {
		d.readOnly = meta.readOnly
	}
	d.naturalSlot = c.legacyCmdSlotWithPos(cmd, firstKey, -1)
	return d
}

func (c *ClusterClient) resolveCommandRoutingDecision(ctx context.Context, cmd Cmder) clusterRoutingDecision {
	view := c.metadataView()
	var resolution commandRoutingResolution
	if c.cmdInfoResolver != nil {
		var err error
		resolution, view, err = c.cmdInfoResolver.resolveCommandRoutingWithView(ctx, cmd, c.metadataView)
		if err != nil {
			// Keep the current view and retry the optional live upgrade later.
			internal.Logger.Printf(ctx, "getting live command metadata: %s", err)
			if view == nil {
				view = c.metadataView()
			}
		}
		return c.routingDecisionWithMeta(
			cmd, view, resolution.policy, resolution.policyFromMetadata,
			resolution.meta, resolution.metaOK,
		)
	}
	return c.routingDecisionInView(ctx, cmd, view, nil)
}

func (c *ClusterClient) cmdSlot(cmd Cmder, prefferedSlot int) int {
	if c.opt != nil && c.opt.DisableRoutingPolicies {
		return c.legacyCmdSlotWithPos(cmd, cmdFirstKeyPosWithInfo(cmd, nil), prefferedSlot)
	}
	view := c.metadataView()
	d := c.routingDecisionInView(context.Background(), cmd, view, nil)
	return c.cmdSlotWithDecision(cmd, d, prefferedSlot)
}

func (c *ClusterClient) cmdSlotWithDecision(cmd Cmder, d clusterRoutingDecision, prefferedSlot int) int {
	if prefferedSlot == -1 {
		return d.naturalSlot
	}
	if c.opt != nil && c.opt.DisableRoutingPolicies {
		return c.legacyCmdSlotWithPos(cmd, d.firstKey, prefferedSlot)
	}
	return c.cmdSlotWithPos(cmd, d.firstKey, prefferedSlot)
}

func (c *ClusterClient) legacyCmdSlotWithPos(cmd Cmder, pos int, prefferedSlot int) int {
	args := cmd.Args()
	if len(args) > 2 && cmd.Name() == "cluster" &&
		(strings.EqualFold(cmd.stringArg(1), "getkeysinslot") || strings.EqualFold(cmd.stringArg(1), "countkeysinslot")) {
		if slot, ok := args[2].(int); ok {
			return slot
		}
	}
	if pos == 0 {
		if prefferedSlot != -1 {
			return prefferedSlot
		}
		return -1
	}
	// Preserve legacy behavior: an unknown key selects a random slot.
	return hashtag.Slot(cmd.stringArg(pos))
}

// cmdSlotWithPos computes the cluster slot for cmd given a pre-resolved first key
// position. Separating pos resolution from slot computation lets callers that
// already know pos avoid a redundant Peek() call.
func (c *ClusterClient) cmdSlotWithPos(cmd Cmder, pos int, prefferedSlot int) int {
	args := cmd.Args()
	if len(args) > 2 && cmd.Name() == "cluster" &&
		(strings.EqualFold(cmd.stringArg(1), "getkeysinslot") || strings.EqualFold(cmd.stringArg(1), "countkeysinslot")) {
		if slotText, ok := routingArgText(cmd, 2); ok {
			if slot, err := strconv.Atoi(slotText); err == nil && slot >= 0 && slot < 16384 {
				return slot
			}
		}
	}
	return cmdSlot(cmd, pos, prefferedSlot)
}

func cmdSlot(cmd Cmder, pos int, prefferedRandomSlot int) int {
	if pos == 0 {
		if prefferedRandomSlot != -1 {
			return prefferedRandomSlot
		}
		// Return -1 for keyless commands to signal that ShardPicker should be used
		return -1
	}
	firstKey, ok := routingArgText(cmd, pos)
	if !ok {
		// Route unknown wire encodings conservatively; Redis reports argument errors.
		return prefferedRandomSlot
	}
	return clusterKeySlot(firstKey)
}

func clusterKeySlot(key string) int {
	// Empty keys hash to slot 0; hashtag.Slot treats empty input as keyless.
	if key == "" {
		return 0
	}
	return hashtag.Slot(key)
}

func (c *ClusterClient) cmdNodeWithDecision(
	ctx context.Context,
	slot int,
	d clusterRoutingDecision,
) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}

	if c.opt.ReadOnly && d.readOnly {
		node, err := c.slotReadOnlyNode(state, slot)
		return node, c.noteTopologySelectionError(err)
	}
	node, err := state.slotMasterNode(slot)
	return node, c.noteTopologySelectionError(err)
}

func (c *ClusterClient) cmdNodeWithShardPickerAndDecision(
	ctx context.Context,
	slot int,
	shardPicker routing.ShardPicker,
	d clusterRoutingDecision,
) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}

	// For keyless commands (slot == -1), use ShardPicker to select a shard
	// This respects the user's configured ShardPicker policy
	if slot == -1 {
		total := len(state.Masters)
		includeReplicas := c.opt.ReadOnly && d.readOnly
		if includeReplicas {
			total += len(state.Slaves)
		}
		if total == 0 {
			return nil, errClusterNoNodes
		}
		idx := shardPicker.Next(total)
		if idx < len(state.Masters) {
			return state.Masters[idx], nil
		}
		return state.Slaves[idx-len(state.Masters)], nil
	}

	if c.opt.ReadOnly && d.readOnly {
		node, err := c.slotReadOnlyNode(state, slot)
		return node, c.noteTopologySelectionError(err)
	}
	node, err := state.slotMasterNode(slot)
	return node, c.noteTopologySelectionError(err)
}

func (c *ClusterClient) noteTopologySelectionError(err error) error {
	if errors.Is(err, errClusterTopologyUnhealthy) {
		c.state.LazyReload()
	}
	return err
}

func (c *ClusterClient) reloadTopologyForRetry(ctx context.Context, err error, attempt int) bool {
	if !errors.Is(err, errClusterTopologyUnhealthy) || attempt >= c.opt.MaxRedirects {
		return false
	}
	// The selection path already schedules a coalesced background reload.
	// Reload synchronously too so this invocation can use its normal retry
	// budget instead of requiring the caller to retry.
	_, _ = c.state.Reload(ctx)
	return true
}

func (c *ClusterClient) slotReadOnlyNode(state *clusterState, slot int) (*clusterNode, error) {
	var node *clusterNode
	var err error
	if c.opt.RouteByLatency {
		node, err = state.slotClosestNode(slot)
	} else if c.opt.RouteRandomly {
		node, err = state.slotRandomNode(slot)
	} else if c.opt.ShardPicker != nil {
		node, err = state.slotShardPickerSlaveNode(slot, c.opt.ShardPicker)
	} else {
		node, err = state.slotSlaveNode(slot)
	}
	return node, c.noteTopologySelectionError(err)
}

func (c *ClusterClient) slotMasterNode(ctx context.Context, slot int) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}
	node, err := state.slotMasterNode(slot)
	return node, c.noteTopologySelectionError(err)
}

// SlaveForKey gets a client for a replica node to run any command on it.
// This is especially useful if we want to run a particular lua script which has
// only read only commands on the replica.
// This is because other redis commands generally have a flag that points that
// they are read only and automatically run on the replica nodes
// if ClusterOptions.ReadOnly flag is set to true.
func (c *ClusterClient) SlaveForKey(ctx context.Context, key string) (*Client, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}
	slot := clusterKeySlot(key)
	node, err := c.slotReadOnlyNode(state, slot)
	if err != nil {
		return nil, err
	}
	return node.Client, err
}

// MasterForKey return a client to the master node for a particular key.
func (c *ClusterClient) MasterForKey(ctx context.Context, key string) (*Client, error) {
	slot := clusterKeySlot(key)
	node, err := c.slotMasterNode(ctx, slot)
	if err != nil {
		return nil, err
	}
	return node.Client, nil
}

func (c *ClusterClient) context(ctx context.Context) context.Context {
	if c.opt.ContextTimeoutEnabled {
		return ctx
	}
	return context.Background()
}

func (c *ClusterClient) GetResolver() *commandInfoResolver {
	return c.cmdInfoResolver
}

func (c *ClusterClient) SetCommandInfoResolver(cmdInfoResolver *commandInfoResolver) {
	c.cmdInfoResolver = cmdInfoResolver
}

// NewDynamicResolver tries a live metadata refresh before using the current view.
func (c *ClusterClient) NewDynamicResolver() *commandInfoResolver {
	return newCommandMetadataPolicyResolverWithEnsure(c.metadataView, c.cmdMeta.ensureLive)
}

func appendIfNotExist[T comparable](vals []T, newVal T) []T {
	if slices.Contains(vals, newVal) {
		return vals
	}
	return append(vals, newVal)
}

//------------------------------------------------------------------------------

type cmdsMap struct {
	mu sync.Mutex
	m  map[*clusterNode][]Cmder
}

func newCmdsMap() *cmdsMap {
	return &cmdsMap{
		m: make(map[*clusterNode][]Cmder),
	}
}

func (m *cmdsMap) Add(node *clusterNode, cmds ...Cmder) {
	m.mu.Lock()
	m.m[node] = append(m.m[node], cmds...)
	m.mu.Unlock()
}
