package redis_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/hashtag"
)

// reDatabaseEndpoint mirrors a raw_endpoints entry from the env0 endpoints config.
type reDatabaseEndpoint struct {
	DNSName string `json:"dns_name"`
	Port    int    `json:"port"`
}

// reDatabaseConfig is a single database entry in the env0 endpoints config file
// referenced by REDIS_ENDPOINTS_CONFIG_PATH.
type reDatabaseConfig struct {
	Username     string               `json:"username"`
	Password     string               `json:"password"`
	TLS          bool                 `json:"tls"`
	RawEndpoints []reDatabaseEndpoint `json:"raw_endpoints"`
	Endpoints    []string             `json:"endpoints"`
}

// reConnection holds the resolved connection parameters for the Redis Enterprise
// database the suite runs against.
type reConnection struct {
	Addr     string
	Username string
	Password string
	TLS      bool
}

// reConn is populated in BeforeSuite when running against a Redis Enterprise
// cluster provisioned outside the test process (RE_CLUSTER=true and
// REDIS_ENDPOINTS_CONFIG_PATH pointing at the env0 endpoints file).
var reConn *reConnection

// loadREConnection reads REDIS_ENDPOINTS_CONFIG_PATH and resolves the database to
// run the Redis Enterprise suite against. RE_DB_NAME selects a named database;
// when empty the first database in the config is used.
func loadREConnection() (*reConnection, error) {
	path := os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH")
	if path == "" {
		return nil, fmt.Errorf("REDIS_ENDPOINTS_CONFIG_PATH is not set")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}

	var databases map[string]reDatabaseConfig
	if err := json.Unmarshal(content, &databases); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	if len(databases) == 0 {
		return nil, fmt.Errorf("no databases found in %s", path)
	}

	name := os.Getenv("RE_DB_NAME")
	var db reDatabaseConfig
	if name != "" {
		found, ok := databases[name]
		if !ok {
			return nil, fmt.Errorf("database %q not found in %s", name, path)
		}
		db = found
	} else {
		// Map iteration order is undefined, so pick the first database by sorted
		// name to make the default selection deterministic across runs.
		names := make([]string, 0, len(databases))
		for n := range databases {
			names = append(names, n)
		}
		sort.Strings(names)
		db = databases[names[0]]
	}

	host, port, tlsEnabled, err := resolveREEndpoint(db)
	if err != nil {
		return nil, err
	}

	return &reConnection{
		// JoinHostPort brackets IPv6 literals ("[::1]:6379"); Sprintf would
		// produce an undialable "::1:6379".
		Addr:     net.JoinHostPort(host, strconv.Itoa(port)),
		Username: db.Username,
		Password: db.Password,
		TLS:      tlsEnabled,
	}, nil
}

// resolveREEndpoint extracts host, port and TLS from a database config, preferring
// raw_endpoints (which carry the resolvable DNS name and port) over the endpoint URLs.
func resolveREEndpoint(db reDatabaseConfig) (string, int, bool, error) {
	tlsEnabled := db.TLS
	if len(db.RawEndpoints) > 0 {
		// Fall through to the endpoint URLs when the raw endpoint carries no
		// DNS name, so a sparse config fails loudly instead of dialing ":port".
		if ep := db.RawEndpoints[0]; ep.DNSName != "" {
			return ep.DNSName, ep.Port, tlsEnabled, nil
		}
	}
	if len(db.Endpoints) > 0 {
		u, err := url.Parse(db.Endpoints[0])
		if err != nil {
			return "", 0, false, fmt.Errorf("failed to parse endpoint %q: %w", db.Endpoints[0], err)
		}
		if u.Scheme == "rediss" {
			tlsEnabled = true
		}
		// The env0 endpoints format may omit the port; fall back to the scheme
		// default (6379 for redis, 6380 for rediss), matching maintnotifications/e2e.
		port := 6379
		if u.Scheme == "rediss" {
			port = 6380
		}
		if p := u.Port(); p != "" {
			parsed, err := strconv.Atoi(p)
			if err != nil {
				return "", 0, false, fmt.Errorf("invalid port in endpoint %q: %w", db.Endpoints[0], err)
			}
			port = parsed
		}
		return u.Hostname(), port, tlsEnabled, nil
	}
	return "", 0, false, fmt.Errorf("no endpoints found in database configuration")
}

// TestLoadREConnectionIPv6 pins the bracketed address form: url.Hostname()
// strips the brackets from an IPv6 literal, and joining with a plain "%s:%d"
// produced an undialable "::1:12000".
func TestLoadREConnectionIPv6(t *testing.T) {
	path := filepath.Join(t.TempDir(), "endpoints.json")
	cfg := `{"db": {"tls": true, "endpoints": ["rediss://[::1]:12000"]}}`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", path)
	t.Setenv("RE_DB_NAME", "db")

	conn, err := loadREConnection()
	if err != nil {
		t.Fatal(err)
	}
	if want := "[::1]:12000"; conn.Addr != want {
		t.Fatalf("Addr = %q, want %q", conn.Addr, want)
	}
	if !conn.TLS {
		t.Fatal("rediss endpoint must enable TLS")
	}
}

// A remote Redis Enterprise endpoint has higher latency than a local server, so
// the overlays below use the same relaxed timeouts as redisOptions() instead of
// the ~3s defaults; otherwise module/search/timeseries helpers can flake on
// slower commands.
const (
	reDialTimeout  = 10 * time.Second
	reReadTimeout  = 30 * time.Second
	reWriteTimeout = 30 * time.Second
)

// applyREConnection overlays the resolved Redis Enterprise credentials and TLS
// settings onto opt. It is a no-op when reConn has not been populated.
func applyREConnection(opt *redis.Options) {
	if reConn == nil {
		return
	}
	opt.Addr = reConn.Addr
	opt.Username = reConn.Username
	opt.Password = reConn.Password
	if reConn.TLS {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	opt.DialTimeout = reDialTimeout
	opt.ReadTimeout = reReadTimeout
	opt.WriteTimeout = reWriteTimeout
	opt.ContextTimeoutEnabled = true
}

// applyREConnectionUniversal overlays the resolved Redis Enterprise endpoint,
// credentials and TLS settings onto a UniversalOptions. It is a no-op when
// reConn has not been populated.
func applyREConnectionUniversal(opt *redis.UniversalOptions) {
	if reConn == nil {
		return
	}
	opt.Addrs = []string{reConn.Addr}
	opt.Username = reConn.Username
	opt.Password = reConn.Password
	if reConn.TLS {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	opt.DialTimeout = reDialTimeout
	opt.ReadTimeout = reReadTimeout
	opt.WriteTimeout = reWriteTimeout
	opt.ContextTimeoutEnabled = true
}

// reNewClient builds a *redis.Client for the configured endpoint, applying the
// Redis Enterprise connection details (host, credentials, TLS) when running
// against an external cluster via REDIS_ENDPOINTS_CONFIG_PATH.
func reNewClient(protocol int, unstableResp3 bool) *redis.Client {
	opt := &redis.Options{
		Addr:          redisAddr,
		Protocol:      protocol,
		UnstableResp3: unstableResp3,
	}
	applyREConnection(opt)
	return redis.NewClient(opt)
}

// TestEnterpriseClusterRouting exercises the supplied Enterprise endpoints,
// including their proxy topology. It owns only its uniquely prefixed test keys.
func TestEnterpriseClusterRouting(t *testing.T) {
	if os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH") == "" {
		t.Skip("requires a dedicated Redis Enterprise database")
	}
	conn, err := loadREConnection()
	if err != nil {
		t.Fatal(err)
	}
	for _, protocol := range []int{2, 3} {
		for _, mode := range []string{"default", "metadata", "dynamic-static", "prefer-live", "disabled"} {
			t.Run(fmt.Sprintf("RESP%d/%s", protocol, mode), func(t *testing.T) {
				opt := &redis.ClusterOptions{
					Addrs: []string{conn.Addr}, Username: conn.Username, Password: conn.Password,
					Protocol: protocol, DialTimeout: reDialTimeout, ReadTimeout: reReadTimeout,
					WriteTimeout: reWriteTimeout, ContextTimeoutEnabled: true,
					DisableRoutingPolicies: mode == "disabled",
				}
				if conn.TLS {
					opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
				}
				if mode == "prefer-live" {
					opt.CommandMetadata = &redis.CommandMetadataConfig{Mode: redis.CommandMetadataPreferLive}
				}
				client := redis.NewClusterClient(opt)
				t.Cleanup(func() { _ = client.Close() })
				switch mode {
				case "metadata", "prefer-live":
					client.SetCommandInfoResolver(redis.NewCommandMetadataResolver())
				case "dynamic-static":
					client.SetCommandInfoResolver(client.NewDynamicResolver())
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := client.Ping(ctx).Err(); err != nil {
					t.Fatal(err)
				}
				shards, err := client.ClusterShards(ctx).Result()
				if err != nil || len(shards) < 3 {
					t.Fatalf("need three Enterprise shards: count=%d error=%v", len(shards), err)
				}
				prefix := fmt.Sprintf("go-redis:pr4093:%d:", time.Now().UnixNano())
				keys := []string{prefix + "{a}", prefix + "{b}", prefix + "{c}"}
				owners := make(map[int]bool)
				for _, key := range keys {
					slot := int64(hashtag.Slot(key))
					for i, shard := range shards {
						for _, slots := range shard.Slots {
							if slots.Start <= slot && slot <= slots.End {
								owners[i] = true
							}
						}
					}
				}
				if len(owners) != len(keys) {
					t.Fatalf("test keys must cover three shard owners, got %d", len(owners))
				}
				t.Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cleanupCancel()
					for _, key := range keys {
						if err := client.Del(cleanupCtx, key).Err(); err != nil {
							t.Errorf("clean test key: %v", err)
						}
					}
				})
				_, err = client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
					for i, key := range keys {
						pipe.Set(ctx, key, i, 2*time.Minute)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				values, err := client.MGet(ctx, keys...).Result()
				if mode == "default" || mode == "disabled" {
					if !redis.HasErrorPrefix(err, "CROSSSLOT") {
						t.Fatalf("routing without opt-in must preserve CROSSSLOT: %v", err)
					}
				} else if err != nil || !reflect.DeepEqual(values, []interface{}{"0", "1", "2"}) {
					t.Fatalf("cross-shard values=%v error=%v", values, err)
				}
				var read *redis.StringCmd
				_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Incr(ctx, keys[0])
					read = pipe.Get(ctx, keys[0])
					pipe.Ping(ctx)
					return nil
				})
				if err != nil || read.Val() != "1" {
					t.Fatalf("transaction value=%q error=%v", read.Val(), err)
				}
				if mode != "metadata" {
					return
				}
				for _, fullDuplex := range []bool{false, true} {
					for _, async := range []bool{false, true} {
						t.Run(fmt.Sprintf("AutoPipeline/fullDuplex=%v/async=%v", fullDuplex, async), func(t *testing.T) {
							apOpt := *opt
							apClient := redis.NewClusterClient(&apOpt)
							defer apClient.Close()
							apClient.SetCommandInfoResolver(redis.NewCommandMetadataResolver())
							cfg := &redis.AutoPipelineOptions{FullDuplex: fullDuplex, MaxBatchSize: 32}
							var ap *redis.AutoPipeliner
							var err error
							if async {
								ap, err = apClient.AsyncAutoPipelineWithOptions(cfg)
							} else {
								ap, err = apClient.AutoPipelineWithOptions(cfg)
							}
							if err != nil {
								t.Fatal(err)
							}
							defer ap.Close()
							commands := make([]*redis.StringCmd, 0, 12)
							for range 4 {
								for _, key := range keys {
									commands = append(commands, ap.Get(ctx, key))
								}
							}
							for i, cmd := range commands {
								want := []string{"1", "1", "2"}[i%len(keys)]
								if value, err := cmd.Result(); err != nil || value != want {
									t.Fatalf("command %d value=%q error=%v", i, value, err)
								}
							}
						})
					}
				}
			})
		}
	}
}

func TestEnterpriseClusterCursor(t *testing.T) {
	if os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH") == "" {
		t.Skip("requires a dedicated Redis Enterprise database with Search")
	}
	conn, err := loadREConnection()
	if err != nil {
		t.Fatal(err)
	}
	newClient := func(protocol int) *redis.ClusterClient {
		opt := &redis.ClusterOptions{
			Addrs: []string{conn.Addr}, Username: conn.Username, Password: conn.Password,
			Protocol: protocol, UnstableResp3: true, ContextTimeoutEnabled: true,
			DialTimeout: reDialTimeout, ReadTimeout: reReadTimeout, WriteTimeout: reWriteTimeout,
		}
		if conn.TLS {
			opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}
		return redis.NewClusterClient(opt)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin := newClient(3)
	defer admin.Close()
	prefix := fmt.Sprintf("go-redis:cursor:%d:", time.Now().UnixNano())
	index := prefix + "index"
	keys := []string{prefix + "{a}", prefix + "{b}", prefix + "{c}"}
	if err := admin.FTCreate(ctx, index, &redis.FTCreateOptions{OnHash: true, Prefix: []interface{}{prefix}},
		&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric}).Err(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := admin.FTDropIndex(cleanupCtx, index).Err(); err != nil {
			t.Errorf("drop test index: %v", err)
		}
		for _, key := range keys {
			if err := admin.Del(cleanupCtx, key).Err(); err != nil {
				t.Errorf("delete test document: %v", err)
			}
		}
	}()
	for i, key := range keys {
		if err := admin.HSet(ctx, key, "n", i).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(15 * time.Second); ; {
		result, err := admin.FTSearch(ctx, index, "*").Result()
		if err == nil && result.Total == len(keys) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("index did not become ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, protocol := range []int{2, 3} {
		for _, mode := range []string{"direct", "pipeline", "transaction", "blocking", "async", "blocking-fd", "async-fd"} {
			t.Run(fmt.Sprintf("RESP%d/%s", protocol, mode), func(t *testing.T) {
				client := newClient(protocol)
				defer client.Close()
				var face redis.Cmdable = client
				if mode == "blocking" || mode == "async" || strings.HasSuffix(mode, "-fd") {
					cfg := &redis.AutoPipelineOptions{FullDuplex: strings.HasSuffix(mode, "-fd")}
					var ap *redis.AutoPipeliner
					var err error
					if strings.HasPrefix(mode, "async") {
						ap, err = client.AsyncAutoPipelineWithOptions(cfg)
					} else {
						ap, err = client.AutoPipelineWithOptions(cfg)
					}
					if err != nil {
						t.Fatal(err)
					}
					defer ap.Close()
					face = ap
				}
				transactionReadDenied := false
				execute := func(build func(redis.Cmdable)) error {
					var err error
					switch mode {
					case "pipeline":
						_, err = client.Pipelined(ctx, func(pipe redis.Pipeliner) error { build(pipe); return nil })
					case "transaction":
						if transactionReadDenied {
							build(client)
						} else {
							_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error { build(pipe); return nil })
						}
					default:
						build(face)
					}
					return err
				}
				var aggregate *redis.AggregateCmd
				create := func(face redis.Cmdable) {
					aggregate = face.FTAggregateWithArgs(ctx, index, "*", &redis.FTAggregateOptions{
						LoadAll: true, WithCursor: true, WithCursorOptions: &redis.FTAggregateWithCursor{Count: 1, MaxIdle: 60000},
					})
				}
				if mode == "transaction" {
					// Enterprise does not allow the blocking aggregate coordinator in
					// MULTI. Test continuation of an already-created cursor instead.
					create(client)
				} else {
					if err := execute(create); err != nil {
						t.Fatal(err)
					}
				}
				result, err := aggregate.Result()
				if err != nil || result == nil || result.CursorID == 0 {
					t.Fatalf("cursor creation result=%v error=%v", result, err)
				}
				token := result.CursorID
				defer func() {
					if token != 0 {
						cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
						defer cleanupCancel()
						_ = client.FTCursorDel(cleanupCtx, index, int(token)).Err()
					}
				}()
				seen := make(map[string]bool)
				add := func(value interface{}) {
					key := fmt.Sprint(value)
					if seen[key] {
						t.Fatalf("cursor repeated document %s", key)
					}
					seen[key] = true
				}
				for _, row := range result.Rows {
					add(row.Fields["n"])
				}
				var collect func(interface{})
				collect = func(value interface{}) {
					switch value := value.(type) {
					case []interface{}:
						for i := 0; i < len(value); i++ {
							if name, ok := value[i].(string); ok && name == "n" && i+1 < len(value) {
								add(value[i+1])
								i++
							} else {
								collect(value[i])
							}
						}
					case map[interface{}]interface{}:
						for key, item := range value {
							if key == "n" {
								add(item)
							} else {
								collect(item)
							}
						}
					}
				}
				for page := 0; token != 0 && page < 10; page++ {
					// Advance the keyless picker; continuation must retain its owner.
					if err := client.Ping(ctx).Err(); err != nil {
						t.Fatal(err)
					}
					var read *redis.MapStringInterfaceCmd
					err := execute(func(face redis.Cmdable) { read = face.FTCursorRead(ctx, index, int(token), 1) })
					if mode == "transaction" && redis.HasErrorPrefix(err, "Cannot perform `FT.CURSOR`: Cannot block") {
						// Enterprise Search 8.8 disallows cursor reads in MULTI. Check
						// transparent error propagation and prove it consumed no rows.
						t.Log("server rejects FT.CURSOR in MULTI; verifying ordinary continuation without lost rows")
						transactionReadDenied = true
						read = client.FTCursorRead(ctx, index, int(token), 1)
					} else if err != nil {
						t.Fatal(err)
					}
					value, err := read.Result()
					if err != nil {
						t.Fatal(err)
					}
					collect(value["results"])
					token, err = strconv.ParseUint(fmt.Sprint(value["cursor"]), 10, 64)
					if err != nil {
						t.Fatalf("cursor reply=%v: %v", value, err)
					}
				}
				if token != 0 || !reflect.DeepEqual(seen, map[string]bool{"0": true, "1": true, "2": true}) {
					t.Fatalf("cursor=%d documents=%v", token, seen)
				}
			})
		}
	}
}
