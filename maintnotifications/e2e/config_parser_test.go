package e2e

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// DatabaseEndpoint represents a single database endpoint configuration
type DatabaseEndpoint struct {
	Addr                               []string `json:"addr"`
	AddrType                           string   `json:"addr_type"`
	DNSName                            string   `json:"dns_name"`
	OSSClusterAPIPreferredEndpointType string   `json:"oss_cluster_api_preferred_endpoint_type"`
	OSSClusterAPIPreferredIPType       string   `json:"oss_cluster_api_preferred_ip_type"`
	Port                               int      `json:"port"`
	ProxyPolicy                        string   `json:"proxy_policy"`
	UID                                string   `json:"uid"`
}

// DatabaseConfig represents the configuration for a single database
type DatabaseConfig struct {
	BdbID                int                `json:"bdb_id,omitempty"`
	Username             string             `json:"username,omitempty"`
	Password             string             `json:"password,omitempty"`
	TLS                  bool               `json:"tls"`
	CertificatesLocation string             `json:"certificatesLocation,omitempty"`
	RawEndpoints         []DatabaseEndpoint `json:"raw_endpoints,omitempty"`
	Endpoints            []string           `json:"endpoints"`
}

// DatabasesConfig represents the complete configuration file structure
type DatabasesConfig map[string]DatabaseConfig

// EnvConfig represents environment configuration for test scenarios
type EnvConfig struct {
	RedisEndpointsConfigPath string
	FaultInjectorURL         string
}

// RedisConnectionConfig represents Redis connection parameters
type RedisConnectionConfig struct {
	Host                 string
	Port                 int
	Username             string
	Password             string
	TLS                  bool
	BdbID                int
	CertificatesLocation string
	Endpoints            []string
}

// GetEnvConfig reads environment variables required for the test scenario
func GetEnvConfig() (*EnvConfig, error) {
	redisConfigPath := os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH")
	if redisConfigPath == "" {
		return nil, fmt.Errorf("REDIS_ENDPOINTS_CONFIG_PATH environment variable must be set")
	}

	faultInjectorURL := os.Getenv("FAULT_INJECTION_API_URL")
	if faultInjectorURL == "" {
		// Default to localhost if not set
		faultInjectorURL = "http://localhost:8080"
	}

	return &EnvConfig{
		RedisEndpointsConfigPath: redisConfigPath,
		FaultInjectorURL:         faultInjectorURL,
	}, nil
}

// GetDatabaseConfigFromEnv reads database configuration from a file
func GetDatabaseConfigFromEnv(filePath string) (DatabasesConfig, error) {
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read database config from %s: %w", filePath, err)
	}

	var config DatabasesConfig
	if err := json.Unmarshal(fileContent, &config); err != nil {
		return nil, fmt.Errorf("failed to parse database config from %s: %w", filePath, err)
	}

	return config, nil
}

// GetDatabaseConfig gets Redis connection parameters for a specific database
func GetDatabaseConfig(databasesConfig DatabasesConfig, databaseName string) (*RedisConnectionConfig, error) {
	var dbConfig DatabaseConfig
	var exists bool

	if databaseName == "" {
		// Get the first database if no name is provided
		for _, config := range databasesConfig {
			dbConfig = config
			exists = true
			break
		}
	} else {
		dbConfig, exists = databasesConfig[databaseName]
	}

	if !exists {
		return nil, fmt.Errorf("database %s not found in configuration", databaseName)
	}

	// Parse connection details from endpoints or raw_endpoints
	var host string
	var port int

	if len(dbConfig.RawEndpoints) > 0 {
		// Use raw_endpoints if available (for more complex configurations)
		endpoint := dbConfig.RawEndpoints[0] // Use the first endpoint
		host = endpoint.DNSName
		port = endpoint.Port
	} else if len(dbConfig.Endpoints) > 0 {
		// Parse from endpoints URLs
		endpointURL, err := url.Parse(dbConfig.Endpoints[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse endpoint URL %s: %w", dbConfig.Endpoints[0], err)
		}

		host = endpointURL.Hostname()
		portStr := endpointURL.Port()
		if portStr == "" {
			// Default ports based on scheme
			switch endpointURL.Scheme {
			case "redis":
				port = 6379
			case "rediss":
				port = 6380
			default:
				port = 6379
			}
		} else {
			port, err = strconv.Atoi(portStr)
			if err != nil {
				return nil, fmt.Errorf("invalid port in endpoint URL %s: %w", dbConfig.Endpoints[0], err)
			}
		}

		// Override TLS setting based on scheme if not explicitly set
		if endpointURL.Scheme == "rediss" {
			dbConfig.TLS = true
		}
	} else {
		return nil, fmt.Errorf("no endpoints found in database configuration")
	}

	return &RedisConnectionConfig{
		Host:                 host,
		Port:                 port,
		Username:             dbConfig.Username,
		Password:             dbConfig.Password,
		TLS:                  dbConfig.TLS,
		BdbID:                dbConfig.BdbID,
		CertificatesLocation: dbConfig.CertificatesLocation,
		Endpoints:            dbConfig.Endpoints,
	}, nil
}

// ClientFactory manages Redis client creation and lifecycle
type ClientFactory struct {
	config  *RedisConnectionConfig
	clients map[string]redis.UniversalClient
	mutex   sync.RWMutex
}

// NewClientFactory creates a new client factory with the specified configuration
func NewClientFactory(config *RedisConnectionConfig) *ClientFactory {
	return &ClientFactory{
		config:  config,
		clients: make(map[string]redis.UniversalClient),
	}
}

// CreateClientOptions represents options for creating Redis clients
type CreateClientOptions struct {
	Protocol                 int
	MaintNotificationsConfig *maintnotifications.Config
	MaxRetries               int
	PoolSize                 int
	MinIdleConns             int
	MaxActiveConns           int
	ClientName               string
	DB                       int
	ReadTimeout              time.Duration
	WriteTimeout             time.Duration
}

// DefaultCreateClientOptions returns default options for creating Redis clients
func DefaultCreateClientOptions() *CreateClientOptions {
	return &CreateClientOptions{
		Protocol: 3, // RESP3 by default for push notifications
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:           maintnotifications.ModeEnabled,
			HandoffTimeout: 30 * time.Second,
			RelaxedTimeout: 10 * time.Second,
			MaxWorkers:     20,
		},
		MaxRetries:     3,
		PoolSize:       10,
		MinIdleConns:   10,
		MaxActiveConns: 10,
	}
}

func (cf *ClientFactory) PrintPoolStats(t *testing.T) {
	cf.mutex.RLock()
	defer cf.mutex.RUnlock()

	for key, client := range cf.clients {
		stats := client.PoolStats()
		t.Logf("Pool stats for client %s: %+v", key, stats)
	}
}

// Create creates a new Redis client with the specified options and connects it
func (cf *ClientFactory) Create(key string, options *CreateClientOptions) (redis.UniversalClient, error) {
	if options == nil {
		options = DefaultCreateClientOptions()
	}

	cf.mutex.Lock()
	defer cf.mutex.Unlock()

	// Check if client already exists
	if client, exists := cf.clients[key]; exists {
		return client, nil
	}

	var client redis.UniversalClient

	// Determine if this is a cluster configuration
	if len(cf.config.Endpoints) > 1 || cf.isClusterEndpoint() {
		// Create cluster client
		clusterOptions := &redis.ClusterOptions{
			Addrs:                    cf.getAddresses(),
			Username:                 cf.config.Username,
			Password:                 cf.config.Password,
			Protocol:                 options.Protocol,
			MaintNotificationsConfig: options.MaintNotificationsConfig,
			MaxRetries:               options.MaxRetries,
			PoolSize:                 options.PoolSize,
			MinIdleConns:             options.MinIdleConns,
			MaxActiveConns:           options.MaxActiveConns,
			ClientName:               options.ClientName,
		}

		if options.ReadTimeout > 0 {
			clusterOptions.ReadTimeout = options.ReadTimeout
		}
		if options.WriteTimeout > 0 {
			clusterOptions.WriteTimeout = options.WriteTimeout
		}

		if cf.config.TLS {
			clusterOptions.TLSConfig = &tls.Config{
				InsecureSkipVerify: true, // For testing purposes
			}
		}

		client = redis.NewClusterClient(clusterOptions)
	} else {
		// Create single client
		clientOptions := &redis.Options{
			Addr:                     fmt.Sprintf("%s:%d", cf.config.Host, cf.config.Port),
			Username:                 cf.config.Username,
			Password:                 cf.config.Password,
			DB:                       options.DB,
			Protocol:                 options.Protocol,
			MaintNotificationsConfig: options.MaintNotificationsConfig,
			MaxRetries:               options.MaxRetries,
			PoolSize:                 options.PoolSize,
			MinIdleConns:             options.MinIdleConns,
			MaxActiveConns:           options.MaxActiveConns,
			ClientName:               options.ClientName,
		}

		if options.ReadTimeout > 0 {
			clientOptions.ReadTimeout = options.ReadTimeout
		}
		if options.WriteTimeout > 0 {
			clientOptions.WriteTimeout = options.WriteTimeout
		}

		if cf.config.TLS {
			clientOptions.TLSConfig = &tls.Config{
				InsecureSkipVerify: true, // For testing purposes
			}
		}

		client = redis.NewClient(clientOptions)
	}

	// Store the client
	cf.clients[key] = client

	return client, nil
}

// Get retrieves an existing client by key or the first one if no key is provided
func (cf *ClientFactory) Get(key string) redis.UniversalClient {
	cf.mutex.RLock()
	defer cf.mutex.RUnlock()

	if key != "" {
		return cf.clients[key]
	}

	// Return the first client if no key is provided
	for _, client := range cf.clients {
		return client
	}

	return nil
}

// GetAll returns all created clients
func (cf *ClientFactory) GetAll() map[string]redis.UniversalClient {
	cf.mutex.RLock()
	defer cf.mutex.RUnlock()

	result := make(map[string]redis.UniversalClient)
	for key, client := range cf.clients {
		result[key] = client
	}

	return result
}

// DestroyAll closes and removes all created clients
func (cf *ClientFactory) DestroyAll() error {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()

	var lastErr error
	for key, client := range cf.clients {
		if err := client.Close(); err != nil {
			lastErr = err
		}
		delete(cf.clients, key)
	}

	return lastErr
}

// Destroy closes and removes a specific client
func (cf *ClientFactory) Destroy(key string) error {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()

	client, exists := cf.clients[key]
	if !exists {
		return fmt.Errorf("client %s not found", key)
	}

	err := client.Close()
	delete(cf.clients, key)
	return err
}

// GetConfig returns the connection configuration
func (cf *ClientFactory) GetConfig() *RedisConnectionConfig {
	return cf.config
}

// Helper methods

// isClusterEndpoint determines if the configuration represents a cluster
func (cf *ClientFactory) isClusterEndpoint() bool {
	// Check if any endpoint contains cluster-related keywords
	for _, endpoint := range cf.config.Endpoints {
		if strings.Contains(strings.ToLower(endpoint), "cluster") {
			return true
		}
	}

	// Check if we have multiple raw endpoints
	if len(cf.config.Endpoints) > 1 {
		return true
	}

	return false
}

// getAddresses returns a list of addresses for cluster configuration
func (cf *ClientFactory) getAddresses() []string {
	if len(cf.config.Endpoints) > 0 {
		addresses := make([]string, 0, len(cf.config.Endpoints))
		for _, endpoint := range cf.config.Endpoints {
			if parsedURL, err := url.Parse(endpoint); err == nil {
				addr := parsedURL.Host
				if addr != "" {
					addresses = append(addresses, addr)
				}
			}
		}
		if len(addresses) > 0 {
			return addresses
		}
	}

	// Fallback to single address
	return []string{fmt.Sprintf("%s:%d", cf.config.Host, cf.config.Port)}
}

// Utility functions for common test scenarios

// CreateTestClientFactory creates a client factory from environment configuration
func CreateTestClientFactory(databaseName string) (*ClientFactory, error) {
	envConfig, err := GetEnvConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get environment config: %w", err)
	}

	databasesConfig, err := GetDatabaseConfigFromEnv(envConfig.RedisEndpointsConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get database config: %w", err)
	}

	dbConfig, err := GetDatabaseConfig(databasesConfig, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to get database config for %s: %w", databaseName, err)
	}

	return NewClientFactory(dbConfig), nil
}

// CreateTestFaultInjector creates a fault injector client from environment configuration
func CreateTestFaultInjector() (*FaultInjectorClient, error) {
	envConfig, err := GetEnvConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get environment config: %w", err)
	}

	return NewFaultInjectorClient(envConfig.FaultInjectorURL), nil
}

// GetAvailableDatabases returns a list of available database names from the configuration
func GetAvailableDatabases(configPath string) ([]string, error) {
	databasesConfig, err := GetDatabaseConfigFromEnv(configPath)
	if err != nil {
		return nil, err
	}

	databases := make([]string, 0, len(databasesConfig))
	for name := range databasesConfig {
		databases = append(databases, name)
	}

	return databases, nil
}
