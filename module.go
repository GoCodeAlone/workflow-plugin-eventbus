// Package eventbus implements the workflow-plugin-eventbus plugin.
// It provides infra.eventbus, infra.eventbus.stream, and infra.eventbus.consumer
// module types plus step and trigger types for durable event-bus integration.
package eventbus

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/anypb"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ── cluster registry ──────────────────────────────────────────────────────────

var (
	clusterMu       sync.RWMutex
	clusterRegistry = make(map[string]*eventbusv1.ClusterConfig)
)

// RegisterCluster stores a ClusterConfig in the global registry under instanceName.
func RegisterCluster(instanceName string, cfg *eventbusv1.ClusterConfig) {
	clusterMu.Lock()
	defer clusterMu.Unlock()
	clusterRegistry[instanceName] = cfg
}

// GetCluster looks up a ClusterConfig by instance name.
func GetCluster(instanceName string) (*eventbusv1.ClusterConfig, bool) {
	clusterMu.RLock()
	defer clusterMu.RUnlock()
	cfg, ok := clusterRegistry[instanceName]
	return cfg, ok
}

// UnregisterCluster removes a ClusterConfig from the registry.
func UnregisterCluster(instanceName string) {
	clusterMu.Lock()
	defer clusterMu.Unlock()
	delete(clusterRegistry, instanceName)
}

// ── bus URI registry ──────────────────────────────────────────────────────────

// busURIRegistry stores broker connection URIs keyed by module instance name.
// Steps look up the URI here via GetOrDialNATSConn to obtain a live connection.
var (
	urlMu          sync.RWMutex
	busURIRegistry = make(map[string]string)
)

// RegisterBusURI stores a broker URI under instanceName.
func RegisterBusURI(instanceName, uri string) {
	urlMu.Lock()
	defer urlMu.Unlock()
	busURIRegistry[instanceName] = uri
}

// GetBusURI returns the broker URI for instanceName.
func GetBusURI(instanceName string) (string, bool) {
	urlMu.RLock()
	defer urlMu.RUnlock()
	uri, ok := busURIRegistry[instanceName]
	return uri, ok
}

// UnregisterBusURI removes the URI entry for instanceName.
func UnregisterBusURI(instanceName string) {
	urlMu.Lock()
	defer urlMu.Unlock()
	delete(busURIRegistry, instanceName)
}

// ── NATS connection cache ─────────────────────────────────────────────────────

// natsConnCache holds one live *nats.Conn per bus instance name.
// Connections are created lazily on the first call to GetOrDialNATSConn.
// Module.Stop() closes and evicts the entry via closeNATSConn.
var (
	connCacheMu sync.Mutex
	natsConnCache = make(map[string]*nats.Conn)
)

// RegisterNATSConn stores a live connection under instanceName. Exported so that
// integration tests and the trigger can pre-populate the cache.
func RegisterNATSConn(instanceName string, conn *nats.Conn) {
	connCacheMu.Lock()
	defer connCacheMu.Unlock()
	natsConnCache[instanceName] = conn
}

// GetNATSConn returns the cached *nats.Conn for instanceName, or false if absent.
func GetNATSConn(instanceName string) (*nats.Conn, bool) {
	connCacheMu.Lock()
	defer connCacheMu.Unlock()
	conn, ok := natsConnCache[instanceName]
	return conn, ok
}

// GetOrDialNATSConn returns the cached NATS connection for instanceName, dialing
// a new one (via natsDialFn) if no live connection is cached. Returns an error if
// no URI is registered for instanceName or the dial fails.
//
// Lock ordering: connCacheMu and urlMu (held inside GetBusURI) are never held
// simultaneously. The URI lookup happens between the fast-path unlock and the
// slow-path re-lock so that no nested acquisition is possible.
func GetOrDialNATSConn(instanceName string) (*nats.Conn, error) {
	// Fast path: return cached live connection without touching urlMu.
	connCacheMu.Lock()
	conn, cached := natsConnCache[instanceName]
	if cached && conn != nil && conn.IsConnected() {
		connCacheMu.Unlock()
		return conn, nil
	}
	// Evict stale entry (closed or nil) while we hold the lock.
	delete(natsConnCache, instanceName)
	connCacheMu.Unlock()

	// Slow path: resolve URI with no lock held (avoids connCacheMu→urlMu nesting).
	uri, uriOk := GetBusURI(instanceName)
	if !uriOk || uri == "" {
		key := strings.ToUpper(strings.ReplaceAll(instanceName, "-", "_"))
		return nil, fmt.Errorf(
			"infra.eventbus: no URI registered for bus %q; set EVENTBUS_%s_URI or NATS_URL",
			instanceName, key)
	}

	// Dial outside any lock — natsDialFn may block for the connection timeout.
	nc, err := natsDialFn(uri)
	if err != nil {
		return nil, fmt.Errorf("infra.eventbus: dial NATS for bus %q at %s: %w", instanceName, uri, err)
	}

	// Re-acquire to insert; check again for a race where another goroutine dialled first.
	connCacheMu.Lock()
	defer connCacheMu.Unlock()
	if existing, ok := natsConnCache[instanceName]; ok && existing != nil && existing.IsConnected() {
		nc.Close() // discard the redundant connection we just dialled
		return existing, nil
	}
	natsConnCache[instanceName] = nc
	return nc, nil
}

// closeNATSConn closes the cached connection for instanceName and evicts it from
// the cache. It is idempotent — a missing or nil entry is not an error.
func closeNATSConn(instanceName string) {
	connCacheMu.Lock()
	defer connCacheMu.Unlock()
	if conn, ok := natsConnCache[instanceName]; ok {
		if conn != nil {
			conn.Close()
		}
		delete(natsConnCache, instanceName)
	}
}

// natsDialFn is the function used to create NATS connections. Tests may replace
// this package-level variable to inject a mock without a real NATS server.
var natsDialFn = func(uri string) (*nats.Conn, error) {
	return nats.Connect(uri,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.Timeout(5*time.Second),
	)
}

// DefaultBusConn returns a live NATS connection for the first registered
// infra.eventbus module. Suitable for single-bus workflow deployments (e.g. the
// BMW pilot). For multi-bus workflows, use GetOrDialNATSConn(instanceName)
// directly.
func DefaultBusConn() (*nats.Conn, error) {
	clusterMu.RLock()
	var first string
	for name := range clusterRegistry {
		first = name
		break
	}
	clusterMu.RUnlock()
	if first == "" {
		return nil, fmt.Errorf(
			"infra.eventbus: no bus module registered; add an infra.eventbus module to your workflow config",
		)
	}
	return GetOrDialNATSConn(first)
}

// ── ClusterModuleFactory (TypedModuleProvider) ────────────────────────────────

// ClusterModuleFactory implements sdk.TypedModuleProvider for the infra.eventbus
// module type. The plugin wires this factory into CreateTypedModule.
type ClusterModuleFactory struct{}

// Compile-time assertion: ClusterModuleFactory implements sdk.TypedModuleProvider.
var _ sdk.TypedModuleProvider = (*ClusterModuleFactory)(nil)

// TypedModuleTypes returns the single module type served by this factory.
func (f *ClusterModuleFactory) TypedModuleTypes() []string {
	return []string{"infra.eventbus"}
}

// CreateTypedModule unpacks the typed proto config and delegates to NewClusterModule.
func (f *ClusterModuleFactory) CreateTypedModule(typeName, name string, config *anypb.Any) (sdk.ModuleInstance, error) {
	if typeName != "infra.eventbus" {
		return nil, fmt.Errorf("%w: module type %q", sdk.ErrTypedContractNotHandled, typeName)
	}
	var cfg eventbusv1.ClusterConfig
	if config != nil {
		if err := config.UnmarshalTo(&cfg); err != nil {
			return nil, fmt.Errorf("infra.eventbus %q: unmarshal typed config: %w", name, err)
		}
	}
	return NewClusterModule(name, &cfg)
}

// ── clusterModule (ModuleInstance) ───────────────────────────────────────────

// clusterModule implements sdk.ModuleInstance for the infra.eventbus module type.
// It validates the ClusterConfig, registers the config and broker URI on Init(),
// and closes the cached NATS connection on Stop().
type clusterModule struct {
	instanceName string
	config       *eventbusv1.ClusterConfig
}

// Compile-time assertion: clusterModule implements sdk.ModuleInstance.
var _ sdk.ModuleInstance = (*clusterModule)(nil)

// NewClusterModule creates a clusterModule from a typed ClusterConfig proto.
//
// Returns an error if:
//   - config.provider is empty or unknown
//   - config.deploy_target is empty or unsupported for the given provider
func NewClusterModule(instanceName string, cfg *eventbusv1.ClusterConfig) (sdk.ModuleInstance, error) {
	if cfg.GetProvider() == "" {
		return nil, fmt.Errorf("infra.eventbus %q: config.provider is required", instanceName)
	}
	if cfg.GetDeployTarget() == "" {
		return nil, fmt.Errorf("infra.eventbus %q: config.deploy_target is required", instanceName)
	}
	target := providers.DeployTarget(cfg.GetDeployTarget())
	if err := providers.ValidateProviderTarget(cfg.GetProvider(), target); err != nil {
		return nil, fmt.Errorf("infra.eventbus %q: %w", instanceName, err)
	}
	return &clusterModule{instanceName: instanceName, config: cfg}, nil
}

// Init registers the cluster config and resolves the broker URI.
//
// URI resolution order:
//  1. EVENTBUS_<UPPERCASE_INSTANCE_NAME>_URI (e.g. EVENTBUS_BMW_EVENTBUS_URI)
//  2. NATS_URL (fallback for the nats provider only)
//
// If neither env var is set the URI is not registered. Steps that need a live
// connection will fail at execution time with a descriptive error. This is
// intentional — the module remains valid for IaC-only (plan/apply) workflows.
func (m *clusterModule) Init() error {
	RegisterCluster(m.instanceName, m.config)

	// Derive instance-specific env var: dashes → underscores, uppercase.
	key := strings.ToUpper(strings.ReplaceAll(m.instanceName, "-", "_"))
	uri := os.Getenv("EVENTBUS_" + key + "_URI")
	if uri == "" && m.config.GetProvider() == "nats" {
		uri = os.Getenv("NATS_URL")
	}
	if uri != "" {
		RegisterBusURI(m.instanceName, uri)
	}
	return nil
}

// Start is a no-op; NATS connections are established lazily by GetOrDialNATSConn.
func (m *clusterModule) Start(_ context.Context) error { return nil }

// Stop closes the cached NATS connection (if any) and unregisters the cluster
// config and URI from global registries.
func (m *clusterModule) Stop(_ context.Context) error {
	closeNATSConn(m.instanceName) // drain + close cached *nats.Conn, idempotent
	UnregisterBusURI(m.instanceName)
	UnregisterCluster(m.instanceName)
	return nil
}
