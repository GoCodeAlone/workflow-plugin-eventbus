// Package eventbus implements the workflow-plugin-eventbus plugin.
// It provides infra.eventbus, infra.eventbus.stream, and infra.eventbus.consumer
// module types plus step and trigger types for durable event-bus integration.
package eventbus

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/anypb"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	natsruntime "github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
	pgchannelruntime "github.com/GoCodeAlone/workflow-plugin-eventbus/providers/pgchannel"
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

// ── broker instance registry ──────────────────────────────────────────────────

// brokerInstanceRegistry maps broker module instance names to their loaded
// *clusterModule. Stream + consumer modules look up their broker by name via
// LookupRuntime to get the runtime + cached Connection.
//
// Registration happens in clusterModule.Start (after runtime.Connect succeeds)
// and removal in clusterModule.Stop. Lookups before Start return "not yet
// started" so callers see a clear lifecycle error rather than a nil deref.
var brokerInstanceRegistry sync.Map // string → *clusterModule

// RegisterBrokerInstance stores m under name. Exported so integration tests
// can pre-seed the registry with a hand-built clusterModule.
func RegisterBrokerInstance(name string, m *clusterModule) {
	brokerInstanceRegistry.Store(name, m)
}

// UnregisterBrokerInstance removes the entry for name. Idempotent.
func UnregisterBrokerInstance(name string) { brokerInstanceRegistry.Delete(name) }

// LookupBrokerInstance returns the *clusterModule registered under name, or
// false when absent. Primarily for tests; production callers should use
// LookupRuntime, which also validates that Start has run.
func LookupBrokerInstance(name string) (*clusterModule, bool) {
	v, ok := brokerInstanceRegistry.Load(name)
	if !ok {
		return nil, false
	}
	return v.(*clusterModule), true
}

// LookupRuntime returns the RuntimeBroker + cached Connection for the named
// broker. Used by stream/consumer modules' Start (Group E) and by step
// factories + the trigger module (Group F) to dispatch through the provider
// abstraction.
//
// Returns an error when:
//   - no broker module is registered under name (Init never ran or wrong name);
//   - the broker module is registered but Start has not yet completed
//     (runtime/conn are still nil).
func LookupRuntime(name string) (providers.RuntimeBroker, providers.Connection, error) {
	m, ok := LookupBrokerInstance(name)
	if !ok {
		return nil, nil, fmt.Errorf("eventbus.broker %q not registered", name)
	}
	if m.runtime == nil || m.conn == nil {
		return nil, nil, fmt.Errorf("eventbus.broker %q: not yet started (runtime/conn nil)", name)
	}
	return m.runtime, m.conn, nil
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

// UnregisterNATSConn removes the cached connection entry for instanceName without
// closing the connection. Use this in tests that manage the connection's lifetime
// separately (e.g., via nc.Close() + embedded-server shutdown).
func UnregisterNATSConn(instanceName string) {
	connCacheMu.Lock()
	defer connCacheMu.Unlock()
	delete(natsConnCache, instanceName)
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

// DefaultBusConn returns a live NATS connection for the lexicographically first
// registered infra.eventbus module. Sorting ensures deterministic selection
// across invocations and concurrent goroutines, even when multiple buses are
// registered. For multi-bus workflows, use GetOrDialNATSConn(instanceName)
// directly.
func DefaultBusConn() (*nats.Conn, error) {
	clusterMu.RLock()
	names := make([]string, 0, len(clusterRegistry))
	for name := range clusterRegistry {
		names = append(names, name)
	}
	clusterMu.RUnlock()
	if len(names) == 0 {
		return nil, fmt.Errorf(
			"infra.eventbus: no bus module registered; add an infra.eventbus module to your workflow config",
		)
	}
	sort.Strings(names)
	return GetOrDialNATSConn(names[0])
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
// selects a provider runtime + opens a connection on Start(), and tears both
// down on Stop().
//
// The runtime + conn fields are populated by Start() based on config.Provider.
// They are nil before Start runs; LookupRuntime guards against that state so
// callers see a clear lifecycle error rather than a nil deref.
type clusterModule struct {
	instanceName string
	config       *eventbusv1.ClusterConfig

	// runtime is the provider-specific RuntimeBroker selected at Start time.
	// nil before Start, nil after Stop.
	runtime providers.RuntimeBroker
	// conn is the live broker Connection opened via runtime.Connect at Start.
	// nil before Start, nil after Stop.
	conn providers.Connection
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

// Start selects a provider runtime based on m.config.Provider, opens a
// broker Connection, and registers the module in the broker-instance
// registry so stream/consumer/step callers can resolve the runtime by
// instance name.
//
// Provider selection:
//   - "nats"      → providers/nats.NewRuntime()
//   - "pgchannel" → providers/pgchannel.NewRuntime()
//   - anything else → error
//
// DSN resolution for nats:
//
//	The runtime expects cfg.Dsn to carry the broker URL. The legacy Init()
//	resolves env-var URIs into the busURIRegistry. When provider is "nats"
//	and cfg.Dsn is empty we fall back to that registry so legacy
//	NATS_URL/EVENTBUS_<NAME>_URI flows keep working. The cfg pointer is
//	mutated in-place because clusterModule owns it.
//
// Step factories and the trigger module still use the legacy
// nats.Conn-direct path (GetOrDialNATSConn, RegisterNATSConn, etc.) until
// Group F refactors them onto LookupRuntime — that is intentional and
// preserves backward compatibility during the staged migration.
func (m *clusterModule) Start(ctx context.Context) error {
	provider := m.config.GetProvider()
	var rt providers.RuntimeBroker
	switch provider {
	case "nats":
		// Backwards-compat: when Dsn is empty fall back to the URI that
		// Init resolved from env vars.
		if m.config.GetDsn() == "" {
			if uri, ok := GetBusURI(m.instanceName); ok && uri != "" {
				m.config.Dsn = uri
			}
		}
		// Legacy degraded mode: when no DSN/URI is available at Start time
		// (the IaC-only / plan-apply flow + tests that exercise the missing-
		// URI error path) skip Connect and leave runtime/conn nil. Legacy
		// steps/trigger paths continue using GetOrDialNATSConn which surfaces
		// the missing-URI error at execution time with the same message.
		// LookupRuntime will return "not yet started" so any Group F caller
		// migrated to the runtime sees a clear error rather than a nil deref.
		if m.config.GetDsn() == "" {
			RegisterBrokerInstance(m.instanceName, m)
			return nil
		}
		rt = natsruntime.NewRuntime()
	case "pgchannel":
		rt = pgchannelruntime.NewRuntime()
	default:
		return fmt.Errorf("eventbus.broker %q: unsupported provider %q for Start (must be nats|pgchannel)", m.instanceName, provider)
	}
	conn, err := rt.Connect(ctx, m.config)
	if err != nil {
		return fmt.Errorf("eventbus.broker %q: connect: %w", m.instanceName, err)
	}
	m.runtime = rt
	m.conn = conn
	RegisterBrokerInstance(m.instanceName, m)
	return nil
}

// Stop tears down resources in the reverse order Start established them:
//  1. unregister the broker instance so new LookupRuntime calls fail fast;
//  2. close the runtime Connection (best-effort — errors are non-fatal but
//     logged via the returned error chain when nothing else fails);
//  3. close + evict any legacy nats.Conn cached for the instance (preserves
//     Group A/B compatibility for steps/triggers that have not yet migrated);
//  4. drop the legacy bus URI + cluster config from the global registries.
//
// Stop is safe to call when Start never ran (runtime/conn nil) — the runtime
// teardown is gated on conn != nil so the lifecycle remains symmetric.
func (m *clusterModule) Stop(_ context.Context) error {
	UnregisterBrokerInstance(m.instanceName)
	var closeErr error
	if m.conn != nil {
		closeErr = m.conn.Close()
		m.conn = nil
	}
	m.runtime = nil
	closeNATSConn(m.instanceName) // drain + close legacy cached *nats.Conn, idempotent
	UnregisterBusURI(m.instanceName)
	UnregisterCluster(m.instanceName)
	if closeErr != nil {
		return fmt.Errorf("eventbus.broker %q: close runtime connection: %w", m.instanceName, closeErr)
	}
	return nil
}
