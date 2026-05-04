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
// Steps look up the URI here to obtain a NATS (or Kafka/Kinesis) connection.
var (
	urlMu           sync.RWMutex
	busURIRegistry  = make(map[string]string)
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

// ── clusterModule ─────────────────────────────────────────────────────────────

// clusterModule implements sdk.ModuleInstance for the infra.eventbus module type.
// It validates the ClusterConfig, registers it for use by stream, consumer, and
// step modules, and resolves the broker URI from environment variables.
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
// If neither env var is set the URI is not registered; steps will fail at
// execution time if they need a live connection. This is intentional — the
// module remains valid for IaC-only (plan/apply) workflows.
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

// Start is a no-op; NATS connections are established lazily by steps.
func (m *clusterModule) Start(_ context.Context) error { return nil }

// Stop unregisters the cluster config and URI from global registries.
func (m *clusterModule) Stop(_ context.Context) error {
	UnregisterCluster(m.instanceName)
	UnregisterBusURI(m.instanceName)
	return nil
}
