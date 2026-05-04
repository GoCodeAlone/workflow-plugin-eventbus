package eventbus

import (
	"context"
	"fmt"
	"sync"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ── stream registry ───────────────────────────────────────────────────────────

var (
	streamMu       sync.RWMutex
	streamRegistry = make(map[string]*eventbusv1.StreamConfig)
)

// RegisterStream stores a StreamConfig in the global registry under instanceName.
func RegisterStream(instanceName string, cfg *eventbusv1.StreamConfig) {
	streamMu.Lock()
	defer streamMu.Unlock()
	streamRegistry[instanceName] = cfg
}

// GetStream looks up a StreamConfig by instance name.
func GetStream(instanceName string) (*eventbusv1.StreamConfig, bool) {
	streamMu.RLock()
	defer streamMu.RUnlock()
	cfg, ok := streamRegistry[instanceName]
	return cfg, ok
}

// UnregisterStream removes a StreamConfig from the registry.
func UnregisterStream(instanceName string) {
	streamMu.Lock()
	defer streamMu.Unlock()
	delete(streamRegistry, instanceName)
}

// ── streamModule ──────────────────────────────────────────────────────────────

// streamModule implements sdk.ModuleInstance for the infra.eventbus.stream
// module type. It declares a durable JetStream stream (or Kafka topic) and
// registers its config for use by step and trigger modules.
type streamModule struct {
	instanceName string
	config       *eventbusv1.StreamConfig
}

// Compile-time assertion: streamModule implements sdk.ModuleInstance.
var _ sdk.ModuleInstance = (*streamModule)(nil)

// NewStreamModule creates a streamModule from a typed StreamConfig proto.
//
// Returns an error if:
//   - config.name is empty
//   - config.subjects contains no entries
func NewStreamModule(instanceName string, cfg *eventbusv1.StreamConfig) (sdk.ModuleInstance, error) {
	if cfg.GetName() == "" {
		return nil, fmt.Errorf("infra.eventbus.stream %q: config.name is required", instanceName)
	}
	if len(cfg.GetSubjects()) == 0 {
		return nil, fmt.Errorf("infra.eventbus.stream %q: config.subjects must contain at least one entry", instanceName)
	}
	return &streamModule{instanceName: instanceName, config: cfg}, nil
}

// Init registers the stream config in the global registry.
func (m *streamModule) Init() error {
	RegisterStream(m.instanceName, m.config)
	return nil
}

// Start is a no-op for the stream module.
func (m *streamModule) Start(_ context.Context) error { return nil }

// Stop unregisters the stream config from the global registry.
func (m *streamModule) Stop(_ context.Context) error {
	UnregisterStream(m.instanceName)
	return nil
}
