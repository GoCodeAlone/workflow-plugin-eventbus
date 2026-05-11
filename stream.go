package eventbus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
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

// ── StreamModuleFactory (TypedModuleProvider) ─────────────────────────────────

// StreamModuleFactory implements sdk.TypedModuleProvider for the
// infra.eventbus.stream module type.
type StreamModuleFactory struct{}

// Compile-time assertion: StreamModuleFactory implements sdk.TypedModuleProvider.
var _ sdk.TypedModuleProvider = (*StreamModuleFactory)(nil)

// TypedModuleTypes returns the single module type served by this factory.
func (f *StreamModuleFactory) TypedModuleTypes() []string {
	return []string{"infra.eventbus.stream"}
}

// CreateTypedModule unpacks the typed proto config and delegates to NewStreamModule.
func (f *StreamModuleFactory) CreateTypedModule(typeName, name string, config *anypb.Any) (sdk.ModuleInstance, error) {
	if typeName != "infra.eventbus.stream" {
		return nil, fmt.Errorf("%w: module type %q", sdk.ErrTypedContractNotHandled, typeName)
	}
	var cfg eventbusv1.StreamConfig
	if config != nil {
		if err := config.UnmarshalTo(&cfg); err != nil {
			return nil, fmt.Errorf("infra.eventbus.stream %q: unmarshal typed config: %w", name, err)
		}
	}
	return NewStreamModule(name, &cfg)
}

// ── streamModule (ModuleInstance) ────────────────────────────────────────────

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

// Start resolves the broker named by m.config.BrokerRef via LookupRuntime and
// dispatches EnsureStream on the runtime so the stream exists before any
// producer publishes to it. The lookup is wrapped in a bounded-retry loop
// (10-second budget) because modular's Start phase has no guaranteed ordering
// between broker and stream modules — the broker may finish Start a few
// hundred microseconds after the stream's Start begins.
//
// Legacy compat: configs without broker_ref skip the dispatch entirely and
// keep behaving as registered-only (Init populated the global registry).
// This is the transitional shape; Group F refactors the step + trigger code
// to require broker_ref and the legacy path will be removed at that point.
func (m *streamModule) Start(ctx context.Context) error {
	brokerRef := m.config.GetBrokerRef()
	if brokerRef == "" {
		return nil
	}
	var (
		rb   providers.RuntimeBroker
		conn providers.Connection
	)
	if err := retryWithBackoff(ctx, 10*time.Second, func() error {
		var lookupErr error
		rb, conn, lookupErr = LookupRuntime(brokerRef)
		return lookupErr
	}); err != nil {
		return fmt.Errorf(
			"infra.eventbus.stream %q: broker %q not available within 10s: %w",
			m.instanceName, brokerRef, err,
		)
	}
	if err := rb.EnsureStream(ctx, conn, m.config); err != nil {
		return fmt.Errorf("infra.eventbus.stream %q: ensure: %w", m.instanceName, err)
	}
	return nil
}

// Stop unregisters the stream config from the global registry.
func (m *streamModule) Stop(_ context.Context) error {
	UnregisterStream(m.instanceName)
	return nil
}
