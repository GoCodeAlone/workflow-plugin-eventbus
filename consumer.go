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

// ── consumer registry ─────────────────────────────────────────────────────────

var (
	consumerMu       sync.RWMutex
	consumerRegistry = make(map[string]*eventbusv1.ConsumerConfig)
)

// RegisterConsumer stores a ConsumerConfig in the global registry under instanceName.
func RegisterConsumer(instanceName string, cfg *eventbusv1.ConsumerConfig) {
	consumerMu.Lock()
	defer consumerMu.Unlock()
	consumerRegistry[instanceName] = cfg
}

// GetConsumer looks up a ConsumerConfig by instance name.
func GetConsumer(instanceName string) (*eventbusv1.ConsumerConfig, bool) {
	consumerMu.RLock()
	defer consumerMu.RUnlock()
	cfg, ok := consumerRegistry[instanceName]
	return cfg, ok
}

// UnregisterConsumer removes a ConsumerConfig from the registry.
func UnregisterConsumer(instanceName string) {
	consumerMu.Lock()
	defer consumerMu.Unlock()
	delete(consumerRegistry, instanceName)
}

// GetConsumerByName looks up a ConsumerConfig by its durable consumer name
// (cfg.name), iterating all registered instances. This is used by
// step.eventbus.consume to resolve the consumer config from the durable name
// supplied in ConsumeRequest.consumer.
func GetConsumerByName(durableName string) (*eventbusv1.ConsumerConfig, bool) {
	consumerMu.RLock()
	defer consumerMu.RUnlock()
	for _, cfg := range consumerRegistry {
		if cfg.GetName() == durableName {
			return cfg, true
		}
	}
	return nil, false
}

// ── ConsumerModuleFactory (TypedModuleProvider) ───────────────────────────────

// ConsumerModuleFactory implements sdk.TypedModuleProvider for the
// infra.eventbus.consumer module type.
type ConsumerModuleFactory struct{}

// Compile-time assertion: ConsumerModuleFactory implements sdk.TypedModuleProvider.
var _ sdk.TypedModuleProvider = (*ConsumerModuleFactory)(nil)

// TypedModuleTypes returns the single module type served by this factory.
func (f *ConsumerModuleFactory) TypedModuleTypes() []string {
	return []string{"infra.eventbus.consumer"}
}

// CreateTypedModule unpacks the typed proto config and delegates to NewConsumerModule.
func (f *ConsumerModuleFactory) CreateTypedModule(typeName, name string, config *anypb.Any) (sdk.ModuleInstance, error) {
	if typeName != "infra.eventbus.consumer" {
		return nil, fmt.Errorf("%w: module type %q", sdk.ErrTypedContractNotHandled, typeName)
	}
	var cfg eventbusv1.ConsumerConfig
	if config != nil {
		if err := config.UnmarshalTo(&cfg); err != nil {
			return nil, fmt.Errorf("infra.eventbus.consumer %q: unmarshal typed config: %w", name, err)
		}
	}
	return NewConsumerModule(name, &cfg)
}

// ── consumerModule (ModuleInstance) ──────────────────────────────────────────

// consumerModule implements sdk.ModuleInstance for the infra.eventbus.consumer
// module type. It declares a durable JetStream consumer (or Kafka consumer group)
// and registers its config for use by step and trigger modules. No background
// goroutines are started — consumption is pull-based, driven by step execution.
type consumerModule struct {
	instanceName string
	config       *eventbusv1.ConsumerConfig
}

// Compile-time assertion: consumerModule implements sdk.ModuleInstance.
var _ sdk.ModuleInstance = (*consumerModule)(nil)

// NewConsumerModule creates a consumerModule from a typed ConsumerConfig proto.
//
// Returns an error if:
//   - config.name is empty
//   - config.stream_name is empty
func NewConsumerModule(instanceName string, cfg *eventbusv1.ConsumerConfig) (sdk.ModuleInstance, error) {
	if cfg.GetName() == "" {
		return nil, fmt.Errorf("infra.eventbus.consumer %q: config.name is required", instanceName)
	}
	if cfg.GetStreamName() == "" {
		return nil, fmt.Errorf("infra.eventbus.consumer %q: config.stream_name is required", instanceName)
	}
	return &consumerModule{instanceName: instanceName, config: cfg}, nil
}

// Init registers the consumer config in the global registry.
func (m *consumerModule) Init() error {
	RegisterConsumer(m.instanceName, m.config)
	return nil
}

// Start resolves the broker named by m.config.BrokerRef via LookupRuntime and
// dispatches EnsureConsumer on the runtime so the durable consumer exists
// before any worker pulls. Mirrors streamModule.Start: the lookup is wrapped
// in a bounded-retry loop (10-second budget) because modular's Start phase
// has no guaranteed ordering between broker and consumer modules.
//
// Pull-based consumption still has no background goroutines — the
// step.eventbus.consume step drives fetch calls. This Start only ensures the
// consumer record exists; it does not start a long-lived subscription.
//
// Legacy compat: configs without broker_ref skip the dispatch entirely and
// keep behaving as registered-only (Init populated the global registry).
// This is the transitional shape; Group F refactors the step + trigger code
// to require broker_ref and the legacy path will be removed at that point.
func (m *consumerModule) Start(ctx context.Context) error {
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
			"infra.eventbus.consumer %q: broker %q not available within 10s: %w",
			m.instanceName, brokerRef, err,
		)
	}
	streamName := m.config.GetStreamName()
	if err := rb.EnsureConsumer(ctx, conn, streamName, m.config); err != nil {
		return fmt.Errorf("infra.eventbus.consumer %q: ensure: %w", m.instanceName, err)
	}
	return nil
}

// Stop unregisters the consumer config from the global registry.
func (m *consumerModule) Stop(_ context.Context) error {
	UnregisterConsumer(m.instanceName)
	return nil
}
