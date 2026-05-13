package main

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/steps"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// eventbusPlugin implements sdk.PluginProvider, sdk.TypedModuleProvider,
// sdk.TypedStepProvider, sdk.TriggerProvider, and sdk.ContractProvider.
type eventbusPlugin struct{}

// Compile-time assertions.
var (
	_ sdk.PluginProvider      = (*eventbusPlugin)(nil)
	_ sdk.TypedModuleProvider = (*eventbusPlugin)(nil)
	_ sdk.TypedStepProvider   = (*eventbusPlugin)(nil)
	_ sdk.TriggerProvider     = (*eventbusPlugin)(nil)
	_ sdk.ContractProvider    = (*eventbusPlugin)(nil)
)

// ── PluginProvider ────────────────────────────────────────────────────────────

// Manifest returns the plugin metadata used by the workflow engine for
// discovery and capability negotiation.
func (p *eventbusPlugin) Manifest() sdk.PluginManifest {
	return sdk.PluginManifest{
		Name:        "workflow-plugin-eventbus",
		Version:     version,
		Author:      "GoCodeAlone",
		Description: "Provisions durable event-bus clusters (NATS / Kafka / Kinesis) as IaC and exposes typed pipeline steps for publish / consume operations.",
	}
}

// ── TypedModuleProvider ───────────────────────────────────────────────────────

// moduleFactories is the ordered list of TypedModuleProvider instances, one per
// module type family.
var moduleFactories = []sdk.TypedModuleProvider{
	&eventbus.ClusterModuleFactory{},
	&eventbus.StreamModuleFactory{},
	&eventbus.ConsumerModuleFactory{},
	&eventbus.SubscribeTriggerModuleFactory{},
}

// TypedModuleTypes returns all module types served by this plugin, including the
// trigger.eventbus.subscribe type which is exposed as a module in the gRPC path.
func (p *eventbusPlugin) TypedModuleTypes() []string {
	types := make([]string, 0, len(moduleFactories))
	for _, f := range moduleFactories {
		types = append(types, f.TypedModuleTypes()...)
	}
	return types
}

// CreateTypedModule routes the create request to the appropriate factory.
func (p *eventbusPlugin) CreateTypedModule(typeName, name string, config *anypb.Any) (sdk.ModuleInstance, error) {
	for _, f := range moduleFactories {
		inst, err := f.CreateTypedModule(typeName, name, config)
		if err == nil {
			return inst, nil
		}
		if !errors.Is(err, sdk.ErrTypedContractNotHandled) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("workflow-plugin-eventbus: unknown module type %q", typeName)
}

// ── TypedStepProvider ─────────────────────────────────────────────────────────

// stepFactories is the ordered list of TypedStepProvider instances.
var stepFactories = []sdk.TypedStepProvider{
	steps.PublishFactory,
	steps.ConsumeFactory,
	steps.AckFactory,
}

// TypedStepTypes returns all step types served by this plugin.
func (p *eventbusPlugin) TypedStepTypes() []string {
	types := make([]string, 0, len(stepFactories))
	for _, f := range stepFactories {
		types = append(types, f.TypedStepTypes()...)
	}
	return types
}

// CreateTypedStep routes the create request to the appropriate factory.
func (p *eventbusPlugin) CreateTypedStep(typeName, name string, config *anypb.Any) (sdk.StepInstance, error) {
	for _, f := range stepFactories {
		inst, err := f.CreateTypedStep(typeName, name, config)
		if err == nil {
			return inst, nil
		}
		if !errors.Is(err, sdk.ErrTypedContractNotHandled) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("workflow-plugin-eventbus: unknown step type %q", typeName)
}

// ── TriggerProvider ───────────────────────────────────────────────────────────

// TriggerTypes returns the trigger type names this plugin provides.
func (p *eventbusPlugin) TriggerTypes() []string {
	return []string{"trigger.eventbus.subscribe"}
}

// CreateTrigger creates a trigger instance for the trigger.eventbus.subscribe type.
// In the external plugin gRPC path the callback client is never wired, so cb is
// always nil and Start is a no-op. The trigger module is created via
// CreateTypedModule in that path; this method exists for the legacy TriggerProvider
// interface.
func (p *eventbusPlugin) CreateTrigger(typeName string, config map[string]any, cb sdk.TriggerCallback) (sdk.TriggerInstance, error) {
	if typeName != "trigger.eventbus.subscribe" {
		return nil, fmt.Errorf("workflow-plugin-eventbus: unknown trigger type %q", typeName)
	}
	// Perform explicit type assertions so callers get a clear error when a field
	// is present but has the wrong type (e.g. config["name"] = 42 gives
	// "config[name] must be a string, got int" rather than "config.name is required").
	//
	// Alias support: BMW and other downstream consumers express trigger configs in
	// terms of the durable consumer row's user-facing identifiers — `consumer` (the
	// consumer name) and `bus` (the broker module name) — rather than the proto
	// canonical fields `name` / `broker_ref`. Accept both forms; canonical fields
	// win when both are supplied. See PR fixing "config.name is required" for the
	// BMW eventbus pipelines (workflow-plugin-eventbus v0.3.1).
	name, err := configStringAlias(config, "name", "consumer")
	if err != nil {
		return nil, fmt.Errorf("workflow-plugin-eventbus: CreateTrigger %q: %w", typeName, err)
	}
	brokerRef, err := configStringAlias(config, "broker_ref", "bus")
	if err != nil {
		return nil, fmt.Errorf("workflow-plugin-eventbus: CreateTrigger %q: %w", typeName, err)
	}
	streamName, err := configString(config, "stream_name")
	if err != nil {
		return nil, fmt.Errorf("workflow-plugin-eventbus: CreateTrigger %q: %w", typeName, err)
	}
	// stream_name fallback: when only the consumer name is supplied (BMW pattern —
	// the consumer's stream binding is declared by the matching infra.eventbus.consumer
	// module), inherit stream_name from the registered consumer. Returns "" if no
	// consumer with this name is registered yet; NewSubscribeTrigger will then
	// surface the missing-stream_name error with a helpful pointer.
	if streamName == "" && name != "" {
		if reg, ok := eventbus.GetConsumerByName(name); ok {
			streamName = reg.GetStreamName()
			// Inherit broker_ref too when the caller did not pin one explicitly,
			// so the trigger uses the same broker the consumer module bound to.
			if brokerRef == "" {
				brokerRef = reg.GetBrokerRef()
			}
		}
	}
	filterSubject, _ := configString(config, "filter_subject") //nolint:errcheck // optional field
	cfg := &eventbusv1.ConsumerConfig{
		Name:          name,
		StreamName:    streamName,
		FilterSubject: filterSubject,
		BrokerRef:     brokerRef,
	}
	inst, err := eventbus.NewSubscribeTrigger(typeName, cfg, cb)
	if err != nil {
		return nil, err
	}
	return inst.(sdk.TriggerInstance), nil
}

// configString extracts key from config as a string. Returns an error if the
// key is present but not a string type.
func configString(config map[string]any, key string) (string, error) {
	v, ok := config[key]
	if !ok {
		return "", nil // absent is fine; required-field validation is in NewSubscribeTrigger
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("config[%s] must be a string, got %T", key, v)
	}
	return s, nil
}

// configStringAlias extracts key from config as a string, falling back to the
// supplied alias key when key is absent or empty. The canonical key wins when
// both are present and non-empty. Returns an error if either key is present
// but not a string type. Used to accept user-friendly trigger config keys
// (`consumer`, `bus`) as aliases for the proto-canonical fields
// (`name`, `broker_ref`).
func configStringAlias(config map[string]any, key, alias string) (string, error) {
	v, err := configString(config, key)
	if err != nil {
		return "", err
	}
	if v != "" {
		return v, nil
	}
	return configString(config, alias)
}

// ── ContractProvider ──────────────────────────────────────────────────────────

// ContractRegistry returns the typed contract descriptors for all plugin
// capabilities. These match the entries in plugin.contracts.json and are used
// by the engine for strict-proto contract negotiation.
func (p *eventbusPlugin) ContractRegistry() *pb.ContractRegistry {
	strict := pb.ContractMode_CONTRACT_MODE_STRICT_PROTO
	return &pb.ContractRegistry{
		Contracts: []*pb.ContractDescriptor{
			// ── modules ───────────────────────────────────────────────────────
			{
				Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
				ModuleType:    "eventbus.broker",
				ConfigMessage: "workflow.plugin.eventbus.v1.ClusterConfig",
				Mode:          strict,
			},
			{
				Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
				ModuleType:    "eventbus.stream",
				ConfigMessage: "workflow.plugin.eventbus.v1.StreamConfig",
				Mode:          strict,
			},
			{
				Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
				ModuleType:    "eventbus.consumer",
				ConfigMessage: "workflow.plugin.eventbus.v1.ConsumerConfig",
				Mode:          strict,
			},
			// ── steps ─────────────────────────────────────────────────────────
			{
				Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
				StepType:      "step.eventbus.publish",
				InputMessage:  "workflow.plugin.eventbus.v1.PublishRequest",
				OutputMessage: "workflow.plugin.eventbus.v1.PublishResponse",
				Mode:          strict,
			},
			{
				Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
				StepType:      "step.eventbus.consume",
				InputMessage:  "workflow.plugin.eventbus.v1.ConsumeRequest",
				OutputMessage: "workflow.plugin.eventbus.v1.ConsumeResponse",
				Mode:          strict,
			},
			{
				Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
				StepType:      "step.eventbus.ack",
				InputMessage:  "workflow.plugin.eventbus.v1.AckRequest",
				OutputMessage: "workflow.plugin.eventbus.v1.AckResponse",
				Mode:          strict,
			},
			// ── triggers ──────────────────────────────────────────────────────
			{
				Kind:          pb.ContractKind_CONTRACT_KIND_TRIGGER,
				TriggerType:   "trigger.eventbus.subscribe",
				ConfigMessage: "workflow.plugin.eventbus.v1.ConsumerConfig",
				OutputMessage: "workflow.plugin.eventbus.v1.Message",
				Mode:          strict,
			},
		},
	}
}
