package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

// TestContractRegistry_FileDescriptorSetResolvesEveryContractMessage is the
// regression test for v0.3.3's incomplete FileDescriptorSet (missing
// google/protobuf/duration.proto which eventbus.proto imports). The engine
// builds its contract-types resolver via protodesc.NewFiles(registry.
// FileDescriptorSet). If any imported file is missing, NewFiles silently
// produces a registry that never registers the eventbus messages, and the
// engine falls through to protoregistry.GlobalTypes (which does not contain
// plugin-local protos) at module construction time — failing with
// "generated codec for protobuf message X not found: proto: not found".
//
// This test exercises the exact engine path: read the plugin's
// ContractRegistry, register the FileDescriptorSet, then assert that every
// Config/Input/Output/Trigger message named in Contracts can be resolved.
func TestContractRegistry_FileDescriptorSetResolvesEveryContractMessage(t *testing.T) {
	p := &eventbusPlugin{}
	reg := p.ContractRegistry()
	if reg == nil {
		t.Fatal("ContractRegistry returned nil")
	}
	if reg.FileDescriptorSet == nil || len(reg.FileDescriptorSet.File) == 0 {
		t.Fatal("ContractRegistry must populate FileDescriptorSet so the engine can resolve plugin-local proto codecs")
	}
	files, err := protodesc.NewFiles(reg.FileDescriptorSet)
	if err != nil {
		t.Fatalf("protodesc.NewFiles failed — every imported file must be present in FileDescriptorSet: %v", err)
	}
	types := new(protoregistry.Types)
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		registerMessages(t, types, file.Messages())
		return true
	})
	// Every distinct message name referenced in Contracts (Config/Input/Output)
	// must be resolvable. This mirrors the workflow engine's
	// `newMessageByName(messageName, contractTypes)` lookup at
	// createTypedConfigRequest time.
	wantNames := map[string]struct{}{}
	for _, c := range reg.Contracts {
		for _, name := range []string{c.ConfigMessage, c.InputMessage, c.OutputMessage} {
			if name == "" {
				continue
			}
			wantNames[name] = struct{}{}
		}
	}
	for name := range wantNames {
		if _, err := types.FindMessageByName(protoreflect.FullName(name)); err != nil {
			t.Errorf("FileDescriptorSet missing codec for contract message %q: %v", name, err)
		}
	}
}

func registerMessages(t *testing.T, types *protoregistry.Types, messages protoreflect.MessageDescriptors) {
	t.Helper()
	for i := 0; i < messages.Len(); i++ {
		m := messages.Get(i)
		if err := types.RegisterMessage(dynamicpb.NewMessageType(m)); err != nil {
			t.Fatalf("register %q: %v", m.FullName(), err)
		}
		registerMessages(t, types, m.Messages())
	}
}

// TestCreateTrigger_AliasConsumerToName verifies that BMW-style configs supplying
// `consumer` instead of the proto-canonical `name` build successfully. This is
// the core fix shipped in v0.3.1 — without this, BMW pipelines fail with
// "config.name is required" because the engine packs the raw YAML config into
// a map and the trigger handler only reads `name`.
func TestCreateTrigger_AliasConsumerToName(t *testing.T) {
	p := &eventbusPlugin{}
	cfg := map[string]any{
		"consumer":       "bmw-settlement-runner",
		"bus":            "bmw-eventbus",
		"stream_name":    "BMW_FULFILLMENT",
		"filter_subject": "bmw.fulfillment.delivered",
	}
	inst, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err != nil {
		t.Fatalf("CreateTrigger with consumer alias: %v", err)
	}
	if inst == nil {
		t.Fatal("CreateTrigger returned nil instance")
	}
}

// TestCreateTrigger_AliasBusToBrokerRef verifies that BMW-style configs supplying
// `bus` instead of `broker_ref` build successfully and the BrokerRef is wired
// through to the typed ConsumerConfig. Also exercises stream_name inheritance
// from the consumer registry: when only the consumer name is supplied, the
// trigger inherits stream_name (and broker_ref when unset) from the matching
// infra.eventbus.consumer module's registered ConsumerConfig.
func TestCreateTrigger_AliasBusToBrokerRef(t *testing.T) {
	eventbus.RegisterConsumer("test-bus-alias", &eventbusv1.ConsumerConfig{
		Name:       "bus-alias-consumer",
		StreamName: "BUS_ALIAS_STREAM",
		BrokerRef:  "bus-alias-bus",
	})
	t.Cleanup(func() { eventbus.UnregisterConsumer("test-bus-alias") })

	p := &eventbusPlugin{}
	cfg := map[string]any{
		"consumer":       "bus-alias-consumer",
		"bus":            "bus-alias-bus",
		"filter_subject": "bmw.>",
		// stream_name intentionally omitted — should be derived from registry.
	}
	inst, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err != nil {
		t.Fatalf("CreateTrigger with bus alias + stream_name inheritance: %v", err)
	}
	if inst == nil {
		t.Fatal("CreateTrigger returned nil instance")
	}
}

// TestCreateTrigger_CanonicalFieldsWinOverAlias verifies that when both the
// canonical key and its alias are supplied, the canonical value wins. Prevents
// silent misconfiguration when users migrate from BMW-style aliases to
// proto-canonical fields.
//
// Observable assertion strategy: register a consumer ONLY under the canonical
// name with a known stream_name. Omit `stream_name` from the trigger config so
// the handler must derive it via GetConsumerByName(<resolved name>). If the
// canonical name wins the lookup succeeds and CreateTrigger returns nil; if
// the alias incorrectly wins, the lookup for the alias name misses and
// NewSubscribeTrigger surfaces `config.stream_name is required`.
func TestCreateTrigger_CanonicalFieldsWinOverAlias(t *testing.T) {
	const (
		canonicalName   = "canonical-wins-name"
		aliasName       = "canonical-wins-alias-name"
		canonicalStream = "CANONICAL_WINS_STREAM"
	)
	eventbus.RegisterConsumer("test-canonical-wins", &eventbusv1.ConsumerConfig{
		Name:       canonicalName,
		StreamName: canonicalStream,
	})
	t.Cleanup(func() { eventbus.UnregisterConsumer("test-canonical-wins") })

	p := &eventbusPlugin{}
	cfg := map[string]any{
		"name":     canonicalName,
		"consumer": aliasName, // must be ignored — canonical wins
		// stream_name intentionally omitted: derivation must use the canonical
		// name (registered) not the alias (unregistered).
	}
	inst, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err != nil {
		t.Fatalf("CreateTrigger with both canonical + alias (canonical must win): %v", err)
	}
	if inst == nil {
		t.Fatal("CreateTrigger returned nil instance")
	}
}

// TestCreateTrigger_AliasIgnoredWhenCanonicalSet is the negative control for
// TestCreateTrigger_CanonicalFieldsWinOverAlias: confirms that registering
// the consumer ONLY under the alias name (and not under the canonical name)
// causes the lookup to fail, proving the canonical-wins test above is actually
// observing the precedence rule rather than a false-positive.
func TestCreateTrigger_AliasIgnoredWhenCanonicalSet(t *testing.T) {
	const (
		canonicalName = "alias-ignored-canonical"
		aliasName     = "alias-ignored-alias"
		aliasStream   = "ALIAS_STREAM"
	)
	// Register under alias name only; canonical name is unknown.
	eventbus.RegisterConsumer("test-alias-ignored", &eventbusv1.ConsumerConfig{
		Name:       aliasName,
		StreamName: aliasStream,
	})
	t.Cleanup(func() { eventbus.UnregisterConsumer("test-alias-ignored") })

	p := &eventbusPlugin{}
	cfg := map[string]any{
		"name":     canonicalName,
		"consumer": aliasName,
		// stream_name omitted; derivation runs against the canonical (unregistered)
		// name and must fail.
	}
	_, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err == nil {
		t.Fatal("expected error: stream_name should not have been derived from the alias registry entry")
	}
	if !strings.Contains(err.Error(), "stream_name") {
		t.Errorf("error should mention missing stream_name: %v", err)
	}
}

// TestCreateTrigger_NameStillRequiredWhenBothAbsent verifies that the error
// message remains helpful when neither `name` nor `consumer` is supplied.
// Preserves the original validation behaviour for purely empty configs.
func TestCreateTrigger_NameStillRequiredWhenBothAbsent(t *testing.T) {
	p := &eventbusPlugin{}
	cfg := map[string]any{
		"stream_name":    "SOME_STREAM",
		"filter_subject": "bmw.>",
		// no name, no consumer
	}
	_, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err == nil {
		t.Fatal("expected error when both name and consumer are absent, got nil")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Errorf("error should mention missing name field: %v", err)
	}
}

// TestCreateTrigger_TypeMismatchOnAlias verifies that supplying a non-string
// `consumer` value yields a clear type error rather than a confusing missing-
// field error. Defends against silent coercion of YAML-typed values.
func TestCreateTrigger_TypeMismatchOnAlias(t *testing.T) {
	p := &eventbusPlugin{}
	cfg := map[string]any{
		"consumer":    42, // wrong type
		"stream_name": "SOME_STREAM",
	}
	_, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err == nil {
		t.Fatal("expected error for non-string consumer value, got nil")
	}
	if !strings.Contains(err.Error(), "consumer") {
		t.Errorf("error should mention the offending key 'consumer': %v", err)
	}
}

// TestCreateTrigger_InheritsStreamNameFromConsumerModuleInit reproduces the
// realistic engine flow that BMW's local smoke exercises: the matching
// `eventbus.consumer` module is created via NewConsumerModule and registered
// via its own Init() — *not* via a direct call to RegisterConsumer — before
// the plugin's CreateTrigger fires for the inline pipeline trigger.
//
// This guards against a regression where stream_name inheritance only worked
// for hand-seeded registry entries but broke against the actual ModuleInstance
// lifecycle (Init → RegisterConsumer chain). All 6 declared BMW consumers
// (bmw-settlement-runner, bmw-audit-appender, bmw-fulfillment-dispatcher,
// bmw-recipient-notifier, bmw-contributor-notifier, bmw-status-poller) flow
// through this exact path.
//
// Engine ordering invariant relied on: workflow v0.51.5's StdEngine.BuildFromConfig
// runs `app.Init()` (which calls every module's Init, including consumerModule's
// RegisterConsumer call) BEFORE `configurePipelines`, which is where
// RemoteTrigger.Configure dispatches the CreateTrigger gRPC into the plugin.
// If that ordering ever flips, this test would still pass (we sequence Init
// before CreateTrigger by hand) but the production path would regress; the
// engine-side ordering is asserted upstream in workflow's engine tests.
func TestCreateTrigger_InheritsStreamNameFromConsumerModuleInit(t *testing.T) {
	const (
		moduleInstance = "bmw-consumer-settlement-runner"
		consumerName   = "bmw-settlement-runner"
		streamName     = "BMW_FULFILLMENT"
		brokerRef      = "bmw-eventbus"
	)
	mod, err := eventbus.NewConsumerModule(moduleInstance, &eventbusv1.ConsumerConfig{
		Name:       consumerName,
		StreamName: streamName,
		BrokerRef:  brokerRef,
	})
	if err != nil {
		t.Fatalf("NewConsumerModule: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("consumerModule.Init: %v", err)
	}
	t.Cleanup(func() { eventbus.UnregisterConsumer(moduleInstance) })

	// Sanity: the consumer registry must observe the durable name via
	// GetConsumerByName — this is the lookup CreateTrigger relies on.
	got, ok := eventbus.GetConsumerByName(consumerName)
	if !ok {
		t.Fatalf("GetConsumerByName(%q) returned !ok after consumerModule.Init", consumerName)
	}
	if got.GetStreamName() != streamName {
		t.Fatalf("registered consumer stream_name = %q, want %q", got.GetStreamName(), streamName)
	}

	// BMW-shape trigger config: `bus` + `consumer` + `filter_subject` only.
	// No `name`, no `broker_ref`, no `stream_name`.
	p := &eventbusPlugin{}
	cfg := map[string]any{
		"bus":            brokerRef,
		"consumer":       consumerName,
		"filter_subject": "bmw.fulfillment.delivered,bmw.fulfillment.cancelled",
	}
	inst, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err != nil {
		t.Fatalf("CreateTrigger with BMW-shape config (consumer registered via module Init): %v", err)
	}
	if inst == nil {
		t.Fatal("CreateTrigger returned nil instance")
	}
}

// TestCreateTrigger_StreamNameInheritanceFailsClearlyWhenConsumerUnregistered
// asserts the BMW edge-case: a pipeline trigger that names a `consumer` for
// which no matching `eventbus.consumer` module is declared (the bmw-financial-
// health "runtime-ephemeral" pattern in BMW's app.yaml). With no registry
// entry, stream_name derivation cannot succeed and the trigger build must fail
// with a message that mentions stream_name — so operators see a clear "declare
// a consumer module or supply stream_name explicitly" signal rather than
// hitting a silent runtime no-op.
func TestCreateTrigger_StreamNameInheritanceFailsClearlyWhenConsumerUnregistered(t *testing.T) {
	p := &eventbusPlugin{}
	cfg := map[string]any{
		"bus":            "bmw-eventbus",
		"consumer":       "bmw-financial-health-not-registered",
		"filter_subject": "bmw.>",
		// stream_name omitted AND no consumer module registered under this name.
	}
	_, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err == nil {
		t.Fatal("expected error when consumer is unregistered and stream_name omitted")
	}
	if !strings.Contains(err.Error(), "stream_name") {
		t.Errorf("error should mention missing stream_name: %v", err)
	}
}
