package eventbus_test

import (
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"google.golang.org/protobuf/types/known/anypb"
)

// TestEventbusPlugin_SubscribeTriggerFactoryNonNil verifies that the
// SubscribeTriggerModuleFactory (trigger.eventbus.subscribe) returns a
// non-nil ModuleInstance when given a valid config. This is the BMW PR 277
// failure surface: trigger factory returning nil without error broke
// silent dispatch at engine startup.
func TestEventbusPlugin_SubscribeTriggerFactoryNonNil(t *testing.T) {
	f := &eventbus.SubscribeTriggerModuleFactory{}

	types := f.TypedModuleTypes()
	if len(types) == 0 {
		t.Fatal("SubscribeTriggerModuleFactory.TypedModuleTypes() returned empty slice")
	}
	found := false
	for _, tp := range types {
		if tp == "trigger.eventbus.subscribe" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("trigger.eventbus.subscribe not in TypedModuleTypes: %v", types)
	}

	// Exercise the BMW PR 277 failure surface via CreateTypedModule (the typed
	// factory path), not the lower-level NewSubscribeTrigger constructor.
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "test-consumer",
		StreamName: "TEST_STREAM",
	}
	packed, err := anypb.New(cfg)
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	mod, err := f.CreateTypedModule("trigger.eventbus.subscribe", "test-instance", packed)
	if err != nil {
		t.Fatalf("CreateTypedModule returned unexpected error: %v", err)
	}
	if mod == nil {
		t.Fatal("CreateTypedModule returned nil ModuleInstance")
	}
}

// TestEventbusPlugin_AllFactoriesNonNil verifies that all exported module
// factories register their expected types and construct non-nil instances with
// valid minimal configs.
func TestEventbusPlugin_AllFactoriesNonNil(t *testing.T) {
	t.Run("ClusterModuleFactory", func(t *testing.T) {
		f := &eventbus.ClusterModuleFactory{}
		types := f.TypedModuleTypes()
		if len(types) == 0 {
			t.Fatal("ClusterModuleFactory.TypedModuleTypes() returned empty slice")
		}
		found := false
		for _, tp := range types {
			if tp == "eventbus.broker" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("eventbus.broker not in TypedModuleTypes: %v", types)
		}
		// Instantiate with a minimal valid ClusterConfig to confirm non-nil return.
		cfg := &eventbusv1.ClusterConfig{
			Provider:     "pgchannel",
			BrokerTarget: "in_process",
			Dsn:          "postgres://u:p@localhost/db?sslmode=disable",
		}
		packed, err := anypb.New(cfg)
		if err != nil {
			t.Fatalf("anypb.New: %v", err)
		}
		mod, err := f.CreateTypedModule("eventbus.broker", "test-broker", packed)
		if err != nil {
			t.Fatalf("ClusterModuleFactory.CreateTypedModule: %v", err)
		}
		if mod == nil {
			t.Fatal("ClusterModuleFactory.CreateTypedModule returned nil ModuleInstance")
		}
	})

	t.Run("StreamModuleFactory", func(t *testing.T) {
		f := &eventbus.StreamModuleFactory{}
		types := f.TypedModuleTypes()
		if len(types) == 0 {
			t.Fatal("StreamModuleFactory.TypedModuleTypes() returned empty slice")
		}
		found := false
		for _, tp := range types {
			if tp == "eventbus.stream" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("eventbus.stream not in TypedModuleTypes: %v", types)
		}
		// Instantiate with a minimal valid StreamConfig to confirm non-nil return.
		cfg := &eventbusv1.StreamConfig{
			Name:     "test-stream",
			Subjects: []string{"TEST.>"},
		}
		packed, err := anypb.New(cfg)
		if err != nil {
			t.Fatalf("anypb.New: %v", err)
		}
		mod, err := f.CreateTypedModule("eventbus.stream", "test-stream", packed)
		if err != nil {
			t.Fatalf("StreamModuleFactory.CreateTypedModule: %v", err)
		}
		if mod == nil {
			t.Fatal("StreamModuleFactory.CreateTypedModule returned nil ModuleInstance")
		}
	})

	t.Run("ConsumerModuleFactory", func(t *testing.T) {
		f := &eventbus.ConsumerModuleFactory{}
		types := f.TypedModuleTypes()
		if len(types) == 0 {
			t.Fatal("ConsumerModuleFactory.TypedModuleTypes() returned empty slice")
		}
		found := false
		for _, tp := range types {
			if tp == "eventbus.consumer" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("eventbus.consumer not in TypedModuleTypes: %v", types)
		}
		// Instantiate with a minimal valid ConsumerConfig to confirm non-nil return.
		cfg := &eventbusv1.ConsumerConfig{
			Name:       "test-consumer",
			StreamName: "TEST_STREAM",
		}
		packed, err := anypb.New(cfg)
		if err != nil {
			t.Fatalf("anypb.New: %v", err)
		}
		mod, err := f.CreateTypedModule("eventbus.consumer", "test-consumer", packed)
		if err != nil {
			t.Fatalf("ConsumerModuleFactory.CreateTypedModule: %v", err)
		}
		if mod == nil {
			t.Fatal("ConsumerModuleFactory.CreateTypedModule returned nil ModuleInstance")
		}
	})
}
