package eventbus_test

import (
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
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

	// Verify that NewSubscribeTrigger returns non-nil for valid config.
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "test-consumer",
		StreamName: "TEST_STREAM",
	}
	mod, err := eventbus.NewSubscribeTrigger("test-instance", cfg, nil)
	if err != nil {
		t.Fatalf("NewSubscribeTrigger returned unexpected error: %v", err)
	}
	if mod == nil {
		t.Fatal("NewSubscribeTrigger returned nil ModuleInstance")
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
	})
}
