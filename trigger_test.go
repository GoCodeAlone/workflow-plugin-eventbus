package eventbus_test

import (
	"context"
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

// ── SubscribeTriggerModuleFactory (TypedModuleProvider) ───────────────────────

func TestSubscribeTriggerModuleFactory_TypedModuleTypes(t *testing.T) {
	f := &eventbus.SubscribeTriggerModuleFactory{}
	types := f.TypedModuleTypes()
	if len(types) != 1 || types[0] != "trigger.eventbus.subscribe" {
		t.Errorf("TypedModuleTypes() = %v, want [trigger.eventbus.subscribe]", types)
	}
}

func TestSubscribeTriggerModuleFactory_CreateTypedModule_WrongType(t *testing.T) {
	f := &eventbus.SubscribeTriggerModuleFactory{}
	_, err := f.CreateTypedModule("infra.eventbus", "x", nil)
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
}

func TestSubscribeTriggerModuleFactory_CreateTypedModule_NilConfig(t *testing.T) {
	f := &eventbus.SubscribeTriggerModuleFactory{}
	// nil config → ConsumerConfig zero value → empty name → expect error
	_, err := f.CreateTypedModule("trigger.eventbus.subscribe", "trigger-factory-nil", nil)
	if err == nil {
		t.Fatal("expected error from NewSubscribeTrigger for empty name")
	}
}

// ── NewSubscribeTrigger validation ────────────────────────────────────────────

func TestNewSubscribeTrigger_ValidConfig(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	m, err := eventbus.NewSubscribeTrigger("trigger-valid", cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil module")
	}
}

func TestNewSubscribeTrigger_EmptyName(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		StreamName: "BMW_FULFILLMENT",
	}
	_, err := eventbus.NewSubscribeTrigger("trigger-empty-name", cfg, nil)
	if err == nil {
		t.Fatal("expected error for empty consumer name")
	}
}

func TestNewSubscribeTrigger_EmptyStreamName(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name: "bmw-fulfillment-handler",
	}
	_, err := eventbus.NewSubscribeTrigger("trigger-empty-stream", cfg, nil)
	if err == nil {
		t.Fatal("expected error for empty stream_name")
	}
}

// ── subscribeTrigger lifecycle (nil callback — external plugin path) ──────────

// TestSubscribeTrigger_LifecycleNilCallback verifies that the trigger module
// lifecycle (Init → Start → Stop) works cleanly when cb=nil (the external
// plugin path where the trigger fires nothing but must not panic or error).
func TestSubscribeTrigger_LifecycleNilCallback(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	m, err := eventbus.NewSubscribeTrigger("trigger-lifecycle-nil", cfg, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// Start with nil callback is a no-op (no goroutine launched).
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Stop must be idempotent and safe even when no goroutine was started.
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
