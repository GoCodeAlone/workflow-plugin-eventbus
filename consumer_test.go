package eventbus_test

import (
	"context"
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

// ── ConsumerModuleFactory (TypedModuleProvider) ───────────────────────────────

func TestConsumerModuleFactory_TypedModuleTypes(t *testing.T) {
	f := &eventbus.ConsumerModuleFactory{}
	types := f.TypedModuleTypes()
	if len(types) != 1 || types[0] != "eventbus.consumer" {
		t.Errorf("TypedModuleTypes() = %v, want [eventbus.consumer]", types)
	}
}

func TestConsumerModuleFactory_CreateTypedModule_WrongType(t *testing.T) {
	f := &eventbus.ConsumerModuleFactory{}
	_, err := f.CreateTypedModule("eventbus.broker", "x", nil)
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
}

func TestConsumerModuleFactory_CreateTypedModule_NilConfig(t *testing.T) {
	f := &eventbus.ConsumerModuleFactory{}
	// nil config → ConsumerConfig zero value → empty name → expect error
	_, err := f.CreateTypedModule("eventbus.consumer", "consumer-factory-nil", nil)
	if err == nil {
		t.Fatal("expected error from NewConsumerModule for empty name")
	}
}

// ── NewConsumerModule validation ──────────────────────────────────────────────

func TestNewConsumerModule_ValidConfig(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	m, err := eventbus.NewConsumerModule("consumer-valid", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil module")
	}
}

func TestNewConsumerModule_EmptyName(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		StreamName: "BMW_FULFILLMENT",
	}
	_, err := eventbus.NewConsumerModule("consumer-empty-name", cfg)
	if err == nil {
		t.Fatal("expected error for empty consumer name")
	}
}

func TestNewConsumerModule_EmptyStreamName(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name: "bmw-fulfillment-handler",
	}
	_, err := eventbus.NewConsumerModule("consumer-empty-stream", cfg)
	if err == nil {
		t.Fatal("expected error for empty stream_name")
	}
}

// ── consumerModule lifecycle ──────────────────────────────────────────────────

func TestConsumerModule_InitRegistersConfig(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	m, err := eventbus.NewConsumerModule("consumer-init-reg", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	got, ok := eventbus.GetConsumer("consumer-init-reg")
	if !ok {
		t.Fatal("consumer not found in registry after Init")
	}
	if got.GetName() != "bmw-fulfillment-handler" {
		t.Errorf("name = %q, want bmw-fulfillment-handler", got.GetName())
	}
	if got.GetStreamName() != "BMW_FULFILLMENT" {
		t.Errorf("stream_name = %q, want BMW_FULFILLMENT", got.GetStreamName())
	}
}

func TestConsumerModule_StopUnregisters(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	m, err := eventbus.NewConsumerModule("consumer-stop-unreg", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = m.Init()
	_ = m.Stop(context.Background())

	_, ok := eventbus.GetConsumer("consumer-stop-unreg")
	if ok {
		t.Fatal("consumer still in registry after Stop")
	}
}
