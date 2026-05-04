package eventbus_test

import (
	"context"
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

// ── StreamModuleFactory (TypedModuleProvider) ─────────────────────────────────

func TestStreamModuleFactory_TypedModuleTypes(t *testing.T) {
	f := &eventbus.StreamModuleFactory{}
	types := f.TypedModuleTypes()
	if len(types) != 1 || types[0] != "infra.eventbus.stream" {
		t.Errorf("TypedModuleTypes() = %v, want [infra.eventbus.stream]", types)
	}
}

func TestStreamModuleFactory_CreateTypedModule_WrongType(t *testing.T) {
	f := &eventbus.StreamModuleFactory{}
	_, err := f.CreateTypedModule("infra.eventbus", "x", nil)
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
}

func TestStreamModuleFactory_CreateTypedModule_NilConfig(t *testing.T) {
	f := &eventbus.StreamModuleFactory{}
	// nil config → StreamConfig zero value → empty name → expect error
	_, err := f.CreateTypedModule("infra.eventbus.stream", "stream-factory-nil", nil)
	if err == nil {
		t.Fatal("expected error from NewStreamModule for empty name")
	}
}

// ── NewStreamModule validation ────────────────────────────────────────────────

func TestNewStreamModule_ValidConfig(t *testing.T) {
	cfg := &eventbusv1.StreamConfig{
		Name:     "BMW_FULFILLMENT",
		Subjects: []string{"fulfillment.>"},
	}
	m, err := eventbus.NewStreamModule("stream-valid", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil module")
	}
}

func TestNewStreamModule_EmptyName(t *testing.T) {
	cfg := &eventbusv1.StreamConfig{
		Subjects: []string{"fulfillment.>"},
	}
	_, err := eventbus.NewStreamModule("stream-empty-name", cfg)
	if err == nil {
		t.Fatal("expected error for empty stream name")
	}
}

func TestNewStreamModule_EmptySubjects(t *testing.T) {
	cfg := &eventbusv1.StreamConfig{
		Name: "BMW_FULFILLMENT",
	}
	_, err := eventbus.NewStreamModule("stream-empty-subjects", cfg)
	if err == nil {
		t.Fatal("expected error for empty subjects")
	}
}

// ── streamModule lifecycle ────────────────────────────────────────────────────

func TestStreamModule_InitRegistersConfig(t *testing.T) {
	cfg := &eventbusv1.StreamConfig{
		Name:     "BMW_FULFILLMENT",
		Subjects: []string{"fulfillment.>"},
	}
	m, err := eventbus.NewStreamModule("stream-init-reg", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	got, ok := eventbus.GetStream("stream-init-reg")
	if !ok {
		t.Fatal("stream not found in registry after Init")
	}
	if got.GetName() != "BMW_FULFILLMENT" {
		t.Errorf("name = %q, want BMW_FULFILLMENT", got.GetName())
	}
}

func TestStreamModule_StopUnregisters(t *testing.T) {
	cfg := &eventbusv1.StreamConfig{
		Name:     "BMW_FULFILLMENT",
		Subjects: []string{"fulfillment.>"},
	}
	m, err := eventbus.NewStreamModule("stream-stop-unreg", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = m.Init()
	_ = m.Stop(context.Background())

	_, ok := eventbus.GetStream("stream-stop-unreg")
	if ok {
		t.Fatal("stream still in registry after Stop")
	}
}
