package eventbus_test

import (
	"context"
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

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
	m, _ := eventbus.NewStreamModule("stream-stop-unreg", cfg)
	_ = m.Init()
	_ = m.Stop(context.Background())

	_, ok := eventbus.GetStream("stream-stop-unreg")
	if ok {
		t.Fatal("stream still in registry after Stop")
	}
}
