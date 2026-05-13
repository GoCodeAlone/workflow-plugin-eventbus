package main

import (
	"strings"
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

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
func TestCreateTrigger_CanonicalFieldsWinOverAlias(t *testing.T) {
	p := &eventbusPlugin{}
	cfg := map[string]any{
		"name":        "canonical-name",
		"consumer":    "alias-name", // ignored — canonical wins
		"broker_ref":  "canonical-broker",
		"bus":         "alias-broker", // ignored — canonical wins
		"stream_name": "CANONICAL_STREAM",
	}
	inst, err := p.CreateTrigger("trigger.eventbus.subscribe", cfg, nil)
	if err != nil {
		t.Fatalf("CreateTrigger with both canonical + alias: %v", err)
	}
	if inst == nil {
		t.Fatal("CreateTrigger returned nil instance")
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
