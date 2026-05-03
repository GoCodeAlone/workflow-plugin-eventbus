package kafka_test

import (
	"strings"
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/kafka"
)

// TestKafkaProvider_Name asserts the provider reports the correct identifier.
func TestKafkaProvider_Name(t *testing.T) {
	p := kafka.New()
	if p.Name() != "kafka" {
		t.Errorf("Name() = %q, want %q", p.Name(), "kafka")
	}
}

// TestKafkaProvider_Stub_ErrorsOnResources asserts that Resources returns a
// "not implemented" error — the kafka provider is a registry stub for the pilot.
func TestKafkaProvider_Stub_ErrorsOnResources(t *testing.T) {
	p := kafka.New()
	_, err := p.Resources(&eventbusv1.ClusterConfig{}, providers.TargetDigitalOceanManagedKafka)
	if err == nil {
		t.Fatal("expected stub error from Resources, got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error does not contain 'not implemented': %v", err)
	}
}

// TestKafkaProvider_Stub_ErrorsOnConnectionString asserts ConnectionString also stubs.
func TestKafkaProvider_Stub_ErrorsOnConnectionString(t *testing.T) {
	p := kafka.New()
	_, err := p.ConnectionString(iac.State{}, "prod")
	if err == nil {
		t.Fatal("expected stub error from ConnectionString, got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error does not contain 'not implemented': %v", err)
	}
}

// TestKafkaProvider_Stub_ErrorsOnStreamResources asserts StreamResources also stubs.
func TestKafkaProvider_Stub_ErrorsOnStreamResources(t *testing.T) {
	p := kafka.New()
	_, err := p.StreamResources(nil, iac.State{})
	if err == nil {
		t.Fatal("expected stub error from StreamResources, got nil")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error does not contain 'not implemented': %v", err)
	}
}

// TestKafkaProvider_Stub_ProbeReturnsUnreachable asserts Probe returns unreachable.
func TestKafkaProvider_Stub_ProbeReturnsUnreachable(t *testing.T) {
	p := kafka.New()
	hc := p.Probe("kafka://localhost:9092")
	if hc.Status != providers.HealthStatusUnreachable {
		t.Errorf("Probe() status = %q, want %q", hc.Status, providers.HealthStatusUnreachable)
	}
	if hc.Err == nil {
		t.Error("Probe() Err should be non-nil for stub")
	}
}
