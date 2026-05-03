package nats_test

import (
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
)

// TestNATSProvider_Name asserts the provider reports the correct identifier.
func TestNATSProvider_Name(t *testing.T) {
	p := nats.New()
	if got := p.Name(); got != "nats" {
		t.Errorf("Name() = %q, want %q", got, "nats")
	}
}

// TestNATSProvider_UnsupportedTarget asserts that Resources returns an error
// for a target not supported by the NATS provider.
func TestNATSProvider_UnsupportedTarget(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(&eventbusv1.ClusterConfig{Version: "2.10"}, providers.TargetAWSKinesis)
	if err == nil {
		t.Fatal("expected error for unsupported target aws.kinesis, got nil")
	}
}

// TestNATSProvider_ConnectionString_ErrorsWithoutURI asserts ConnectionString
// returns an error when the state does not contain a uri output.
func TestNATSProvider_ConnectionString_ErrorsWithoutURI(t *testing.T) {
	p := nats.New()
	_, err := p.ConnectionString(iac.State{Outputs: map[string]iac.Output{}}, "prod")
	if err == nil {
		t.Fatal("expected error when uri is absent from state, got nil")
	}
}

// TestNATSProvider_StreamResources_NilStreamList asserts StreamResources with
// a nil slice returns an empty list without error.
func TestNATSProvider_StreamResources_NilStreamList(t *testing.T) {
	p := nats.New()
	res, err := p.StreamResources(nil, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources(nil) error: %v", err)
	}
	if len(res) != 0 {
		t.Errorf("StreamResources(nil) returned %d resources, want 0", len(res))
	}
}

// TestNATSProvider_Probe_UnreachableOnEmptyURI asserts Probe returns
// HealthStatusUnreachable for an empty URI (not a real broker).
func TestNATSProvider_Probe_UnreachableOnEmptyURI(t *testing.T) {
	p := nats.New()
	hc := p.Probe("")
	if hc.Status != providers.HealthStatusUnreachable {
		t.Errorf("Probe() status = %q, want %q", hc.Status, providers.HealthStatusUnreachable)
	}
	if hc.Err == nil {
		t.Error("Probe() Err should be non-nil for empty URI")
	}
}
