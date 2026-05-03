package nats_test

import (
	"strings"
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
	"google.golang.org/protobuf/types/known/durationpb"
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

// ── ConnectionString ─────────────────────────────────────────────────────────

// TestNATSProvider_ConnectionString_ErrorsWithoutURI asserts ConnectionString
// returns an error when neither a uri nor an env-specific uri is in state.
func TestNATSProvider_ConnectionString_ErrorsWithoutURI(t *testing.T) {
	p := nats.New()
	_, err := p.ConnectionString(iac.State{Outputs: map[string]iac.Output{}}, "prod")
	if err == nil {
		t.Fatal("expected error when uri is absent from state, got nil")
	}
}

// TestNATSProvider_ConnectionString_DOAppPlatformFormat asserts ConnectionString
// returns the DO App Platform internal DNS URI for a provisioned NATS service.
func TestNATSProvider_ConnectionString_DOAppPlatformFormat(t *testing.T) {
	p := nats.New()
	state := iac.State{Outputs: map[string]iac.Output{
		"uri": {Value: "nats://nats.internal:4222", Sensitive: true},
	}}
	got, err := p.ConnectionString(state, "staging")
	if err != nil {
		t.Fatalf("ConnectionString() error: %v", err)
	}
	if got != "nats://nats.internal:4222" {
		t.Errorf("ConnectionString() = %q, want %q", got, "nats://nats.internal:4222")
	}
}

// TestNATSProvider_ConnectionString_EnvOverride asserts that an env-specific
// output (uri.<env>) takes precedence over the base "uri" key.
func TestNATSProvider_ConnectionString_EnvOverride(t *testing.T) {
	p := nats.New()
	state := iac.State{Outputs: map[string]iac.Output{
		"uri":      {Value: "nats://nats.internal:4222", Sensitive: true},
		"uri.prod": {Value: "nats://nats-prod.internal:4222", Sensitive: true},
	}}
	got, err := p.ConnectionString(state, "prod")
	if err != nil {
		t.Fatalf("ConnectionString() error: %v", err)
	}
	if got != "nats://nats-prod.internal:4222" {
		t.Errorf("ConnectionString(env=prod) = %q, want env-specific URI %q", got, "nats://nats-prod.internal:4222")
	}
}

// TestNATSProvider_ConnectionString_FallsBackToBaseURI asserts that when no
// env-specific output exists, the base "uri" output is returned.
func TestNATSProvider_ConnectionString_FallsBackToBaseURI(t *testing.T) {
	p := nats.New()
	state := iac.State{Outputs: map[string]iac.Output{
		"uri": {Value: "nats://nats.internal:4222", Sensitive: true},
	}}
	got, err := p.ConnectionString(state, "staging")
	if err != nil {
		t.Fatalf("ConnectionString() error: %v", err)
	}
	if got != "nats://nats.internal:4222" {
		t.Errorf("ConnectionString(env=staging) = %q, want base URI", got)
	}
}

// ── StreamResources ──────────────────────────────────────────────────────────

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

// TestNATSProvider_StreamResources_EmitsStreamCreate asserts that each
// StreamConfig produces one nats.stream_create resource.
func TestNATSProvider_StreamResources_EmitsStreamCreate(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{
		{Name: "BMW_FULFILLMENT", Subjects: []string{"fulfillment.>"}},
	}
	res, err := p.StreamResources(streams, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("StreamResources() returned %d resources, want 1", len(res))
	}
	if res[0].Kind != "nats.stream_create" {
		t.Errorf("resource Kind = %q, want %q", res[0].Kind, "nats.stream_create")
	}
}

// TestNATSProvider_StreamResources_ResourceName asserts the resource Name
// matches the StreamConfig name.
func TestNATSProvider_StreamResources_ResourceName(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{{Name: "BMW_FULFILLMENT", Subjects: []string{"fulfillment.>"}}}
	res, err := p.StreamResources(streams, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	if res[0].Name != "BMW_FULFILLMENT" {
		t.Errorf("resource Name = %q, want %q", res[0].Name, "BMW_FULFILLMENT")
	}
}

// TestNATSProvider_StreamResources_Subjects asserts subjects are captured in
// the resource properties.
func TestNATSProvider_StreamResources_Subjects(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{
		{Name: "S", Subjects: []string{"fulfillment.>", "order.>"}},
	}
	res, err := p.StreamResources(streams, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	subjects := res[0].Properties["subjects"]
	if !strings.Contains(subjects, "fulfillment.>") || !strings.Contains(subjects, "order.>") {
		t.Errorf("subjects property %q missing expected subjects", subjects)
	}
}

// TestNATSProvider_StreamResources_RetentionPolicy asserts the retention_policy
// property is emitted as the NATS-native lowercase value, not the proto enum name.
func TestNATSProvider_StreamResources_RetentionPolicy(t *testing.T) {
	cases := []struct {
		rp   eventbusv1.RetentionPolicy
		want string
	}{
		{eventbusv1.RetentionPolicy_RETENTION_POLICY_WORKQUEUE, "workqueue"},
		{eventbusv1.RetentionPolicy_RETENTION_POLICY_INTEREST, "interest"},
		{eventbusv1.RetentionPolicy_RETENTION_POLICY_LIMITS, "limits"},
		{eventbusv1.RetentionPolicy_RETENTION_POLICY_UNSPECIFIED, "limits"}, // default → limits
	}
	p := nats.New()
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			streams := []*eventbusv1.StreamConfig{
				{Name: "S", Subjects: []string{"s.>"}, RetentionPolicy: tc.rp},
			}
			res, err := p.StreamResources(streams, iac.State{})
			if err != nil {
				t.Fatalf("StreamResources() error: %v", err)
			}
			if got := res[0].Properties["retention_policy"]; got != tc.want {
				t.Errorf("retention_policy = %q, want NATS-native %q", got, tc.want)
			}
		})
	}
}

// TestNATSProvider_StreamResources_EmptyNameErrors asserts StreamResources returns
// an error when a StreamConfig has an empty name.
func TestNATSProvider_StreamResources_EmptyNameErrors(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{{Name: "", Subjects: []string{"s.>"}}}
	_, err := p.StreamResources(streams, iac.State{})
	if err == nil {
		t.Fatal("StreamResources() with empty name: expected error, got nil")
	}
}

// TestNATSProvider_StreamResources_EmptySubjectsErrors asserts StreamResources
// returns an error when a StreamConfig has no subjects.
func TestNATSProvider_StreamResources_EmptySubjectsErrors(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{{Name: "S", Subjects: nil}}
	_, err := p.StreamResources(streams, iac.State{})
	if err == nil {
		t.Fatal("StreamResources() with nil subjects: expected error, got nil")
	}
}

// TestNATSProvider_StreamResources_DefaultNumReplicas asserts that an unset
// NumReplicas (zero value) is emitted as "1", not "0".
func TestNATSProvider_StreamResources_DefaultNumReplicas(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{{Name: "S", Subjects: []string{"s.>"}}}
	res, err := p.StreamResources(streams, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	if got := res[0].Properties["num_replicas"]; got != "1" {
		t.Errorf("num_replicas = %q, want %q (default)", got, "1")
	}
}

// TestNATSProvider_StreamResources_MultipleStreams asserts one resource is
// emitted per StreamConfig.
func TestNATSProvider_StreamResources_MultipleStreams(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{
		{Name: "A", Subjects: []string{"a.>"}},
		{Name: "B", Subjects: []string{"b.>"}},
		{Name: "C", Subjects: []string{"c.>"}},
	}
	res, err := p.StreamResources(streams, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	if len(res) != 3 {
		t.Errorf("StreamResources() returned %d resources, want 3", len(res))
	}
}

// TestNATSProvider_StreamResources_ServerURIFromState asserts that when state
// contains a "uri" output, each stream resource carries a server_uri property.
func TestNATSProvider_StreamResources_ServerURIFromState(t *testing.T) {
	p := nats.New()
	state := iac.State{Outputs: map[string]iac.Output{
		"uri": {Value: "nats://nats.internal:4222", Sensitive: true},
	}}
	streams := []*eventbusv1.StreamConfig{{Name: "S", Subjects: []string{"s.>"}}}
	res, err := p.StreamResources(streams, state)
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	if res[0].Properties["server_uri"] != "nats://nats.internal:4222" {
		t.Errorf("server_uri = %q, want %q", res[0].Properties["server_uri"], "nats://nats.internal:4222")
	}
}

// TestNATSProvider_StreamResources_MaxAge asserts max_age duration is stored
// when set on the StreamConfig.
func TestNATSProvider_StreamResources_MaxAge(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{
		{
			Name:     "S",
			Subjects: []string{"s.>"},
			MaxAge:   durationpb.New(168 * 60 * 60 * 1e9), // 168h in nanoseconds
		},
	}
	res, err := p.StreamResources(streams, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	if res[0].Properties["max_age"] == "" {
		t.Error("max_age property is empty when MaxAge is set")
	}
}

// TestNATSProvider_StreamResources_NilSkipped asserts nil entries in the
// stream slice are skipped without error.
func TestNATSProvider_StreamResources_NilSkipped(t *testing.T) {
	p := nats.New()
	streams := []*eventbusv1.StreamConfig{
		{Name: "A", Subjects: []string{"a.>"}},
		nil,
		{Name: "B", Subjects: []string{"b.>"}},
	}
	res, err := p.StreamResources(streams, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources() error: %v", err)
	}
	if len(res) != 2 {
		t.Errorf("StreamResources() returned %d resources, want 2 (nil skipped)", len(res))
	}
}

// ── Probe ────────────────────────────────────────────────────────────────────

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
