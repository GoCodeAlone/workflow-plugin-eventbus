// Package providers_test contains conformance tests that exercise the complete
// Provider interface lifecycle from provision through health probe.
//
// NATS × DigitalOcean App Platform tests are gated behind INTEGRATION_NATS_DO=1
// and skipped cleanly when the variable is absent — no panic, no setup overhead.
package providers_test

import (
	"os"
	"strings"
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	natsprovider "github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
	"google.golang.org/protobuf/types/known/durationpb"
)

// integrationKey gates NATS × DigitalOcean App Platform conformance tests.
const integrationKey = "INTEGRATION_NATS_DO"

// skipUnlessNATSDO skips the calling test when INTEGRATION_NATS_DO is not set.
func skipUnlessNATSDO(t *testing.T) {
	t.Helper()
	if os.Getenv(integrationKey) == "" {
		t.Skipf("set %s=1 to run NATS × DigitalOcean App Platform conformance tests", integrationKey)
	}
}

// provisionedState simulates IaC state after a NATS cluster has been provisioned
// on DigitalOcean App Platform (the "uri" output is populated by the realizer).
var provisionedState = iac.State{
	Outputs: map[string]iac.Output{
		"uri": {Value: "nats://nats.internal:4222", Sensitive: true},
	},
}

// bmwClusterCfg returns the ClusterConfig for the BMW pilot NATS cluster.
func bmwClusterCfg() *eventbusv1.ClusterConfig {
	return &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 3,
		Jetstream: &eventbusv1.JetStreamConfig{
			Enabled:         true,
			MaxStorageBytes: 10 * 1024 * 1024 * 1024, // 10 GiB
		},
	}
}

// bmwStreams returns representative BMW pilot JetStream stream configs.
func bmwStreams() []*eventbusv1.StreamConfig {
	return []*eventbusv1.StreamConfig{
		{
			Name:            "BMW_FULFILLMENT",
			Subjects:        []string{"fulfillment.>"},
			RetentionPolicy: eventbusv1.RetentionPolicy_RETENTION_POLICY_LIMITS,
			NumReplicas:     3,
			MaxBytes:        1 << 30, // 1 GiB
		},
		{
			Name:            "BMW_ORDERS",
			Subjects:        []string{"orders.created", "orders.updated"},
			RetentionPolicy: eventbusv1.RetentionPolicy_RETENTION_POLICY_WORKQUEUE,
			NumReplicas:     3,
			MaxAge:          durationpb.New(7 * 24 * 60 * 60 * 1e9), // 7 days in nanoseconds
		},
	}
}

// TestNATSConformance_DOApp exercises the complete Provider lifecycle for
// NATS × DigitalOcean App Platform across four lifecycle steps:
//
//  1. provision  — Resources() emits valid IaC cluster declarations
//  2. stream     — StreamResources() emits valid nats.stream_create resources
//  3. connect    — ConnectionString() derives the correct broker URI from state
//  4. probe      — Probe() returns a HealthCheck without panicking
//
// All subtests require INTEGRATION_NATS_DO=1.
func TestNATSConformance_DOApp(t *testing.T) {
	skipUnlessNATSDO(t)

	// p is declared as the interface — conformance tests must not rely on
	// internals of the concrete *provider type.
	var p providers.Provider = natsprovider.New()

	// ── 1. provision ─────────────────────────────────────────────────────────
	t.Run("provision", func(t *testing.T) {
		resources, err := p.Resources(bmwClusterCfg(), providers.TargetDigitalOceanApp)
		if err != nil {
			t.Fatalf("Resources() error: %v", err)
		}
		if len(resources) < 2 {
			t.Fatalf("Resources() returned %d resources, want ≥2 (container_service + storage)", len(resources))
		}

		// ── container service ────────────────────────────────────────────────
		cs := resources[0]
		if cs.Kind != "infra.container_service" {
			t.Errorf("resources[0].Kind = %q, want %q", cs.Kind, "infra.container_service")
		}
		if !strings.HasPrefix(cs.Properties["image"], "docker.io/library/nats") {
			t.Errorf("image = %q, want prefix %q", cs.Properties["image"], "docker.io/library/nats")
		}
		if !strings.Contains(cs.Properties["internal_ports"], "4222") {
			t.Errorf("internal_ports = %q, want client port 4222", cs.Properties["internal_ports"])
		}
		if cs.Properties["storage_ref"] == "" {
			t.Error("storage_ref is empty; JetStream requires a volume reference on the container resource")
		}
		if cs.Labels["deploy_target"] != string(providers.TargetDigitalOceanApp) {
			t.Errorf("deploy_target label = %q, want %q",
				cs.Labels["deploy_target"], string(providers.TargetDigitalOceanApp))
		}

		// ── jetstream storage volume ─────────────────────────────────────────
		st := resources[1]
		if st.Kind != "infra.storage" {
			t.Errorf("resources[1].Kind = %q, want %q", st.Kind, "infra.storage")
		}
		if st.Properties["storage_size_bytes"] == "" {
			t.Error("infra.storage: storage_size_bytes property is empty")
		}
	})

	// ── 2. stream ─────────────────────────────────────────────────────────────
	t.Run("stream", func(t *testing.T) {
		resources, err := p.StreamResources(bmwStreams(), provisionedState)
		if err != nil {
			t.Fatalf("StreamResources() error: %v", err)
		}
		if len(resources) != 2 {
			t.Fatalf("StreamResources() returned %d resources, want 2", len(resources))
		}

		for _, r := range resources {
			if r.Kind != "nats.stream_create" {
				t.Errorf("stream %q Kind = %q, want %q", r.Name, r.Kind, "nats.stream_create")
			}
			if r.Properties["server_uri"] != "nats://nats.internal:4222" {
				t.Errorf("stream %q server_uri = %q, want %q",
					r.Name, r.Properties["server_uri"], "nats://nats.internal:4222")
			}
			rp := r.Properties["retention_policy"]
			switch rp {
			case "limits", "workqueue", "interest":
				// valid NATS-native values
			default:
				t.Errorf("stream %q retention_policy = %q, want NATS-native value (limits/workqueue/interest)", r.Name, rp)
			}
			nr := r.Properties["num_replicas"]
			if nr == "" || nr == "0" {
				t.Errorf("stream %q num_replicas = %q, want ≥1", r.Name, nr)
			}
		}

		// BMW_ORDERS has max_age=7d — verify it is emitted.
		var ordersResource *iac.Resource
		for i := range resources {
			if resources[i].Name == "BMW_ORDERS" {
				ordersResource = &resources[i]
				break
			}
		}
		if ordersResource == nil {
			t.Fatal("BMW_ORDERS resource not found in StreamResources output")
		}
		if ordersResource.Properties["max_age"] == "" {
			t.Error("BMW_ORDERS: max_age property is empty when MaxAge=7d is configured")
		}
	})

	// ── 3. connect ────────────────────────────────────────────────────────────
	t.Run("connect", func(t *testing.T) {
		uri, err := p.ConnectionString(provisionedState, "")
		if err != nil {
			t.Fatalf("ConnectionString() error: %v", err)
		}
		if !strings.HasPrefix(uri, "nats://") {
			t.Errorf("ConnectionString() = %q, want nats:// scheme", uri)
		}
		if !strings.Contains(uri, ":4222") {
			t.Errorf("ConnectionString() = %q, want NATS client port 4222", uri)
		}
	})

	// ── 3a. connect / env-override ────────────────────────────────────────────
	t.Run("connect/env-override", func(t *testing.T) {
		envState := iac.State{
			Outputs: map[string]iac.Output{
				"uri":      {Value: "nats://nats.internal:4222", Sensitive: true},
				"uri.prod": {Value: "nats://nats-prod.internal:4222", Sensitive: true},
			},
		}
		uri, err := p.ConnectionString(envState, "prod")
		if err != nil {
			t.Fatalf("ConnectionString(env=prod) error: %v", err)
		}
		if uri != "nats://nats-prod.internal:4222" {
			t.Errorf("ConnectionString(env=prod) = %q, want env-specific URI %q",
				uri, "nats://nats-prod.internal:4222")
		}
	})

	// ── 3b. connect / missing state ───────────────────────────────────────────
	t.Run("connect/missing-state", func(t *testing.T) {
		_, err := p.ConnectionString(iac.State{}, "")
		if err == nil {
			t.Error("ConnectionString() with empty state: expected error, got nil")
		}
	})

	// ── 4. probe ──────────────────────────────────────────────────────────────
	t.Run("probe", func(t *testing.T) {
		const uri = "nats://nats.internal:4222"
		hc := p.Probe(uri)

		// URI must be echoed back regardless of reachability.
		if hc.URI != uri {
			t.Errorf("Probe().URI = %q, want %q", hc.URI, uri)
		}
		// Status must be a recognised value.
		switch hc.Status {
		case providers.HealthStatusHealthy, providers.HealthStatusDegraded, providers.HealthStatusUnreachable:
			// all valid
		default:
			t.Errorf("Probe().Status = %q, want one of healthy/degraded/unreachable", hc.Status)
		}
	})

	// ── 4a. probe / empty URI ─────────────────────────────────────────────────
	t.Run("probe/empty-uri", func(t *testing.T) {
		hc := p.Probe("")
		if hc.Status != providers.HealthStatusUnreachable {
			t.Errorf("Probe(\"\").Status = %q, want %q", hc.Status, providers.HealthStatusUnreachable)
		}
		if hc.Err == nil {
			t.Error("Probe(\"\").Err should be non-nil for empty URI")
		}
	})
}
