// Package providers_test contains conformance tests that exercise the complete
// Provider interface lifecycle from provision through teardown.
//
// NATS × DigitalOcean App Platform tests are gated behind INTEGRATION_NATS_DO=1
// and skipped cleanly when the variable is absent — no panic, no setup overhead.
// The runtime phases (publish → consume → ack → drain → teardown) require a
// reachable NATS server; set NATS_URL to override the default localhost:4222.
package providers_test

import (
	"os"
	"strings"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

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

// natsServerURI returns the NATS server URI for integration tests.
// NATS_URL env var overrides the default; falls back to localhost:4222.
func natsServerURI() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return natsclient.DefaultURL // "nats://127.0.0.1:4222"
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

// TestNATSConformance_DOApp exercises the complete NATS × DigitalOcean App
// Platform Provider lifecycle across nine phases:
//
//  1. provision  — Resources() emits valid IaC cluster declarations
//  2. stream     — StreamResources() emits valid nats.stream_create resources
//  3. connect    — ConnectionString() derives the correct broker URI from state
//  4. probe      — Probe() returns a HealthCheck without panicking
//  5. publish    — connect to NATS server, create JetStream stream, publish message
//  6. consume    — subscribe and fetch the published message
//  7. ack        — acknowledge the fetched message
//  8. drain      — drain the subscription
//  9. teardown   — delete the stream, assert it is gone
//
// All subtests require INTEGRATION_NATS_DO=1.
// Runtime phases (5–9) additionally require a reachable NATS server (NATS_URL
// or localhost:4222 by default).
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

		// Locate resources by Kind rather than index so the test is resilient to
		// ordering changes in resourcesForDOApp.
		var cs, st *iac.Resource
		for i := range resources {
			switch resources[i].Kind {
			case "infra.container_service":
				cs = &resources[i]
			case "infra.storage":
				st = &resources[i]
			}
		}
		if cs == nil {
			t.Fatal("provision: no infra.container_service resource emitted")
		}
		if st == nil {
			t.Fatal("provision: no infra.storage resource emitted (JetStream requires it)")
		}

		// ── container service ────────────────────────────────────────────────
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

	// ── Runtime phases 5–9: publish → consume → ack → drain → teardown ───────
	//
	// These phases connect to a live NATS server (NATS_URL or localhost:4222)
	// and verify end-to-end message flow through a JetStream stream.
	// Shared mutable state is captured in the parent scope; each subtest nil-guards
	// its dependencies so a partial failure produces a SKIP rather than a panic.
	// nc is closed via the parent test's Cleanup so it outlives all subtests.

	const (
		conformanceStream   = "CONFORMANCE_TEST"
		conformanceSubject  = "conformance.test"
		conformanceConsumer = "conformance-consumer"
		conformancePayload  = "conformance-check"
	)

	var (
		nc         *natsclient.Conn
		js         natsclient.JetStreamContext
		sub        *natsclient.Subscription
		fetchedMsg *natsclient.Msg
	)

	// Register nc close on the parent test — nc must outlive all subtests.
	t.Cleanup(func() {
		if nc != nil {
			nc.Close()
		}
	})

	// ── 5. publish ────────────────────────────────────────────────────────────
	t.Run("publish", func(t *testing.T) {
		var err error
		nc, err = natsclient.Connect(natsServerURI())
		if err != nil {
			t.Fatalf("nats.Connect(%q) error: %v — ensure a NATS server is running (NATS_URL overrides default)", natsServerURI(), err)
		}

		js, err = nc.JetStream()
		if err != nil {
			t.Fatalf("nc.JetStream() error: %v", err)
		}

		// Clean up any leftover stream from a previous interrupted run.
		_ = js.DeleteStream(conformanceStream)

		_, err = js.AddStream(&natsclient.StreamConfig{
			Name:      conformanceStream,
			Subjects:  []string{conformanceSubject},
			Retention: natsclient.LimitsPolicy,
			Replicas:  1,
		})
		if err != nil {
			t.Fatalf("js.AddStream(%q) error: %v", conformanceStream, err)
		}

		pubAck, err := js.Publish(conformanceSubject, []byte(conformancePayload))
		if err != nil {
			t.Fatalf("js.Publish(%q) error: %v", conformanceSubject, err)
		}
		if pubAck.Stream != conformanceStream {
			t.Errorf("PubAck.Stream = %q, want %q", pubAck.Stream, conformanceStream)
		}
		if pubAck.Sequence != 1 {
			t.Errorf("PubAck.Sequence = %d, want 1 (first message in stream)", pubAck.Sequence)
		}
	})

	// ── 6. consume ────────────────────────────────────────────────────────────
	t.Run("consume", func(t *testing.T) {
		if js == nil {
			t.Skip("skipping: publish phase did not establish a JetStream context")
		}
		var err error
		sub, err = js.SubscribeSync(conformanceSubject, natsclient.Durable(conformanceConsumer))
		if err != nil {
			t.Fatalf("js.SubscribeSync(%q) error: %v", conformanceSubject, err)
		}
		fetchedMsg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("sub.NextMsg() error: %v", err)
		}
		if string(fetchedMsg.Data) != conformancePayload {
			t.Errorf("message payload = %q, want %q", string(fetchedMsg.Data), conformancePayload)
		}
	})

	// ── 7. ack ────────────────────────────────────────────────────────────────
	t.Run("ack", func(t *testing.T) {
		if fetchedMsg == nil {
			t.Skip("skipping: consume phase did not produce a message")
		}
		if err := fetchedMsg.Ack(); err != nil {
			t.Fatalf("msg.Ack() error: %v", err)
		}
	})

	// ── 8. drain ──────────────────────────────────────────────────────────────
	t.Run("drain", func(t *testing.T) {
		if sub == nil {
			t.Skip("skipping: consume phase did not produce a subscription")
		}
		if err := sub.Drain(); err != nil {
			t.Fatalf("sub.Drain() error: %v", err)
		}
	})

	// ── 9. teardown ───────────────────────────────────────────────────────────
	t.Run("teardown", func(t *testing.T) {
		if js == nil {
			t.Skip("skipping: publish phase did not establish a JetStream context")
		}
		if err := js.DeleteStream(conformanceStream); err != nil {
			t.Fatalf("js.DeleteStream(%q) error: %v", conformanceStream, err)
		}
		// Confirm the stream is gone — StreamInfo should return an error.
		_, err := js.StreamInfo(conformanceStream)
		if err == nil {
			t.Errorf("js.StreamInfo(%q) succeeded after deletion; stream was not torn down", conformanceStream)
		}
	})
}

// TestNATSConformance_StubTargets asserts that every not-yet-activated deploy
// target (ECS, EKS, Kubernetes, SelfHosted) returns a non-nil error from
// Resources() with a message that mentions "not implemented". No infrastructure
// is required — this test always runs.
func TestNATSConformance_StubTargets(t *testing.T) {
	var p providers.Provider = natsprovider.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}

	cases := []struct {
		target  providers.DeployTarget
		mention string // substring expected in the error message
	}{
		{providers.TargetAWSECS, "aws.ecs"},
		{providers.TargetAWSEKS, "aws.eks"},
		{providers.TargetKubernetes, "kubernetes"},
		{providers.TargetSelfHosted, "self_hosted"},
	}

	for _, tc := range cases {
		t.Run(string(tc.target), func(t *testing.T) {
			res, err := p.Resources(cfg, tc.target)
			if err == nil {
				t.Fatalf("Resources(cfg, %q) returned nil error, want stub error", tc.target)
			}
			if res != nil {
				t.Errorf("Resources(cfg, %q) returned non-nil resources with error: %v", tc.target, res)
			}
			if !strings.Contains(err.Error(), "not implemented") {
				t.Errorf("error for %q does not contain 'not implemented': %v", tc.target, err)
			}
			if !strings.Contains(err.Error(), tc.mention) {
				t.Errorf("error for %q does not mention %q: %v", tc.target, tc.mention, err)
			}
		})
	}
}
