package eventbusv1_test

import (
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"google.golang.org/protobuf/proto"
)

// TestClusterConfigRoundTrip verifies that ClusterConfig survives a proto
// marshal → unmarshal cycle without data loss. This keeps the CI test job
// non-vacuous even before provider implementations exist.
func TestClusterConfigRoundTrip(t *testing.T) {
	orig := &eventbusv1.ClusterConfig{
		Provider:    "nats",
		DeployTarget: "kubernetes",
		Replicas:    3,
		Jetstream: &eventbusv1.JetStreamConfig{
			Enabled:         true,
			MaxStorageBytes: 53687091200,
		},
	}
	b, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := &eventbusv1.ClusterConfig{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Provider != orig.Provider || got.Replicas != orig.Replicas || got.DeployTarget != orig.DeployTarget {
		t.Errorf("round-trip mismatch: got %v, want %v", got, orig)
	}
	if got.Jetstream == nil || got.Jetstream.Enabled != orig.Jetstream.Enabled {
		t.Errorf("jetstream round-trip mismatch")
	}
}

// TestConsumerConfigEnums verifies that typed enum fields survive a round-trip
// and are not silently zeroed out.
func TestConsumerConfigEnums(t *testing.T) {
	orig := &eventbusv1.ConsumerConfig{
		Name:          "bmw-handler",
		StreamName:    "BMW_FULFILLMENT",
		DeliverPolicy: eventbusv1.DeliverPolicy_DELIVER_POLICY_NEW,
		AckPolicy:     eventbusv1.AckPolicy_ACK_POLICY_EXPLICIT,
		MaxDeliver:    5,
	}
	b, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := &eventbusv1.ConsumerConfig{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.DeliverPolicy != orig.DeliverPolicy {
		t.Errorf("deliver_policy: got %v, want %v", got.DeliverPolicy, orig.DeliverPolicy)
	}
	if got.AckPolicy != orig.AckPolicy {
		t.Errorf("ack_policy: got %v, want %v", got.AckPolicy, orig.AckPolicy)
	}
}

// TestStreamConfigRetentionPolicy verifies RetentionPolicy enum round-trip.
func TestStreamConfigRetentionPolicy(t *testing.T) {
	orig := &eventbusv1.StreamConfig{
		Name:            "BMW_FULFILLMENT",
		RetentionPolicy: eventbusv1.RetentionPolicy_RETENTION_POLICY_WORKQUEUE,
		NumReplicas:     3,
	}
	b, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := &eventbusv1.StreamConfig{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.RetentionPolicy != orig.RetentionPolicy {
		t.Errorf("retention_policy: got %v, want %v", got.RetentionPolicy, orig.RetentionPolicy)
	}
}
