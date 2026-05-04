package steps_test

import (
	"context"
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/steps"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestAckHandler_EmptyToken(t *testing.T) {
	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.AckRequest]{
		Config: &emptypb.Empty{},
		Input:  &eventbusv1.AckRequest{},
	}
	_, err := steps.AckHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for empty ack_token")
	}
}

func TestAckHandler_NoBusRegistered(t *testing.T) {
	// The steps test binary has no bus registered in this scope; DefaultBusConn
	// should return a descriptive error before attempting to publish the ack.
	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.AckRequest]{
		Config: &emptypb.Empty{},
		Input:  &eventbusv1.AckRequest{AckToken: "_INBOX.sometoken"},
	}
	_, err := steps.AckHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when no bus is registered")
	}
}

// TestAckHandler_BusRegisteredNoURI verifies that when a cluster is registered
// but has no URI, GetOrDialNATSConn returns a descriptive error.
func TestAckHandler_BusRegisteredNoURI(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, err := eventbus.NewClusterModule("ack-test-bus", cfg)
	if err != nil {
		t.Fatalf("create module: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.AckRequest]{
		Config: &emptypb.Empty{},
		Input:  &eventbusv1.AckRequest{AckToken: "_INBOX.sometoken"},
	}
	_, err = steps.AckHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when bus has no URI")
	}
}
