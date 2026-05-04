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

func TestPublishHandler_EmptySubject(t *testing.T) {
	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.PublishRequest]{
		Config: &emptypb.Empty{},
		Input:  &eventbusv1.PublishRequest{},
	}
	_, err := steps.PublishHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for empty subject")
	}
}

func TestPublishHandler_NoBusRegistered(t *testing.T) {
	// The steps test binary starts with no modules registered.
	// This test verifies DefaultBusConn returns a descriptive error.
	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.PublishRequest]{
		Config: &emptypb.Empty{},
		Input:  &eventbusv1.PublishRequest{Subject: "test.subject", Payload: []byte("hello")},
	}
	_, err := steps.PublishHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when no bus is registered")
	}
}

// TestPublishHandler_BusRegisteredNoURI verifies that when a cluster is
// registered but no URI is set, GetOrDialNATSConn returns a descriptive error.
func TestPublishHandler_BusRegisteredNoURI(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, err := eventbus.NewClusterModule("publish-test-bus", cfg)
	if err != nil {
		t.Fatalf("create module: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.PublishRequest]{
		Config: &emptypb.Empty{},
		Input:  &eventbusv1.PublishRequest{Subject: "test.subject", Payload: []byte("hello")},
	}
	_, err = steps.PublishHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when bus has no URI")
	}
}
