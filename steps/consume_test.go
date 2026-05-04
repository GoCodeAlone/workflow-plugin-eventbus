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

func TestConsumeHandler_EmptyConsumer(t *testing.T) {
	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.ConsumeRequest]{
		Config: &emptypb.Empty{},
		Input:  &eventbusv1.ConsumeRequest{},
	}
	_, err := steps.ConsumeHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for empty consumer name")
	}
}

func TestConsumeHandler_ConsumerNotFound(t *testing.T) {
	// No infra.eventbus.consumer module registered with this durable name.
	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.ConsumeRequest]{
		Config: &emptypb.Empty{},
		Input: &eventbusv1.ConsumeRequest{
			Consumer:  "nonexistent-consumer",
			BatchSize: 1,
		},
	}
	_, err := steps.ConsumeHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when consumer is not registered")
	}
}

// TestConsumeHandler_NoBusRegistered verifies the error path when a consumer
// config exists but no bus is reachable (no URI registered).
func TestConsumeHandler_NoBusRegistered(t *testing.T) {
	// Register a consumer module so the consumer lookup succeeds.
	consumerCfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	cm, err := eventbus.NewConsumerModule("consume-test-consumer", consumerCfg)
	if err != nil {
		t.Fatalf("create consumer module: %v", err)
	}
	if err := cm.Init(); err != nil {
		t.Fatalf("init consumer: %v", err)
	}
	t.Cleanup(func() { _ = cm.Stop(context.Background()) })

	// No bus cluster registered — DefaultBusConn should fail.
	req := sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.ConsumeRequest]{
		Config: &emptypb.Empty{},
		Input: &eventbusv1.ConsumeRequest{
			Consumer:  "bmw-handler",
			BatchSize: 1,
		},
	}
	_, err = steps.ConsumeHandler(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when no bus is registered")
	}
}
