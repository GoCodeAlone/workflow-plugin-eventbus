package steps

import (
	"context"
	"fmt"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

// AckHandler implements step.eventbus.ack. It publishes an empty message to the
// JetStream reply subject (ack_token) supplied by step.eventbus.consume, which
// causes the broker to mark the message as acknowledged.
func AckHandler(
	ctx context.Context,
	req sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.AckRequest],
) (*sdk.TypedStepResult[*eventbusv1.AckResponse], error) {
	if req.Input.GetAckToken() == "" {
		return nil, fmt.Errorf("step.eventbus.ack: ack_token is required")
	}

	nc, err := eventbus.DefaultBusConn()
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.ack: get bus connection: %w", err)
	}

	if err := nc.Publish(req.Input.GetAckToken(), nil); err != nil {
		return nil, fmt.Errorf("step.eventbus.ack: publish ack: %w", err)
	}

	return &sdk.TypedStepResult[*eventbusv1.AckResponse]{
		Output: &eventbusv1.AckResponse{},
	}, nil
}
