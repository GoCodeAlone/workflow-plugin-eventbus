package steps

import (
	"context"
	"fmt"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

// AckHandler implements step.eventbus.ack. It dispatches the ack through the
// broker named by AckRequest.broker_ref via RuntimeBroker.Ack on the
// registered runtime/connection pair.
//
// Legacy fallback: when broker_ref is empty AND exactly one broker module is
// registered, that broker is used automatically (see LookupRuntimeWithFallback).
func AckHandler(
	ctx context.Context,
	req sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.AckRequest],
) (*sdk.TypedStepResult[*eventbusv1.AckResponse], error) {
	if req.Input.GetAckToken() == "" {
		return nil, fmt.Errorf("step.eventbus.ack: ack_token is required")
	}

	rb, conn, err := eventbus.LookupRuntimeWithFallback(req.Input.GetBrokerRef())
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.ack: %w", err)
	}

	if err := rb.Ack(ctx, conn, req.Input.GetAckToken()); err != nil {
		return nil, fmt.Errorf("step.eventbus.ack: %w", err)
	}

	return &sdk.TypedStepResult[*eventbusv1.AckResponse]{
		Output: &eventbusv1.AckResponse{},
	}, nil
}
