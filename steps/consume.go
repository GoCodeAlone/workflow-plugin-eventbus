package steps

import (
	"context"
	"fmt"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ConsumeHandler implements step.eventbus.consume. It resolves the durable
// consumer's stream name from the in-process registry (populated by the
// eventbus.consumer module on Init), then dispatches the bounded-batch fetch
// through the broker named by ConsumeRequest.broker_ref via
// RuntimeBroker.Consume.
//
// Returned messages include ack_token, which the caller passes to
// step.eventbus.ack to acknowledge each message individually.
//
// Legacy fallback: when broker_ref is empty AND exactly one broker module is
// registered, that broker is used automatically (see LookupRuntimeWithFallback).
func ConsumeHandler(
	ctx context.Context,
	req sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.ConsumeRequest],
) (*sdk.TypedStepResult[*eventbusv1.ConsumeResponse], error) {
	input := req.Input
	if input.GetConsumer() == "" {
		return nil, fmt.Errorf("step.eventbus.consume: consumer is required")
	}

	// The ConsumeRequest proto carries the durable consumer name but not the
	// stream it lives on. We resolve stream_name from the registered
	// ConsumerConfig (Init-time registration via eventbus.consumer module).
	cfg, ok := eventbus.GetConsumerByName(input.GetConsumer())
	if !ok {
		return nil, fmt.Errorf(
			"step.eventbus.consume: consumer %q not registered; add an eventbus.consumer module with name=%q",
			input.GetConsumer(), input.GetConsumer(),
		)
	}
	streamName := cfg.GetStreamName()
	if streamName == "" {
		return nil, fmt.Errorf("step.eventbus.consume: consumer %q has empty stream_name in its registered config", input.GetConsumer())
	}

	rb, conn, err := eventbus.LookupRuntimeWithFallback(input.GetBrokerRef())
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.consume: %w", err)
	}

	resp, err := rb.Consume(ctx, conn, streamName, input)
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.consume: %w", err)
	}

	return &sdk.TypedStepResult[*eventbusv1.ConsumeResponse]{Output: resp}, nil
}
