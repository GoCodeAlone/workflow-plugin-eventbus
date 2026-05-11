// Package steps provides typed step handlers for the workflow-plugin-eventbus
// plugin: publish, consume, and ack operations dispatched through the
// providers.RuntimeBroker abstraction.
package steps

import (
	"context"
	"fmt"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PublishHandler implements step.eventbus.publish. It dispatches the publish
// through the broker named by PublishRequest.broker_ref via LookupRuntime,
// then calls RuntimeBroker.Publish on the registered runtime/connection pair.
//
// Legacy fallback: when broker_ref is empty AND exactly one broker module is
// registered, that broker is used automatically. The fallback exists so
// configs predating the broker_ref proto field continue to work in single-bus
// deployments; multi-bus workflows must set broker_ref explicitly.
//
// The step config is empty (no per-step config required). All parameters are
// supplied via the typed input message.
func PublishHandler(
	ctx context.Context,
	req sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.PublishRequest],
) (*sdk.TypedStepResult[*eventbusv1.PublishResponse], error) {
	input := req.Input
	if input.GetSubject() == "" {
		return nil, fmt.Errorf("step.eventbus.publish: subject is required")
	}

	rb, conn, err := eventbus.LookupRuntimeWithFallback(input.GetBrokerRef())
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.publish: %w", err)
	}

	resp, err := rb.Publish(ctx, conn, input)
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.publish: %w", err)
	}

	return &sdk.TypedStepResult[*eventbusv1.PublishResponse]{Output: resp}, nil
}
