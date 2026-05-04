package steps

import (
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Compile-time assertions: each factory implements sdk.TypedStepProvider.
var (
	_ sdk.TypedStepProvider = (*sdk.TypedStepFactory[*emptypb.Empty, *eventbusv1.PublishRequest, *eventbusv1.PublishResponse])(nil)
	_ sdk.TypedStepProvider = (*sdk.TypedStepFactory[*emptypb.Empty, *eventbusv1.ConsumeRequest, *eventbusv1.ConsumeResponse])(nil)
	_ sdk.TypedStepProvider = (*sdk.TypedStepFactory[*emptypb.Empty, *eventbusv1.AckRequest, *eventbusv1.AckResponse])(nil)
)

// PublishFactory is the TypedStepProvider for step.eventbus.publish.
var PublishFactory = sdk.NewTypedStepFactory(
	"step.eventbus.publish",
	&emptypb.Empty{},
	&eventbusv1.PublishRequest{},
	PublishHandler,
)

// ConsumeFactory is the TypedStepProvider for step.eventbus.consume.
var ConsumeFactory = sdk.NewTypedStepFactory(
	"step.eventbus.consume",
	&emptypb.Empty{},
	&eventbusv1.ConsumeRequest{},
	ConsumeHandler,
)

// AckFactory is the TypedStepProvider for step.eventbus.ack.
var AckFactory = sdk.NewTypedStepFactory(
	"step.eventbus.ack",
	&emptypb.Empty{},
	&eventbusv1.AckRequest{},
	AckHandler,
)

// All returns all step TypedStepProvider factories exported by this package.
// Register each with the plugin's sdk.Server via WithTypedStepProvider.
func All() []sdk.TypedStepProvider {
	return []sdk.TypedStepProvider{
		PublishFactory,
		ConsumeFactory,
		AckFactory,
	}
}
