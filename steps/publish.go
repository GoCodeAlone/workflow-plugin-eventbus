// Package steps provides typed step handlers for the workflow-plugin-eventbus
// plugin: publish, consume, and ack operations over NATS JetStream.
package steps

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PublishHandler implements step.eventbus.publish. It publishes a single
// message to the NATS JetStream subject specified in PublishRequest.subject and
// returns the broker-assigned sequence number and acknowledgement timestamp.
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

	nc, err := eventbus.DefaultBusConn()
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.publish: get bus connection: %w", err)
	}

	js, err := nc.JetStream(nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.publish: jetstream context: %w", err)
	}

	msg := &nats.Msg{
		Subject: input.GetSubject(),
		Data:    input.GetPayload(),
	}
	if hdrs := input.GetHeaders(); len(hdrs) > 0 {
		msg.Header = make(nats.Header, len(hdrs))
		for k, v := range hdrs {
			msg.Header.Set(k, v)
		}
	}
	if cid := input.GetCorrelationId(); cid != "" {
		if msg.Header == nil {
			msg.Header = make(nats.Header, 1)
		}
		msg.Header.Set("Nats-Correlation-Id", cid)
	}

	ack, err := js.PublishMsg(msg)
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.publish: %w", err)
	}

	return &sdk.TypedStepResult[*eventbusv1.PublishResponse]{
		Output: &eventbusv1.PublishResponse{
			Sequence: strconv.FormatUint(ack.Sequence, 10),
			AckedAt:  time.Now().UTC().Format(time.RFC3339),
		},
	}, nil
}
