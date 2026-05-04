package steps

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ConsumeHandler implements step.eventbus.consume. It binds to an existing
// JetStream durable consumer (looked up by the durable name in
// ConsumeRequest.consumer) and fetches up to batch_size messages, waiting at
// most max_wait for the batch to fill.
//
// Returned messages include ack_token = msg.Reply, which the caller passes to
// step.eventbus.ack to acknowledge each message individually.
func ConsumeHandler(
	ctx context.Context,
	req sdk.TypedStepRequest[*emptypb.Empty, *eventbusv1.ConsumeRequest],
) (*sdk.TypedStepResult[*eventbusv1.ConsumeResponse], error) {
	input := req.Input
	if input.GetConsumer() == "" {
		return nil, fmt.Errorf("step.eventbus.consume: consumer is required")
	}

	cfg, ok := eventbus.GetConsumerByName(input.GetConsumer())
	if !ok {
		return nil, fmt.Errorf(
			"step.eventbus.consume: consumer %q not registered; add an infra.eventbus.consumer module with name=%q",
			input.GetConsumer(), input.GetConsumer(),
		)
	}

	const maxBatchSize = 1000
	batch := int(input.GetBatchSize())
	if batch <= 0 {
		batch = 1
	}
	if batch > maxBatchSize {
		batch = maxBatchSize
	}

	nc, err := eventbus.DefaultBusConn()
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.consume: get bus connection: %w", err)
	}

	js, err := nc.JetStream(nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.consume: jetstream context: %w", err)
	}

	// Bind to the existing JetStream consumer by durable name + stream name.
	// filter_subject is passed as the subject; when empty, BindStream alone
	// identifies the consumer.
	subj := cfg.GetFilterSubject()
	opts := []nats.SubOpt{nats.BindStream(cfg.GetStreamName())}
	sub, err := js.PullSubscribe(subj, cfg.GetName(), opts...)
	if err != nil {
		return nil, fmt.Errorf("step.eventbus.consume: pull subscribe: %w", err)
	}
	defer sub.Drain() //nolint:errcheck // best-effort; PullSubscribe is ephemeral per-fetch

	var fetchOpts []nats.PullOpt
	if mw := input.GetMaxWait(); mw != nil && mw.AsDuration() > 0 {
		fetchOpts = append(fetchOpts, nats.MaxWait(mw.AsDuration()))
	}

	msgs, err := sub.Fetch(batch, fetchOpts...)
	if err != nil && !errors.Is(err, nats.ErrTimeout) {
		return nil, fmt.Errorf("step.eventbus.consume: fetch: %w", err)
	}

	result := make([]*eventbusv1.Message, 0, len(msgs))
	for _, m := range msgs {
		pbMsg := &eventbusv1.Message{
			Subject:  m.Subject,
			Payload:  m.Data,
			AckToken: m.Reply,
		}
		if len(m.Header) > 0 {
			pbMsg.Headers = make(map[string]string, len(m.Header))
			for k, vals := range m.Header {
				if len(vals) > 0 {
					pbMsg.Headers[k] = vals[0]
				}
			}
		}
		if meta, err := m.Metadata(); err == nil && meta != nil {
			pbMsg.Sequence = strconv.FormatUint(meta.Sequence.Stream, 10)
			pbMsg.PublishedAt = meta.Timestamp.UTC().Format(time.RFC3339)
		}
		result = append(result, pbMsg)
	}

	return &sdk.TypedStepResult[*eventbusv1.ConsumeResponse]{
		Output: &eventbusv1.ConsumeResponse{Messages: result},
	}, nil
}
