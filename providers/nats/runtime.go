// Package nats — providers.RuntimeBroker implementation backed by nats-go.
//
// This file extracts the NATS-specific publish/subscribe/ensure logic
// previously inlined in steps/publish.go, steps/consume.go, steps/ack.go,
// and trigger.go into a single Provider-shaped struct that satisfies the
// providers.RuntimeBroker contract.
//
// The extraction is structural only — same nats-go calls, same semantics,
// just relocated. steps/*.go and trigger.go are NOT modified in this PR;
// they continue calling nats-go directly. Group F refactors them to dispatch
// through the broker registry.
//
// URL resolution note (Group B deviation):
//
//	The plan references ClusterConfig.GetUrl(), but the proto currently
//	exposes only ClusterConfig.GetDsn() (documented as "Postgres DSN" for
//	the pgchannel provider). For the runtime layer we widen the meaning of
//	dsn to "broker connection string" so the same field carries the NATS
//	URL when provider="nats". This avoids a proto change inside Group B
//	(structural refactor only); a dedicated url field can land alongside
//	the steps/trigger refactor in Group F if desired.
package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	natsgo "github.com/nats-io/nats.go"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// natsRuntime is the providers.RuntimeBroker implementation for NATS JetStream.
// It carries no per-instance state; the underlying *natsgo.Conn is held by
// the returned Connection.
type natsRuntime struct{}

// NewRuntime returns a fresh providers.RuntimeBroker backed by nats-go.
// Each Connect call opens a new connection — runtimes themselves are stateless
// and safe to share across goroutines.
func NewRuntime() providers.RuntimeBroker { return &natsRuntime{} }

// natsConn wraps a *natsgo.Conn and implements providers.Connection.
type natsConn struct {
	nc *natsgo.Conn
}

// Close releases the underlying *natsgo.Conn. Idempotent — calling Close on
// an already-closed connection is a no-op via nats-go's own guard.
func (c *natsConn) Close() error {
	if c == nil || c.nc == nil {
		return nil
	}
	c.nc.Close()
	return nil
}

// Provider returns the static provider identifier "nats".
func (c *natsConn) Provider() string { return "nats" }

// Connect opens a NATS connection using the URL carried in cfg. The cfg's
// provider field must be "nats"; any other value (including empty) returns
// an error so callers cannot accidentally route a non-NATS cluster through
// this runtime.
//
// The connection URL is read from cfg.GetDsn() (see package doc for the
// dsn-as-broker-URL rationale). When dsn is empty Connect returns an error
// — there is no implicit env-var fallback at this layer; that responsibility
// stays with the module/step glue that resolves env → ClusterConfig.dsn.
func (r *natsRuntime) Connect(ctx context.Context, cfg *eventbusv1.ClusterConfig) (providers.Connection, error) {
	if cfg == nil {
		return nil, errors.New("nats: Connect: cfg is nil")
	}
	if got := cfg.GetProvider(); got != "nats" {
		return nil, fmt.Errorf("nats: Connect: provider = %q, want \"nats\"", got)
	}
	url := cfg.GetDsn()
	if url == "" {
		return nil, errors.New("nats: Connect: cfg.dsn is empty; populate ClusterConfig.dsn with the NATS URL before calling Connect")
	}
	nc, err := natsgo.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats: Connect: dial %s: %w", url, err)
	}
	return &natsConn{nc: nc}, nil
}

// asNATS extracts the underlying *natsgo.Conn from an opaque Connection.
// Returns an error when conn is nil or originated from a different provider.
func asNATS(conn providers.Connection) (*natsgo.Conn, error) {
	if conn == nil {
		return nil, errors.New("nats: connection is nil")
	}
	nc, ok := conn.(*natsConn)
	if !ok {
		return nil, fmt.Errorf("nats: connection has provider %q, want \"nats\"", conn.Provider())
	}
	if nc.nc == nil {
		return nil, errors.New("nats: underlying *nats.Conn is nil")
	}
	return nc.nc, nil
}

// translateStreamConfig converts a typed StreamConfig proto into nats.StreamConfig.
// Defaults align with the JetStream conventions used by the existing
// integration_test.go path: empty replicas → 1, zero MaxAge/AckWait → unset.
func translateStreamConfig(cfg *eventbusv1.StreamConfig) natsgo.StreamConfig {
	out := natsgo.StreamConfig{
		Name:     cfg.GetName(),
		Subjects: append([]string(nil), cfg.GetSubjects()...),
		MaxBytes: cfg.GetMaxBytes(),
	}
	if r := cfg.GetNumReplicas(); r > 0 {
		out.Replicas = int(r)
	} else {
		out.Replicas = 1
	}
	switch cfg.GetRetentionPolicy() {
	case eventbusv1.RetentionPolicy_RETENTION_POLICY_INTEREST:
		out.Retention = natsgo.InterestPolicy
	case eventbusv1.RetentionPolicy_RETENTION_POLICY_WORKQUEUE:
		out.Retention = natsgo.WorkQueuePolicy
	default:
		out.Retention = natsgo.LimitsPolicy
	}
	if d := cfg.GetMaxAge(); d != nil && d.IsValid() {
		out.MaxAge = d.AsDuration()
	}
	return out
}

// translateConsumerConfig converts a typed ConsumerConfig proto into
// nats.ConsumerConfig. Sensible defaults match step.eventbus.consume's
// expectations: explicit ack when unspecified (so step.eventbus.ack works),
// new delivery policy when unspecified.
func translateConsumerConfig(cfg *eventbusv1.ConsumerConfig) natsgo.ConsumerConfig {
	out := natsgo.ConsumerConfig{
		Durable:       cfg.GetName(),
		FilterSubject: cfg.GetFilterSubject(),
		MaxDeliver:    int(cfg.GetMaxDeliver()),
	}
	switch cfg.GetAckPolicy() {
	case eventbusv1.AckPolicy_ACK_POLICY_NONE:
		out.AckPolicy = natsgo.AckNonePolicy
	case eventbusv1.AckPolicy_ACK_POLICY_ALL:
		out.AckPolicy = natsgo.AckAllPolicy
	default:
		out.AckPolicy = natsgo.AckExplicitPolicy
	}
	switch cfg.GetDeliverPolicy() {
	case eventbusv1.DeliverPolicy_DELIVER_POLICY_ALL:
		out.DeliverPolicy = natsgo.DeliverAllPolicy
	case eventbusv1.DeliverPolicy_DELIVER_POLICY_LAST:
		out.DeliverPolicy = natsgo.DeliverLastPolicy
	case eventbusv1.DeliverPolicy_DELIVER_POLICY_BY_START_SEQUENCE:
		out.DeliverPolicy = natsgo.DeliverByStartSequencePolicy
	case eventbusv1.DeliverPolicy_DELIVER_POLICY_BY_START_TIME:
		out.DeliverPolicy = natsgo.DeliverByStartTimePolicy
	default:
		out.DeliverPolicy = natsgo.DeliverNewPolicy
	}
	return out
}

// streamConfigMatches reports whether the existing stream's config already
// matches what cfg would declare. Used to short-circuit UpdateStream when
// EnsureStream is called repeatedly with the same input.
func streamConfigMatches(existing natsgo.StreamConfig, want natsgo.StreamConfig) bool {
	if existing.Name != want.Name {
		return false
	}
	if existing.Retention != want.Retention {
		return false
	}
	if existing.MaxBytes != want.MaxBytes {
		return false
	}
	if existing.MaxAge != want.MaxAge {
		return false
	}
	if existing.Replicas != want.Replicas {
		return false
	}
	if len(existing.Subjects) != len(want.Subjects) {
		return false
	}
	for i := range existing.Subjects {
		if existing.Subjects[i] != want.Subjects[i] {
			return false
		}
	}
	return true
}

// consumerConfigMatches reports whether the existing consumer's config matches
// what cfg would declare. Sister of streamConfigMatches for EnsureConsumer.
func consumerConfigMatches(existing natsgo.ConsumerConfig, want natsgo.ConsumerConfig) bool {
	if existing.Durable != want.Durable {
		return false
	}
	if existing.FilterSubject != want.FilterSubject {
		return false
	}
	if existing.AckPolicy != want.AckPolicy {
		return false
	}
	if existing.DeliverPolicy != want.DeliverPolicy {
		return false
	}
	if existing.MaxDeliver != want.MaxDeliver {
		return false
	}
	return true
}

// EnsureStream idempotently creates or updates the JetStream stream described
// by cfg. If the stream already exists with a matching config, EnsureStream
// is a no-op; otherwise it calls js.AddStream (when missing) or js.UpdateStream
// (when present-but-different).
func (r *natsRuntime) EnsureStream(ctx context.Context, conn providers.Connection, cfg *eventbusv1.StreamConfig) error {
	nc, err := asNATS(conn)
	if err != nil {
		return fmt.Errorf("nats: EnsureStream: %w", err)
	}
	if cfg == nil {
		return errors.New("nats: EnsureStream: cfg is nil")
	}
	if cfg.GetName() == "" {
		return errors.New("nats: EnsureStream: cfg.name is required")
	}
	if len(cfg.GetSubjects()) == 0 {
		return fmt.Errorf("nats: EnsureStream: stream %q has no subjects; at least one subject filter is required", cfg.GetName())
	}
	js, err := nc.JetStream(natsgo.Context(ctx))
	if err != nil {
		return fmt.Errorf("nats: EnsureStream %q: jetstream context: %w", cfg.GetName(), err)
	}
	want := translateStreamConfig(cfg)
	info, infoErr := js.StreamInfo(cfg.GetName(), natsgo.Context(ctx))
	switch {
	case infoErr == nil:
		if streamConfigMatches(info.Config, want) {
			return nil
		}
		if _, err := js.UpdateStream(&want, natsgo.Context(ctx)); err != nil {
			return fmt.Errorf("nats: EnsureStream %q: update: %w", cfg.GetName(), err)
		}
		return nil
	case errors.Is(infoErr, natsgo.ErrStreamNotFound):
		if _, err := js.AddStream(&want, natsgo.Context(ctx)); err != nil {
			return fmt.Errorf("nats: EnsureStream %q: add: %w", cfg.GetName(), err)
		}
		return nil
	default:
		return fmt.Errorf("nats: EnsureStream %q: stream info: %w", cfg.GetName(), infoErr)
	}
}

// EnsureConsumer idempotently creates or updates the durable JetStream
// consumer described by cfg on the named stream. Same three-way pattern as
// EnsureStream: no-op when matching, UpdateConsumer when diverging,
// AddConsumer when missing.
func (r *natsRuntime) EnsureConsumer(ctx context.Context, conn providers.Connection, streamName string, cfg *eventbusv1.ConsumerConfig) error {
	nc, err := asNATS(conn)
	if err != nil {
		return fmt.Errorf("nats: EnsureConsumer: %w", err)
	}
	if streamName == "" {
		return errors.New("nats: EnsureConsumer: streamName is required")
	}
	if cfg == nil {
		return errors.New("nats: EnsureConsumer: cfg is nil")
	}
	if cfg.GetName() == "" {
		return errors.New("nats: EnsureConsumer: cfg.name is required (durable consumer name)")
	}
	js, err := nc.JetStream(natsgo.Context(ctx))
	if err != nil {
		return fmt.Errorf("nats: EnsureConsumer %q: jetstream context: %w", cfg.GetName(), err)
	}
	want := translateConsumerConfig(cfg)
	info, infoErr := js.ConsumerInfo(streamName, cfg.GetName(), natsgo.Context(ctx))
	switch {
	case infoErr == nil:
		if consumerConfigMatches(info.Config, want) {
			return nil
		}
		if _, err := js.UpdateConsumer(streamName, &want, natsgo.Context(ctx)); err != nil {
			return fmt.Errorf("nats: EnsureConsumer %q on stream %q: update: %w", cfg.GetName(), streamName, err)
		}
		return nil
	case errors.Is(infoErr, natsgo.ErrConsumerNotFound):
		if _, err := js.AddConsumer(streamName, &want, natsgo.Context(ctx)); err != nil {
			return fmt.Errorf("nats: EnsureConsumer %q on stream %q: add: %w", cfg.GetName(), streamName, err)
		}
		return nil
	default:
		return fmt.Errorf("nats: EnsureConsumer %q on stream %q: consumer info: %w", cfg.GetName(), streamName, infoErr)
	}
}

// subscribeBatchSize is the per-Fetch batch size used by the Subscribe loop.
// Matches the existing trigger.go default (one message per fetch) — Group F
// will reconsider this once pull-vs-push semantics split.
const subscribeBatchSize = 1

// subscribeMaxWait is the per-Fetch MaxWait cap, mirroring trigger.go's
// fetchPollInterval so the goroutine wakes at least once per interval to
// observe ctx cancellation even when the stream is idle.
const subscribeMaxWait = 2 * time.Second

// Publish publishes a single message to JetStream and returns the
// broker-assigned sequence number + ack timestamp. Header preservation
// mirrors steps/publish.go: PublishRequest.headers populate nats.Header;
// CorrelationId (when non-empty) is stamped onto a "Nats-Correlation-Id"
// header so existing consumers see identical metadata to the legacy path.
//
// The returned PublishResponse.Sequence is the stream-scoped sequence from
// nats.PubAck (formatted as decimal string to match the proto's typing);
// AckedAt is the local UTC time at which the broker confirmed the publish.
func (r *natsRuntime) Publish(ctx context.Context, conn providers.Connection, req *eventbusv1.PublishRequest) (*eventbusv1.PublishResponse, error) {
	nc, err := asNATS(conn)
	if err != nil {
		return nil, fmt.Errorf("nats: Publish: %w", err)
	}
	if req == nil {
		return nil, errors.New("nats: Publish: req is nil")
	}
	if req.GetSubject() == "" {
		return nil, errors.New("nats: Publish: subject is required")
	}
	js, err := nc.JetStream(natsgo.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("nats: Publish: jetstream context: %w", err)
	}
	msg := &natsgo.Msg{
		Subject: req.GetSubject(),
		Data:    req.GetPayload(),
	}
	if hdrs := req.GetHeaders(); len(hdrs) > 0 {
		msg.Header = make(natsgo.Header, len(hdrs))
		for k, v := range hdrs {
			msg.Header.Set(k, v)
		}
	}
	if cid := req.GetCorrelationId(); cid != "" {
		if msg.Header == nil {
			msg.Header = make(natsgo.Header, 1)
		}
		msg.Header.Set("Nats-Correlation-Id", cid)
	}
	ack, err := js.PublishMsg(msg, natsgo.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("nats: Publish: %w", err)
	}
	return &eventbusv1.PublishResponse{
		Sequence: strconv.FormatUint(ack.Sequence, 10),
		AckedAt:  time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// Subscribe attaches handler to the named durable consumer and blocks until
// ctx is cancelled or an unrecoverable error occurs. It uses JetStream's
// pull-based Fetch model in a loop — this is "push-via-fetch" semantics,
// matching the existing trigger.go path verbatim.
//
// The RuntimeBroker.Subscribe signature today (with positional streamName +
// consumerName) is interim; see providers/runtime.go TODO(Group F) for the
// planned split into pull (returns one batch) and push (long-lived handler)
// variants once step factories refactor onto this interface.
//
// Per-message contract:
//   - handler returning nil → m.Ack() (the message is acknowledged on the broker).
//   - handler returning err  → m.Nak() (the message is redelivered, subject
//     to ConsumerConfig.max_deliver).
//
// Loop termination:
//   - ctx cancellation exits cleanly between Fetch rounds, returning ctx.Err().
//   - nats.ErrTimeout (idle fetch) is non-fatal and triggers the next Fetch.
//   - Any other Fetch error is returned to the caller (no retry inside the
//     loop; the caller decides whether to retry by calling Subscribe again).
//
// Each delivered nats.Msg's Reply subject is exposed on Message.AckToken so
// downstream callers can pass it to Ack for delayed/explicit acknowledgement
// (the step.eventbus.consume + step.eventbus.ack pattern). When the handler
// returns nil we also call m.Ack() directly here — Group B preserves both
// ack paths (auto-ack via handler nil; manual-ack via AckToken + Ack()) so
// the structural refactor doesn't break either existing caller.
func (r *natsRuntime) Subscribe(ctx context.Context, conn providers.Connection, streamName, consumerName string, handler providers.MessageHandler) error {
	nc, err := asNATS(conn)
	if err != nil {
		return fmt.Errorf("nats: Subscribe: %w", err)
	}
	if streamName == "" {
		return errors.New("nats: Subscribe: streamName is required")
	}
	if consumerName == "" {
		return errors.New("nats: Subscribe: consumerName is required")
	}
	if handler == nil {
		return errors.New("nats: Subscribe: handler is nil")
	}
	js, err := nc.JetStream(natsgo.Context(ctx))
	if err != nil {
		return fmt.Errorf("nats: Subscribe: jetstream context: %w", err)
	}
	// Empty subject + BindStream identifies the consumer by durable name + stream.
	sub, err := js.PullSubscribe("", consumerName, natsgo.BindStream(streamName))
	if err != nil {
		return fmt.Errorf("nats: Subscribe: pull subscribe: %w", err)
	}
	defer func() { _ = sub.Drain() }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		msgs, fetchErr := sub.Fetch(subscribeBatchSize, natsgo.MaxWait(subscribeMaxWait))
		if fetchErr != nil {
			if errors.Is(fetchErr, natsgo.ErrTimeout) {
				continue // idle, normal
			}
			// If ctx was cancelled mid-fetch, surface that instead of the wrapper.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("nats: Subscribe: fetch: %w", fetchErr)
		}
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
			handlerErr := handler(ctx, pbMsg)
			if handlerErr != nil {
				if nakErr := m.Nak(); nakErr != nil {
					// Surface the handler error; nak failure is logged context.
					return fmt.Errorf("nats: Subscribe: handler: %w (nak also failed: %v)", handlerErr, nakErr)
				}
				return fmt.Errorf("nats: Subscribe: handler: %w", handlerErr)
			}
			if ackErr := m.Ack(); ackErr != nil {
				return fmt.Errorf("nats: Subscribe: ack: %w", ackErr)
			}
		}
	}
}

// Ack acknowledges a previously delivered JetStream message identified by
// ackToken. The token is the NATS reply subject (Message.AckToken from
// Subscribe / step.eventbus.consume); publishing an empty payload to that
// subject is the standard JetStream explicit-ack pattern — same mechanism
// as steps/ack.go.
//
// ctx cancellation is observed via natsgo.Context on the publish RPC so an
// Ack call cannot block indefinitely on a wedged connection.
func (r *natsRuntime) Ack(ctx context.Context, conn providers.Connection, ackToken string) error {
	nc, err := asNATS(conn)
	if err != nil {
		return fmt.Errorf("nats: Ack: %w", err)
	}
	if ackToken == "" {
		return errors.New("nats: Ack: ackToken is required")
	}
	// nc.Publish does not accept a context; use a RequestWithContext-style guard
	// by checking ctx before the call. nc.Publish is fire-and-forget and returns
	// quickly (it only enqueues into the client write buffer), so ctx cancel
	// during the publish itself is not a practical concern.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("nats: Ack: %w", err)
	}
	if err := nc.Publish(ackToken, nil); err != nil {
		return fmt.Errorf("nats: Ack: publish ack: %w", err)
	}
	// Flush so the ack reaches the broker before Ack returns; otherwise the
	// caller could observe a stale "not yet acked" state. Prefer the
	// context-aware flush when ctx has a deadline; otherwise fall back to the
	// nats-go default timeout (nc.Flush()) — FlushWithContext rejects a
	// deadline-less context with "nats: context requires a deadline".
	if _, deadlined := ctx.Deadline(); deadlined {
		if err := nc.FlushWithContext(ctx); err != nil {
			return fmt.Errorf("nats: Ack: flush: %w", err)
		}
	} else {
		if err := nc.Flush(); err != nil {
			return fmt.Errorf("nats: Ack: flush: %w", err)
		}
	}
	return nil
}

// Compile-time assertion that natsRuntime satisfies providers.RuntimeBroker.
var _ providers.RuntimeBroker = (*natsRuntime)(nil)
