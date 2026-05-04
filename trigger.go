package eventbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/anypb"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ── SubscribeTriggerModuleFactory (TypedModuleProvider) ───────────────────────

// SubscribeTriggerModuleFactory implements sdk.TypedModuleProvider for the
// trigger.eventbus.subscribe module type. The external plugin adapter calls
// CreateTypedModule with the trigger type name to instantiate triggers over gRPC.
type SubscribeTriggerModuleFactory struct{}

// Compile-time assertion: SubscribeTriggerModuleFactory implements sdk.TypedModuleProvider.
var _ sdk.TypedModuleProvider = (*SubscribeTriggerModuleFactory)(nil)

// TypedModuleTypes returns the single trigger module type served by this factory.
func (f *SubscribeTriggerModuleFactory) TypedModuleTypes() []string {
	return []string{"trigger.eventbus.subscribe"}
}

// CreateTypedModule unpacks the typed proto config and delegates to NewSubscribeTrigger.
// cb is always nil in the external gRPC subprocess path (the callback client is
// never wired in production SDK code); triggers that receive cb=nil behave as
// no-ops on Start, which is correct for IaC-only and plan/apply workflows.
func (f *SubscribeTriggerModuleFactory) CreateTypedModule(typeName, name string, config *anypb.Any) (sdk.ModuleInstance, error) {
	if typeName != "trigger.eventbus.subscribe" {
		return nil, fmt.Errorf("%w: module type %q", sdk.ErrTypedContractNotHandled, typeName)
	}
	var cfg eventbusv1.ConsumerConfig
	if config != nil {
		if err := config.UnmarshalTo(&cfg); err != nil {
			return nil, fmt.Errorf("trigger.eventbus.subscribe %q: unmarshal typed config: %w", name, err)
		}
	}
	// cb is nil in the external plugin gRPC path; the trigger is a no-op on Start.
	return NewSubscribeTrigger(name, &cfg, nil)
}

// ── subscribeTrigger (ModuleInstance + TriggerInstance) ──────────────────────

// subscribeTrigger implements sdk.ModuleInstance and sdk.TriggerInstance for the
// trigger.eventbus.subscribe trigger type. When started with a non-nil callback it
// subscribes to the configured JetStream stream and invokes cb for each message
// received. When cb is nil (the external plugin gRPC path), Start is a no-op.
//
// The background goroutine is bounded:
//   - It exits cleanly when the context derived from Stop is cancelled.
//   - Each fetch has a maxWait cap so the loop wakes up at least once per
//     fetchPollInterval even when the stream is idle — this ensures timely shutdown.
//   - Backpressure is handled by the JetStream PullSubscribe+Fetch model:
//     the goroutine processes one batch synchronously before fetching the next.
type subscribeTrigger struct {
	instanceName string
	config       *eventbusv1.ConsumerConfig
	cb           sdk.TriggerCallback

	cancel context.CancelFunc  // set by Start; nil before first Start
	done   chan struct{}        // closed when the goroutine exits (nil before first Start with cb)
}

// Compile-time assertions.
var (
	_ sdk.ModuleInstance  = (*subscribeTrigger)(nil)
	_ sdk.TriggerInstance = (*subscribeTrigger)(nil)
)

// fetchPollInterval is the maximum wait per JetStream Fetch call inside the
// trigger goroutine. Keeping it short ensures the goroutine can detect ctx
// cancellation quickly without waiting for a full batch timeout.
const fetchPollInterval = 2 * time.Second

// NewSubscribeTrigger creates a subscribeTrigger from a typed ConsumerConfig proto.
//
// Returns an error if:
//   - config.name is empty
//   - config.stream_name is empty
func NewSubscribeTrigger(instanceName string, cfg *eventbusv1.ConsumerConfig, cb sdk.TriggerCallback) (sdk.ModuleInstance, error) {
	if cfg.GetName() == "" {
		return nil, fmt.Errorf("trigger.eventbus.subscribe %q: config.name is required", instanceName)
	}
	if cfg.GetStreamName() == "" {
		return nil, fmt.Errorf("trigger.eventbus.subscribe %q: config.stream_name is required", instanceName)
	}
	return &subscribeTrigger{
		instanceName: instanceName,
		config:       cfg,
		cb:           cb,
	}, nil
}

// Init is a no-op; the trigger registers nothing during init.
func (t *subscribeTrigger) Init() error { return nil }

// Start launches the trigger goroutine if cb is non-nil. If cb is nil (the
// external plugin gRPC path), Start is a no-op.
func (t *subscribeTrigger) Start(ctx context.Context) error {
	if t.cb == nil {
		// External plugin path: callback is never wired — no goroutine needed.
		return nil
	}

	trigCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	t.done = make(chan struct{})

	go t.fetchLoop(trigCtx)
	return nil
}

// Stop cancels the trigger goroutine and waits for it to exit.
// Stop is idempotent — calling it before Start or when cb was nil is safe.
func (t *subscribeTrigger) Stop(_ context.Context) error {
	if t.cancel != nil {
		t.cancel()
	}
	if t.done != nil {
		<-t.done
	}
	return nil
}

// fetchLoop is the background goroutine that pulls messages from JetStream and
// invokes the trigger callback. It exits when ctx is cancelled.
func (t *subscribeTrigger) fetchLoop(ctx context.Context) {
	defer close(t.done)

	for {
		// Exit immediately on context cancellation before each fetch round.
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := t.fetchAndFire(ctx); err != nil {
			// Log the error but keep retrying — the bus may be temporarily
			// unavailable or the stream may not exist yet.
			// A 1-second back-off prevents a tight spin loop on persistent errors.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

// fetchAndFire dials the bus, fetches one batch of messages, and invokes cb for
// each one. It returns an error if the connection or fetch fails (the caller
// retries). A JetStream timeout (empty batch) is not treated as an error.
func (t *subscribeTrigger) fetchAndFire(ctx context.Context) error {
	nc, err := DefaultBusConn()
	if err != nil {
		return fmt.Errorf("trigger.eventbus.subscribe %q: get bus connection: %w", t.instanceName, err)
	}

	js, err := nc.JetStream(nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("trigger.eventbus.subscribe %q: jetstream context: %w", t.instanceName, err)
	}

	subj := t.config.GetFilterSubject()
	opts := []nats.SubOpt{nats.BindStream(t.config.GetStreamName())}
	sub, err := js.PullSubscribe(subj, t.config.GetName(), opts...)
	if err != nil {
		return fmt.Errorf("trigger.eventbus.subscribe %q: pull subscribe: %w", t.instanceName, err)
	}
	defer sub.Drain() //nolint:errcheck // best-effort; ephemeral per-fetch subscription

	msgs, err := sub.Fetch(1, nats.MaxWait(fetchPollInterval))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil // no messages — normal idle case
		}
		return fmt.Errorf("trigger.eventbus.subscribe %q: fetch: %w", t.instanceName, err)
	}

	for _, m := range msgs {
		data := map[string]any{
			"subject": m.Subject,
			"payload": string(m.Data),
			"reply":   m.Reply,
		}
		if err := t.cb("message", data); err != nil {
			// Callback errors are non-fatal; log via returned error and continue.
			return fmt.Errorf("trigger.eventbus.subscribe %q: callback: %w", t.instanceName, err)
		}
	}
	return nil
}
