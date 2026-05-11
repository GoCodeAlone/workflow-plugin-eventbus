package eventbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
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
// trigger.eventbus.subscribe trigger type. When started with a non-nil callback
// it dispatches RuntimeBroker.Subscribe through the broker named by
// config.broker_ref (or the single registered broker as a legacy fallback) and
// invokes cb for each message received. When cb is nil (the external plugin
// gRPC path), Start is a no-op.
//
// The background goroutine is bounded:
//   - It exits cleanly when the context derived from Stop is cancelled.
//   - Subscribe blocks until ctx is cancelled or returns an error; on error the
//     loop pauses for triggerRetryDelay before re-dispatching, so a transient
//     broker outage does not spin.
//   - Backpressure is handled by Subscribe semantics: the handler runs
//     synchronously per message.
type subscribeTrigger struct {
	instanceName string
	config       *eventbusv1.ConsumerConfig
	cb           sdk.TriggerCallback

	cancel context.CancelFunc // set by Start; nil before first Start
	done   chan struct{}      // closed when the goroutine exits (nil before first Start with cb)
}

// Compile-time assertions.
var (
	_ sdk.ModuleInstance  = (*subscribeTrigger)(nil)
	_ sdk.TriggerInstance = (*subscribeTrigger)(nil)
)

// triggerRetryDelay is the pause between Subscribe dispatches when the
// previous Subscribe returned an error (broker unavailable, transient network
// fault). Short enough to recover quickly, long enough that a wedged broker
// doesn't spin the goroutine. Mirrors the prior fetchAndFire backoff window.
const triggerRetryDelay = time.Second

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
//
// Returns an error if Start has already been called without a matching Stop
// (double-start guard: avoids goroutine leak when the SDK calls Start twice).
func (t *subscribeTrigger) Start(ctx context.Context) error {
	if t.cb == nil {
		// External plugin path: callback is never wired — no goroutine needed.
		return nil
	}
	if t.cancel != nil {
		return fmt.Errorf("trigger.eventbus.subscribe %q: already started", t.instanceName)
	}

	trigCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	t.done = make(chan struct{})

	go t.subscribeLoop(trigCtx)
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

// subscribeLoop is the background goroutine that dispatches Subscribe through
// the configured RuntimeBroker. It exits when ctx is cancelled. On Subscribe
// returning an error (broker not registered yet, dial failure, etc.) it pauses
// triggerRetryDelay and re-dispatches; this preserves the prior fetchLoop's
// "keep retrying until the bus is up" behaviour through the new abstraction.
//
// Allocation note: a single *time.Timer is reused across retry iterations
// (Reset between sleeps, drained before Reset to handle the rare-but-
// possible fire-before-drain race). time.After would allocate a fresh
// Timer + channel on every failed Subscribe, which compounds when the
// broker is wedged for an extended period.
func (t *subscribeTrigger) subscribeLoop(ctx context.Context) {
	defer close(t.done)
	timer := time.NewTimer(triggerRetryDelay)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := t.dispatchSubscribe(ctx)
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Subscribe returned without error or because ctx was cancelled
			// — exit cleanly. Subscribe's contract is to block until ctx
			// cancellation, so a nil return is unusual but treated as
			// completion.
			return
		}
		// Pause before retrying so a wedged broker (e.g., not-yet-started)
		// doesn't spin the goroutine. Reset the shared timer rather than
		// allocating a fresh time.After channel on each iteration.
		timer.Reset(triggerRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

// dispatchSubscribe resolves the runtime + connection from the broker
// instance registry and invokes RuntimeBroker.Subscribe with a handler that
// shapes each Message into the trigger callback's data map. Returns
// Subscribe's error (or LookupRuntimeWithFallback's lookup error), to be
// retried by the surrounding loop.
//
// The callback data map mirrors workflow.plugin.eventbus.v1.Message:
// "subject", "payload" ([]byte), "headers" (map[string]string), "sequence",
// "published_at", "ack_token".
func (t *subscribeTrigger) dispatchSubscribe(ctx context.Context) error {
	rb, conn, err := LookupRuntimeWithFallback(t.config.GetBrokerRef())
	if err != nil {
		return fmt.Errorf("trigger.eventbus.subscribe %q: %w", t.instanceName, err)
	}

	handler := providers.MessageHandler(func(_ context.Context, msg *eventbusv1.Message) error {
		data := map[string]any{
			"subject":      msg.GetSubject(),
			"payload":      msg.GetPayload(),
			"headers":      msg.GetHeaders(),
			"sequence":     msg.GetSequence(),
			"published_at": msg.GetPublishedAt(),
			"ack_token":    msg.GetAckToken(),
		}
		if cbErr := t.cb("message", data); cbErr != nil {
			return fmt.Errorf("trigger.eventbus.subscribe %q: callback: %w", t.instanceName, cbErr)
		}
		return nil
	})

	if err := rb.Subscribe(ctx, conn, t.config.GetStreamName(), t.config.GetName(), handler); err != nil {
		return fmt.Errorf("trigger.eventbus.subscribe %q: subscribe: %w", t.instanceName, err)
	}
	return nil
}
