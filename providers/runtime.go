// Package providers — RuntimeBroker abstraction.
//
// RuntimeBroker decouples publish/subscribe + stream/consumer management
// from the specific broker backend. Provider implementations live in
// sub-packages (providers/nats, providers/pgchannel, future kafka/kinesis)
// and are selected at module Start time from ClusterConfig.Provider.
//
// This interface is consumed by:
//   - module.clusterModule  (Connect + lifecycle)
//   - module.streamModule   (EnsureStream)
//   - module.consumerModule (EnsureConsumer)
//   - steps/publish.go      (Publish)
//   - steps/consume.go      (Subscribe — pull semantics)
//   - steps/ack.go          (Ack)
//   - trigger.go            (Subscribe — push semantics)
package providers

import (
	"context"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

// RuntimeBroker abstracts publish/subscribe + stream/consumer operations
// across providers (nats, pgchannel, future kafka/kinesis).
//
// Implementations MUST be safe for concurrent use across goroutines once
// Connect has returned a Connection. All methods take a context; cancellation
// MUST propagate to in-flight broker operations.
type RuntimeBroker interface {
	// Connect opens a connection to the broker described by cfg. The returned
	// Connection is opaque to callers and is passed back into subsequent
	// EnsureStream / EnsureConsumer / Publish / Subscribe / Ack calls.
	Connect(ctx context.Context, cfg *eventbusv1.ClusterConfig) (Connection, error)

	// EnsureStream creates or updates the stream described by cfg. Implementations
	// MUST be idempotent: calling EnsureStream twice with the same cfg is a no-op.
	EnsureStream(ctx context.Context, conn Connection, cfg *eventbusv1.StreamConfig) error

	// EnsureConsumer creates or updates the consumer described by cfg on the named
	// stream. Implementations MUST be idempotent.
	EnsureConsumer(ctx context.Context, conn Connection, streamName string, cfg *eventbusv1.ConsumerConfig) error

	// Publish publishes a single message via the broker. The returned
	// PublishResponse carries provider-assigned sequence / timestamp metadata.
	Publish(ctx context.Context, conn Connection, req *eventbusv1.PublishRequest) (*eventbusv1.PublishResponse, error)

	// Subscribe attaches handler to streamName / consumerName and blocks until
	// ctx is cancelled or an unrecoverable error occurs. Returning nil from
	// handler acks the message; returning an error naks it (delivery counted
	// against max_deliver per ConsumerConfig).
	Subscribe(ctx context.Context, conn Connection, streamName, consumerName string, handler MessageHandler) error

	// Ack acknowledges a previously delivered message identified by ackToken
	// (Message.ack_token). Used by step.eventbus.ack for explicit-ack flows.
	Ack(ctx context.Context, conn Connection, ackToken string) error
}

// Connection is a provider-specific connection handle. Concrete types live in
// each provider sub-package and embed whatever broker client they need
// (*nats.Conn, *pgxpool.Pool, etc.).
//
// Callers MUST treat Connection as opaque and only pass it back into
// RuntimeBroker methods. Close releases all underlying resources.
type Connection interface {
	// Close releases the underlying broker connection / pool. Idempotent.
	Close() error
	// Provider returns the provider identifier this connection belongs to
	// (e.g. "nats", "pgchannel"). Used for diagnostics + telemetry.
	Provider() string
}

// MessageHandler is invoked by RuntimeBroker.Subscribe for each delivered message.
// Returning nil acknowledges the message; returning a non-nil error naks it,
// counting toward ConsumerConfig.max_deliver.
type MessageHandler func(ctx context.Context, msg *eventbusv1.Message) error
