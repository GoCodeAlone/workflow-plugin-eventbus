// Package providers defines the Provider interface and DeployTarget compatibility
// matrix for workflow-plugin-eventbus. Each provider (nats, kafka, kinesis)
// implements Provider and emits typed IaC resource declarations for its supported
// deploy targets.
package providers

import (
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
)

// HealthStatus is the typed health state returned by Provider.Probe.
type HealthStatus string

const (
	// HealthStatusHealthy indicates the broker is reachable and fully operational.
	HealthStatusHealthy HealthStatus = "healthy"
	// HealthStatusDegraded indicates the broker is reachable but impaired
	// (e.g. a replica is down, JetStream is slow).
	HealthStatusDegraded HealthStatus = "degraded"
	// HealthStatusUnreachable indicates the broker did not respond within the probe timeout.
	HealthStatusUnreachable HealthStatus = "unreachable"
)

// HealthCheck is the result of a liveness probe against an event-bus URI.
type HealthCheck struct {
	// URI is the address that was probed.
	URI string
	// Status is the typed health state.
	Status HealthStatus
	// Err is non-nil when Status is HealthStatusDegraded or HealthStatusUnreachable.
	Err error
}

// Provider is the interface all event-bus provider adapters must implement.
// Each provider translates a ClusterConfig + DeployTarget into a set of typed
// IaC resource declarations without directly calling cloud APIs.
//
// Implementations live at providers/{nats,kafka,kinesis}/.
type Provider interface {
	// Name returns the provider identifier: "nats", "kafka", or "kinesis".
	Name() string

	// Resources returns the IaC resource declarations required to provision
	// the event-bus cluster described by cfg on the given deploy target.
	// cfg is passed as a pointer because proto messages embed sync.Mutex via
	// protoimpl.MessageState and must not be copied by value.
	// Returns an error if the provider × target combination is unsupported
	// or if cfg is invalid for this provider.
	Resources(cfg *eventbusv1.ClusterConfig, target DeployTarget) ([]iac.Resource, error)

	// ConnectionString derives the broker connection string from provisioned state.
	// env selects environment-specific outputs (e.g. "prod", "staging").
	ConnectionString(state iac.State, env string) (string, error)

	// StreamResources returns the IaC resource declarations required to
	// declare the given streams against an already-provisioned cluster
	// (represented by state). Streams are pointers for the same reason as cfg.
	StreamResources(streams []*eventbusv1.StreamConfig, state iac.State) ([]iac.Resource, error)

	// Probe probes the event-bus cluster at uri and returns its health state.
	Probe(uri string) HealthCheck
}
