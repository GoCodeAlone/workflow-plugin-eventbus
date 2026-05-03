// Package kafka provides a stub implementation of the kafka event-bus Provider.
//
// Per pilot manifest out-of-scope: "DO Managed Kafka and AWS Kinesis as eventbus
// providers active for pilot — built into plugin but not activated; NATS only."
// This stub registers the kafka provider in the registry so that config referencing
// provider: kafka fails fast with a clear error rather than panicking or silently
// doing nothing. Real implementation lands when a downstream consumer activates it.
package kafka

import (
	"errors"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// errNotImplemented is the canonical error returned by all stub methods.
var errNotImplemented = errors.New("kafka provider not implemented for pilot; register a real implementation when activating")

// provider is the stub kafka Provider.
type provider struct{}

// New returns the stub kafka Provider.
func New() providers.Provider {
	return &provider{}
}

// Name implements providers.Provider.
func (p *provider) Name() string { return "kafka" }

// Resources implements providers.Provider — stub, always errors.
func (p *provider) Resources(_ *eventbusv1.ClusterConfig, _ providers.DeployTarget) ([]iac.Resource, error) {
	return nil, errNotImplemented
}

// ConnectionString implements providers.Provider — stub, always errors.
func (p *provider) ConnectionString(_ iac.State, _ string) (string, error) {
	return "", errNotImplemented
}

// StreamResources implements providers.Provider — stub, always errors.
func (p *provider) StreamResources(_ []*eventbusv1.StreamConfig, _ iac.State) ([]iac.Resource, error) {
	return nil, errNotImplemented
}

// Probe implements providers.Provider — stub, always returns unreachable.
func (p *provider) Probe(uri string) providers.HealthCheck {
	return providers.HealthCheck{
		URI:    uri,
		Status: providers.HealthStatusUnreachable,
		Err:    errNotImplemented,
	}
}
