// Package nats provides the NATS event-bus Provider implementation.
// It emits typed IaC resource declarations for supported deploy targets.
//
// Activated targets for the BMW pilot:
//   - digitalocean.app_platform (TargetDigitalOceanApp) — fully implemented.
//
// Stub targets (not yet activated, return ErrNotImplemented-style errors):
//   - aws.ecs      — deploy_aws_ecs.go
//   - aws.eks      — deploy_aws_eks.go
//   - kubernetes   — deploy_kubernetes.go
package nats

import (
	"errors"
	"fmt"
	"strings"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// defaultVersion is the NATS server version used when ClusterConfig.Version is empty.
const defaultVersion = "2.10"

// provider is the NATS Provider implementation.
type provider struct{}

// New returns a fully-initialised NATS Provider.
func New() providers.Provider {
	return &provider{}
}

// Name implements providers.Provider.
func (p *provider) Name() string { return "nats" }

// Resources implements providers.Provider.
// It dispatches to the deploy-target–specific builder and returns the list of
// IaC resource declarations required to provision a NATS cluster on the target.
func (p *provider) Resources(cfg *eventbusv1.ClusterConfig, target providers.DeployTarget) ([]iac.Resource, error) {
	if err := providers.ValidateProviderTarget("nats", target); err != nil {
		return nil, err
	}
	switch target {
	case providers.TargetDigitalOceanApp:
		return resourcesForDOApp(cfg)
	case providers.TargetAWSECS:
		return resourcesForAWSECS(cfg)
	case providers.TargetAWSEKS:
		return resourcesForAWSEKS(cfg)
	case providers.TargetKubernetes:
		return resourcesForKubernetes(cfg)
	default:
		// TargetSelfHosted and any future recognised-but-unimplemented targets.
		return nil, fmt.Errorf(
			"nats: deploy target %q is not implemented for the pilot; "+
				"only %q is active — add a deploy_%s.go stub to activate this target",
			target, providers.TargetDigitalOceanApp,
			strings.ReplaceAll(string(target), ".", "_"),
		)
	}
}

// ConnectionString implements providers.Provider.
// It derives the broker connection URI from provisioned state. The state must
// contain a "uri" output (emitted by the IaC engine after provisioning the
// infra.container_service resource).
func (p *provider) ConnectionString(state iac.State, _ string) (string, error) {
	uri, ok := state.Output("uri")
	if !ok || uri == "" {
		return "", errors.New("nats: ConnectionString: 'uri' output not found in state; ensure the cluster has been provisioned")
	}
	return uri, nil
}

// StreamResources implements providers.Provider.
// It returns IaC resource declarations for the given JetStream streams.
// Task 21 provides the full implementation; this stub returns an empty list
// (acceptable until stream/consumer IaC emission is wired up).
func (p *provider) StreamResources(streams []*eventbusv1.StreamConfig, _ iac.State) ([]iac.Resource, error) {
	if len(streams) == 0 {
		return nil, nil
	}
	// Full implementation added in Task 21.
	return nil, fmt.Errorf("nats: StreamResources not yet implemented (Task 21)")
}

// Probe implements providers.Provider.
// It attempts a lightweight TCP connection to the NATS monitoring endpoint to
// determine cluster health. This implementation is network-free: the eventbus
// plugin does not import a NATS client SDK, so Probe currently returns
// HealthStatusUnreachable for any non-empty URI as a conservative default.
//
// A real health probe (HTTP GET /healthz on port 8222) is added once the
// NATS Go client dependency is approved via the dependency-review gate.
func (p *provider) Probe(uri string) providers.HealthCheck {
	if uri == "" {
		return providers.HealthCheck{
			URI:    uri,
			Status: providers.HealthStatusUnreachable,
			Err:    errors.New("nats: Probe: URI is empty"),
		}
	}
	// Conservative stub — no network I/O. Full probe added when NATS client
	// dependency is included.
	return providers.HealthCheck{
		URI:    uri,
		Status: providers.HealthStatusUnreachable,
		Err:    errors.New("nats: Probe: network probe not yet implemented; verify cluster status via the DigitalOcean console"),
	}
}
