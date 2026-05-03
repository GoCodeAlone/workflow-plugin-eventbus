package nats

import (
	"fmt"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// resourcesForKubernetes is the deploy-target stub for kubernetes.
//
// Per BMW pilot manifest: NATS on generic Kubernetes is built into the plugin
// but not activated for the pilot. This stub ensures config referencing
// deploy_target: kubernetes fails fast with a clear, actionable error.
//
// Full implementation (infra.k8s.statefulset + infra.k8s.service +
// infra.k8s.persistent_volume_claim for JetStream persistence, with optional
// NATS Operator CRDs) lands when a downstream consumer activates this target.
func resourcesForKubernetes(_ *eventbusv1.ClusterConfig) ([]iac.Resource, error) {
	return nil, fmt.Errorf(
		"nats: deploy target %q is not implemented for the pilot; "+
			"only %q is active — activate kubernetes by implementing deploy_kubernetes.go",
		providers.TargetKubernetes, providers.TargetDigitalOceanApp,
	)
}
