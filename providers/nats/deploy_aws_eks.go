package nats

import (
	"fmt"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// resourcesForAWSEKS is the deploy-target stub for aws.eks.
//
// Per BMW pilot manifest: NATS on AWS EKS is built into the plugin but not
// activated for the pilot. This stub ensures config referencing
// deploy_target: aws.eks fails fast with a clear, actionable error.
//
// Full implementation (infra.k8s.statefulset + infra.k8s.service +
// infra.k8s.persistent_volume_claim via EBS for JetStream persistence)
// lands when a downstream consumer activates this target.
func resourcesForAWSEKS(_ *eventbusv1.ClusterConfig) ([]iac.Resource, error) {
	return nil, fmt.Errorf(
		"nats: deploy target %q is not implemented for the pilot; "+
			"only %q is active — activate aws.eks by implementing deploy_aws_eks.go",
		providers.TargetAWSEKS, providers.TargetDigitalOceanApp,
	)
}
