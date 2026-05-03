package nats

import (
	"fmt"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// resourcesForAWSECS is the deploy-target stub for aws.ecs.
//
// Per BMW pilot manifest: NATS on AWS ECS is built into the plugin but not
// activated for the pilot — NATS × digitalocean.app_platform is the only
// active path. This stub ensures that config referencing
// deploy_target: aws.ecs fails fast with a clear, actionable error rather
// than falling through to a silent no-op or a confusing generic message.
//
// Full implementation (infra.ecs.task_definition + infra.ecs.service +
// infra.efs.file_system for JetStream persistence) lands when a downstream
// consumer activates this target.
func resourcesForAWSECS(_ *eventbusv1.ClusterConfig) ([]iac.Resource, error) {
	return nil, fmt.Errorf(
		"nats: deploy target %q is not implemented for the pilot; "+
			"only %q is active — activate aws.ecs by implementing deploy_aws_ecs.go",
		providers.TargetAWSECS, providers.TargetDigitalOceanApp,
	)
}
