package nats

import (
	"fmt"
	"strings"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
)

// natsClientPort is the standard NATS client connection port.
const natsClientPort = "4222"

// natsMonitorPort is the NATS HTTP monitoring endpoint port.
const natsMonitorPort = "8222"

// natsClusterPort is the NATS cluster routing port (inter-node, multi-replica).
const natsClusterPort = "6222"

// natsImage is the canonical Docker Hub NATS server image prefix.
const natsImage = "docker.io/library/nats"

// jetStreamStorageDir is the in-container path used for JetStream file storage.
const jetStreamStorageDir = "/data"

// resourcesForDOApp emits the IaC resource declarations required to run a NATS
// server (optionally with JetStream) on DigitalOcean App Platform.
//
// Emitted resources:
//   - infra.container_service — the NATS server process.
//
// The infra.container_service Properties are consumed by workflow-plugin-digitalocean
// (infra.container_service resource driver). String-encoded values follow the
// canonical key schema expected by the driver's buildAppSpec helper:
//
//	image            – Docker Hub image reference including tag.
//	instance_count   – number of replicas (string-encoded int32).
//	run_command      – NATS server flags (JetStream, storage dir, monitoring).
//	internal_ports   – comma-separated list of container ports.
//
// JetStream storage is ephemeral for the DO App Platform target — App Platform
// does not support attached block volumes. Data survives in-process but is lost
// on container restart. This is acceptable for the BMW pilot (staging); a
// production-grade persistent solution requires TargetAWSEKS or TargetKubernetes
// with a PersistentVolumeClaim.
func resourcesForDOApp(cfg *eventbusv1.ClusterConfig) ([]iac.Resource, error) {
	version := cfg.GetVersion()
	if version == "" {
		version = defaultVersion
	}

	replicas := cfg.GetReplicas()
	if replicas <= 0 {
		replicas = 1
	}

	image := fmt.Sprintf("%s:%s", natsImage, version)
	runCmd := buildRunCommand(cfg)
	ports := buildInternalPorts(cfg)

	svc := iac.Resource{
		Kind: "infra.container_service",
		Name: "nats",
		Properties: map[string]string{
			"image":          image,
			"instance_count": fmt.Sprintf("%d", replicas),
			"run_command":    runCmd,
			"internal_ports": ports,
		},
		Labels: map[string]string{
			"provider":    "nats",
			"deploy_target": string(cfg.GetDeployTarget()),
		},
	}

	return []iac.Resource{svc}, nil
}

// buildRunCommand constructs the NATS server command-line flags from ClusterConfig.
// When JetStream is enabled the -js (JetStream) and -sd (store directory) flags
// are included so the server persists stream state to jetStreamStorageDir.
func buildRunCommand(cfg *eventbusv1.ClusterConfig) string {
	var flags []string

	// Enable HTTP monitoring on the standard monitoring port.
	flags = append(flags, fmt.Sprintf("-m %s", natsMonitorPort))

	js := cfg.GetJetstream()
	if js != nil && js.GetEnabled() {
		flags = append(flags, "-js")
		flags = append(flags, fmt.Sprintf("-sd %s", jetStreamStorageDir))

		if js.GetMaxStorageBytes() > 0 {
			flags = append(flags, fmt.Sprintf("-ms %d", js.GetMaxStorageBytes()))
		}
		if js.GetMaxMemoryBytes() > 0 {
			flags = append(flags, fmt.Sprintf("-mm %d", js.GetMaxMemoryBytes()))
		}
	}

	// Cluster routing — only relevant when replicas > 1, but the flag is
	// harmless for single-instance deployments and enables zero-config
	// scale-up without a redeploy.
	if cfg.GetReplicas() > 1 {
		flags = append(flags, fmt.Sprintf("--cluster nats://0.0.0.0:%s", natsClusterPort))
	}

	return strings.Join(flags, " ")
}

// buildInternalPorts returns the comma-separated list of container ports to
// expose as internal (non-public) App Platform ports.
func buildInternalPorts(_ *eventbusv1.ClusterConfig) string {
	// 4222 — NATS client connections (always required).
	// 8222 — HTTP monitoring endpoint (health checks, JetStream stats).
	return strings.Join([]string{natsClientPort, natsMonitorPort}, ",")
}
