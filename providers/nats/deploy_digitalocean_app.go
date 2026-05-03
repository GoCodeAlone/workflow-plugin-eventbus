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
//   - infra.container_service — the NATS server process (always).
//   - infra.storage           — a DigitalOcean Spaces bucket for JetStream
//     persistence (emitted only when JetStream is enabled). The Spaces bucket
//     provides durable S3-compatible object storage; the workflow engine wires
//     the bucket credentials into the container via env vars so the NATS server
//     can sync JetStream state to Spaces on shutdown / restore on startup.
//
// The infra.container_service Properties are consumed by workflow-plugin-digitalocean
// (infra.container_service resource driver). String-encoded values follow the
// canonical key schema expected by the driver's buildAppSpec helper:
//
//	image            – Docker Hub image reference including tag.
//	instance_count   – number of replicas (string-encoded int32).
//	run_command      – NATS server flags (JetStream, storage dir, monitoring).
//	internal_ports   – comma-separated list of exposed container ports.
//
// The infra.storage Properties are consumed by workflow-plugin-digitalocean
// (SpacesDriver). Relevant keys:
//
//	storage_size_bytes – optional maximum storage hint (from JetStreamConfig).
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
	ports := buildInternalPorts()

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
			"provider":      "nats",
			"deploy_target": string(cfg.GetDeployTarget()),
		},
	}

	resources := []iac.Resource{svc}

	// Emit a DigitalOcean Spaces bucket as the JetStream backing store when
	// JetStream is enabled. The bucket is realised by workflow-plugin-digitalocean's
	// SpacesDriver (infra.storage resource kind).
	js := cfg.GetJetstream()
	if js != nil && js.GetEnabled() {
		vol := buildJetStreamVolume(cfg.GetVersion(), js)
		resources = append(resources, vol)
	}

	return resources, nil
}

// buildJetStreamVolume constructs the infra.storage (DO Spaces) resource that
// backs JetStream persistence for the DO App Platform deploy target.
func buildJetStreamVolume(version string, js *eventbusv1.JetStreamConfig) iac.Resource {
	props := map[string]string{}
	if js.GetMaxStorageBytes() > 0 {
		props["storage_size_bytes"] = fmt.Sprintf("%d", js.GetMaxStorageBytes())
	}
	if version != "" {
		props["nats_version"] = version
	}

	return iac.Resource{
		Kind:       "infra.storage",
		Name:       "nats-jetstream",
		Properties: props,
		Labels: map[string]string{
			"provider": "nats",
			"purpose":  "jetstream",
		},
	}
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
//
//   - 4222 — NATS client connections (always required).
//   - 8222 — HTTP monitoring endpoint (health checks, JetStream stats).
//   - 6222 — NATS cluster routing (inter-node communication; always exposed
//     so the container can join a cluster without redeploy).
func buildInternalPorts() string {
	return strings.Join([]string{natsClientPort, natsMonitorPort, natsClusterPort}, ",")
}
