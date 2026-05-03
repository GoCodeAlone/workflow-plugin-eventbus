package nats

import (
	"fmt"
	"strings"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
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

// natsJetStreamStorageName is the canonical name for the JetStream infra.storage
// resource emitted alongside infra.container_service when JetStream is enabled.
// The container service references this name via the storage_ref property so the
// workflow engine can inject Spaces credentials as env vars at provisioning time.
const natsJetStreamStorageName = "nats-jetstream"

// resourcesForDOApp emits the IaC resource declarations required to run a NATS
// server (optionally with JetStream) on DigitalOcean App Platform.
//
// Emitted resources:
//   - infra.container_service — the NATS server process (always).
//   - infra.storage           — a DigitalOcean Spaces bucket for JetStream
//     persistence (emitted only when JetStream is enabled). The container_service
//     references the bucket by name via its storage_ref property; the workflow
//     engine uses this edge to inject Spaces credentials as env vars so the
//     NATS server can access the bucket at runtime.
//
// The infra.container_service Properties are consumed by workflow-plugin-digitalocean
// (infra.container_service resource driver). String-encoded values follow the
// canonical key schema expected by the driver's buildAppSpec helper:
//
//	image            – Docker Hub image reference including tag.
//	instance_count   – number of replicas (string-encoded int32).
//	run_command      – NATS server flags (JetStream, storage dir, monitoring, cluster).
//	internal_ports   – comma-separated list of exposed container ports.
//	storage_ref      – name of the infra.storage resource to link (JetStream only).
//
// The infra.storage Properties are consumed by workflow-plugin-digitalocean
// (SpacesDriver). Relevant keys:
//
//	storage_size_bytes – optional maximum storage hint (from JetStreamConfig).
//
// Returns an error if cfg.Version is "latest" (unpinned versions are rejected
// to ensure reproducible deployments).
func resourcesForDOApp(cfg *eventbusv1.ClusterConfig) ([]iac.Resource, error) {
	version := cfg.GetVersion()
	if version == "" {
		version = defaultVersion
	}
	if strings.EqualFold(version, "latest") {
		return nil, fmt.Errorf("nats: Version %q is not allowed; specify a pinned version tag (e.g. %q)", version, defaultVersion)
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
			"deploy_target": string(providers.TargetDigitalOceanApp),
		},
	}

	resources := []iac.Resource{svc}

	// Emit a DigitalOcean Spaces bucket as the JetStream backing store when
	// JetStream is enabled. The bucket is realised by workflow-plugin-digitalocean's
	// SpacesDriver (infra.storage resource kind). The container_service carries a
	// storage_ref property pointing at the bucket name so the workflow engine can
	// wire Spaces credentials into the container's env at provisioning time.
	js := cfg.GetJetstream()
	if js != nil && js.GetEnabled() {
		// Link the container service to the storage resource explicitly.
		resources[0].Properties["storage_ref"] = natsJetStreamStorageName

		vol := buildJetStreamVolume(js)
		resources = append(resources, vol)
	}

	return resources, nil
}

// buildJetStreamVolume constructs the infra.storage (DO Spaces) resource that
// backs JetStream persistence for the DO App Platform deploy target.
func buildJetStreamVolume(js *eventbusv1.JetStreamConfig) iac.Resource {
	props := map[string]string{}
	if js.GetMaxStorageBytes() > 0 {
		props["storage_size_bytes"] = fmt.Sprintf("%d", js.GetMaxStorageBytes())
	}

	return iac.Resource{
		Kind:       "infra.storage",
		Name:       natsJetStreamStorageName,
		Properties: props,
		Labels: map[string]string{
			"provider": "nats",
			"purpose":  "jetstream",
		},
	}
}

// buildRunCommand constructs the NATS server command-line flags from ClusterConfig.
//
//   - HTTP monitoring is always enabled on natsMonitorPort.
//   - When JetStream is enabled, -js and -sd flags are added together with
//     optional storage and memory limits.
//   - Cluster routing (--cluster) is always enabled so replicas can be added
//     without a run_command change (zero-config scale-up).
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

	// Always enable cluster routing so instances can join a cluster without a
	// run_command change (matching the always-exposed port 6222).
	flags = append(flags, fmt.Sprintf("--cluster nats://0.0.0.0:%s", natsClusterPort))

	return strings.Join(flags, " ")
}

// buildInternalPorts returns the comma-separated list of container ports to
// expose as internal (non-public) App Platform ports.
//
//   - 4222 — NATS client connections (always required).
//   - 8222 — HTTP monitoring endpoint (health checks, JetStream stats).
//   - 6222 — NATS cluster routing (always exposed; matches always-on --cluster flag).
func buildInternalPorts() string {
	return strings.Join([]string{natsClientPort, natsMonitorPort, natsClusterPort}, ",")
}
