package nats_test

import (
	"strings"
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
)

// TestDOApp_EmitsContainerService asserts that Resources for TargetDigitalOceanApp
// emits at least one infra.container_service resource.
func TestDOApp_EmitsContainerService(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: string(providers.TargetDigitalOceanApp),
		Version:      "2.10",
		Replicas:     2,
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	if len(resources) == 0 {
		t.Fatal("Resources() returned empty slice")
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Errorf("no infra.container_service resource emitted; kinds: %v", resourceKindList(resources))
	}
}

// TestDOApp_NATSImage asserts the NATS Docker image includes the configured version.
func TestDOApp_NATSImage(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	img := svc.Properties["image"]
	if img == "" {
		t.Error("image property is empty")
	}
	if !strings.Contains(img, "2.10") {
		t.Errorf("image %q does not contain version 2.10", img)
	}
}

// TestDOApp_DefaultVersion asserts that an empty version defaults to a non-empty image tag.
func TestDOApp_DefaultVersion(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if svc.Properties["image"] == "" {
		t.Error("image property is empty for default version")
	}
}

// TestDOApp_Replicas asserts that ClusterConfig.Replicas maps to instance_count.
func TestDOApp_Replicas(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 3}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if svc.Properties["instance_count"] != "3" {
		t.Errorf("instance_count = %q, want %q", svc.Properties["instance_count"], "3")
	}
}

// TestDOApp_JetStreamEnabled asserts that enabling JetStream adds -js to run_command.
func TestDOApp_JetStreamEnabled(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 1,
		Jetstream: &eventbusv1.JetStreamConfig{
			Enabled:         true,
			MaxStorageBytes: 10737418240,
		},
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	runCmd := svc.Properties["run_command"]
	if !strings.Contains(runCmd, "-js") {
		t.Errorf("run_command %q does not contain JetStream flag -js", runCmd)
	}
}

// TestDOApp_JetStreamDisabled asserts -js is absent when JetStream is not enabled.
func TestDOApp_JetStreamDisabled(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if strings.Contains(svc.Properties["run_command"], "-js") {
		t.Errorf("run_command %q should not contain -js when JetStream is disabled", svc.Properties["run_command"])
	}
}

// TestDOApp_ClientPort asserts NATS client port 4222 appears in internal_ports.
func TestDOApp_ClientPort(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if !strings.Contains(svc.Properties["internal_ports"], "4222") {
		t.Errorf("internal_ports %q does not contain NATS client port 4222", svc.Properties["internal_ports"])
	}
}

// TestDOApp_MonitorPort asserts NATS monitoring port 8222 appears in internal_ports.
func TestDOApp_MonitorPort(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if !strings.Contains(svc.Properties["internal_ports"], "8222") {
		t.Errorf("internal_ports %q does not contain monitoring port 8222", svc.Properties["internal_ports"])
	}
}

// TestDOApp_ClusterPort asserts NATS cluster routing port 6222 appears in
// internal_ports unconditionally (regardless of replica count).
func TestDOApp_ClusterPort(t *testing.T) {
	tests := []struct {
		name     string
		replicas int32
	}{
		{"single-replica", 1},
		{"multi-replica", 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := nats.New()
			cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: tc.replicas}
			resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
			if err != nil {
				t.Fatalf("Resources() error: %v", err)
			}
			svc := findResourceByKind(resources, "infra.container_service")
			if svc == nil {
				t.Fatal("no infra.container_service resource emitted")
			}
			if !strings.Contains(svc.Properties["internal_ports"], "6222") {
				t.Errorf("internal_ports %q does not contain cluster routing port 6222", svc.Properties["internal_ports"])
			}
		})
	}
}

// TestDOApp_Labels asserts provider and deploy_target labels are set correctly
// on the container service, regardless of whether DeployTarget is set in cfg.
func TestDOApp_Labels(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if svc.Labels["provider"] != "nats" {
		t.Errorf("label provider = %q, want %q", svc.Labels["provider"], "nats")
	}
	if svc.Labels["deploy_target"] != string(providers.TargetDigitalOceanApp) {
		t.Errorf("label deploy_target = %q, want %q", svc.Labels["deploy_target"], string(providers.TargetDigitalOceanApp))
	}
}

// TestDOApp_JetStreamVolume asserts that enabling JetStream emits an infra.storage
// resource (DigitalOcean Spaces bucket) as the JetStream backing store.
func TestDOApp_JetStreamVolume(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 1,
		Jetstream: &eventbusv1.JetStreamConfig{
			Enabled:         true,
			MaxStorageBytes: 10737418240,
		},
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	vol := findResourceByKind(resources, "infra.storage")
	if vol == nil {
		t.Fatalf("no infra.storage resource emitted when JetStream is enabled; kinds: %v", resourceKindList(resources))
	}
	if vol.Labels["purpose"] != "jetstream" {
		t.Errorf("infra.storage label purpose = %q, want %q", vol.Labels["purpose"], "jetstream")
	}
}

// TestDOApp_JetStreamVolumeAbsent asserts no infra.storage is emitted when
// JetStream is not enabled.
func TestDOApp_JetStreamVolumeAbsent(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	if vol := findResourceByKind(resources, "infra.storage"); vol != nil {
		t.Error("infra.storage resource should not be emitted when JetStream is disabled")
	}
}

// TestDOApp_JetStreamVolumeStorageSizeProperty asserts the infra.storage resource
// carries the storage_size_bytes property when MaxStorageBytes is set.
func TestDOApp_JetStreamVolumeStorageSizeProperty(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 1,
		Jetstream: &eventbusv1.JetStreamConfig{
			Enabled:         true,
			MaxStorageBytes: 53687091200, // 50 GB
		},
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	vol := findResourceByKind(resources, "infra.storage")
	if vol == nil {
		t.Fatal("no infra.storage resource emitted")
	}
	if vol.Properties["storage_size_bytes"] != "53687091200" {
		t.Errorf("storage_size_bytes = %q, want %q", vol.Properties["storage_size_bytes"], "53687091200")
	}
}

// TestDOApp_ClusterFlagAlwaysPresent asserts --cluster appears in run_command
// for both single-replica and multi-replica deployments (always-on for zero-config
// scale-up, matching the always-exposed port 6222).
func TestDOApp_ClusterFlagAlwaysPresent(t *testing.T) {
	tests := []struct {
		name     string
		replicas int32
	}{
		{"single-replica", 1},
		{"multi-replica", 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := nats.New()
			cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: tc.replicas}
			resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
			if err != nil {
				t.Fatalf("Resources() error: %v", err)
			}
			svc := findResourceByKind(resources, "infra.container_service")
			if svc == nil {
				t.Fatal("no infra.container_service resource emitted")
			}
			if !strings.Contains(svc.Properties["run_command"], "--cluster") {
				t.Errorf("run_command %q does not contain --cluster for %s deployment", svc.Properties["run_command"], tc.name)
			}
		})
	}
}

// TestDOApp_LatestVersionRejected asserts that Version "latest" (and case
// variants) returns a non-nil error — unpinned tags are not allowed.
func TestDOApp_LatestVersionRejected(t *testing.T) {
	variants := []string{"latest", "LATEST", "Latest"}
	for _, v := range variants {
		t.Run(v, func(t *testing.T) {
			p := nats.New()
			cfg := &eventbusv1.ClusterConfig{Version: v, Replicas: 1}
			_, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
			if err == nil {
				t.Errorf("expected error for Version %q, got nil", v)
			}
		})
	}
}

// TestDOApp_StorageRefLinksContainerToVolume asserts that when JetStream is
// enabled the container_service carries a storage_ref property whose value
// matches the name of the emitted infra.storage resource.
func TestDOApp_StorageRefLinksContainerToVolume(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 1,
		Jetstream: &eventbusv1.JetStreamConfig{
			Enabled:         true,
			MaxStorageBytes: 10737418240,
		},
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	vol := findResourceByKind(resources, "infra.storage")
	if vol == nil {
		t.Fatal("no infra.storage resource emitted")
	}
	storageRef := svc.Properties["storage_ref"]
	if storageRef == "" {
		t.Error("infra.container_service missing storage_ref property when JetStream is enabled")
	}
	if storageRef != vol.Name {
		t.Errorf("storage_ref %q does not match infra.storage name %q", storageRef, vol.Name)
	}
}

// TestDOApp_StorageRefAbsentWithoutJetStream asserts no storage_ref appears
// on the container service when JetStream is not enabled.
func TestDOApp_StorageRefAbsentWithoutJetStream(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findResourceByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if ref := svc.Properties["storage_ref"]; ref != "" {
		t.Errorf("storage_ref should be absent when JetStream is disabled, got %q", ref)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func findResourceByKind(resources []iac.Resource, kind string) *iac.Resource {
	for i := range resources {
		if resources[i].Kind == kind {
			return &resources[i]
		}
	}
	return nil
}

func resourceKindList(resources []iac.Resource) []string {
	kinds := make([]string, len(resources))
	for i, r := range resources {
		kinds[i] = r.Kind
	}
	return kinds
}
