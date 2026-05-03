package nats_test

import (
	"strings"
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/iac"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
)

// TestNATSProvider_Name asserts the provider reports the correct identifier.
func TestNATSProvider_Name(t *testing.T) {
	p := nats.New()
	if got := p.Name(); got != "nats" {
		t.Errorf("Name() = %q, want %q", got, "nats")
	}
}

// TestNATSProvider_UnsupportedTarget asserts that Resources returns an error
// for a target not supported by the NATS provider.
func TestNATSProvider_UnsupportedTarget(t *testing.T) {
	p := nats.New()
	_, err := p.Resources(&eventbusv1.ClusterConfig{Version: "2.10"}, providers.TargetAWSKinesis)
	if err == nil {
		t.Fatal("expected error for unsupported target aws.kinesis, got nil")
	}
}

// TestNATSProvider_DOAppPlatform_EmitsContainerService asserts that Resources for
// TargetDigitalOceanApp emits at least one infra.container_service resource.
func TestNATSProvider_DOAppPlatform_EmitsContainerService(t *testing.T) {
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
	var found bool
	for _, r := range resources {
		if r.Kind == "infra.container_service" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("no infra.container_service resource emitted; got kinds: %v", resourceKinds(resources))
	}
}

// TestNATSProvider_DOAppPlatform_NATSImage asserts the NATS Docker image is set
// and includes the configured version.
func TestNATSProvider_DOAppPlatform_NATSImage(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 1,
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findByKind(resources, "infra.container_service")
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

// TestNATSProvider_DOAppPlatform_DefaultVersion asserts that an empty version
// defaults to a non-empty NATS image tag.
func TestNATSProvider_DOAppPlatform_DefaultVersion(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if svc.Properties["image"] == "" {
		t.Error("image property is empty for default version")
	}
}

// TestNATSProvider_DOAppPlatform_Replicas asserts that the ClusterConfig.Replicas
// field is mapped to the instance_count property.
func TestNATSProvider_DOAppPlatform_Replicas(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 3,
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if svc.Properties["instance_count"] != "3" {
		t.Errorf("instance_count = %q, want %q", svc.Properties["instance_count"], "3")
	}
}

// TestNATSProvider_DOAppPlatform_JetStreamEnabled asserts that enabling JetStream
// adds the -js flag to the run command.
func TestNATSProvider_DOAppPlatform_JetStreamEnabled(t *testing.T) {
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
	svc := findByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	runCmd := svc.Properties["run_command"]
	if !strings.Contains(runCmd, "-js") {
		t.Errorf("run_command %q does not contain JetStream flag -js", runCmd)
	}
}

// TestNATSProvider_DOAppPlatform_JetStreamDisabled asserts that when JetStream
// is not enabled, the -js flag is absent from the run command.
func TestNATSProvider_DOAppPlatform_JetStreamDisabled(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{
		Version:  "2.10",
		Replicas: 1,
	}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	runCmd := svc.Properties["run_command"]
	if strings.Contains(runCmd, "-js") {
		t.Errorf("run_command %q should not contain -js when JetStream is disabled", runCmd)
	}
}

// TestNATSProvider_DOAppPlatform_ClientPort asserts the NATS client port 4222
// appears in internal_ports.
func TestNATSProvider_DOAppPlatform_ClientPort(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	ports := svc.Properties["internal_ports"]
	if !strings.Contains(ports, "4222") {
		t.Errorf("internal_ports %q does not contain NATS client port 4222", ports)
	}
}

// TestNATSProvider_DOAppPlatform_Labels asserts provider/bus labels are set.
func TestNATSProvider_DOAppPlatform_Labels(t *testing.T) {
	p := nats.New()
	cfg := &eventbusv1.ClusterConfig{Version: "2.10", Replicas: 1}
	resources, err := p.Resources(cfg, providers.TargetDigitalOceanApp)
	if err != nil {
		t.Fatalf("Resources() error: %v", err)
	}
	svc := findByKind(resources, "infra.container_service")
	if svc == nil {
		t.Fatal("no infra.container_service resource emitted")
	}
	if svc.Labels["provider"] != "nats" {
		t.Errorf("label provider = %q, want %q", svc.Labels["provider"], "nats")
	}
}

// TestNATSProvider_ConnectionString_ErrorsWithoutURI asserts ConnectionString
// returns an error when the state does not contain a uri output.
func TestNATSProvider_ConnectionString_ErrorsWithoutURI(t *testing.T) {
	p := nats.New()
	_, err := p.ConnectionString(iac.State{Outputs: map[string]iac.Output{}}, "prod")
	if err == nil {
		t.Fatal("expected error when uri is absent from state, got nil")
	}
}

// TestNATSProvider_StreamResources_NilStreamList asserts StreamResources with
// a nil slice returns an empty list without error.
func TestNATSProvider_StreamResources_NilStreamList(t *testing.T) {
	p := nats.New()
	res, err := p.StreamResources(nil, iac.State{})
	if err != nil {
		t.Fatalf("StreamResources(nil) error: %v", err)
	}
	if len(res) != 0 {
		t.Errorf("StreamResources(nil) returned %d resources, want 0", len(res))
	}
}

// TestNATSProvider_Probe_UnreachableOnEmptyURI asserts Probe returns
// HealthStatusUnreachable for an empty URI (not a real broker).
func TestNATSProvider_Probe_UnreachableOnEmptyURI(t *testing.T) {
	p := nats.New()
	hc := p.Probe("")
	if hc.Status != providers.HealthStatusUnreachable {
		t.Errorf("Probe() status = %q, want %q", hc.Status, providers.HealthStatusUnreachable)
	}
	if hc.Err == nil {
		t.Error("Probe() Err should be non-nil for empty URI")
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func findByKind(resources []iac.Resource, kind string) *iac.Resource {
	for i := range resources {
		if resources[i].Kind == kind {
			return &resources[i]
		}
	}
	return nil
}

func resourceKinds(resources []iac.Resource) []string {
	kinds := make([]string, len(resources))
	for i, r := range resources {
		kinds[i] = r.Kind
	}
	return kinds
}
