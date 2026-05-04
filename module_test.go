package eventbus_test

import (
	"context"
	"os"
	"testing"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

func TestNewClusterModule_ValidConfig(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, err := eventbus.NewClusterModule("bus-valid", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil module")
	}
}

func TestNewClusterModule_EmptyProvider(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		DeployTarget: "digitalocean.app_platform",
	}
	_, err := eventbus.NewClusterModule("bus-empty-provider", cfg)
	if err == nil {
		t.Fatal("expected error for empty provider")
	}
}

func TestNewClusterModule_EmptyDeployTarget(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider: "nats",
	}
	_, err := eventbus.NewClusterModule("bus-empty-target", cfg)
	if err == nil {
		t.Fatal("expected error for empty deploy_target")
	}
}

func TestNewClusterModule_UnsupportedProviderTarget(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "kinesis",
		DeployTarget: "digitalocean.app_platform", // kinesis only supports aws.kinesis
	}
	_, err := eventbus.NewClusterModule("bus-bad-combo", cfg)
	if err == nil {
		t.Fatal("expected error for unsupported provider × target combination")
	}
}

func TestClusterModule_InitRegistersConfig(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, err := eventbus.NewClusterModule("bus-init-reg", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	got, ok := eventbus.GetCluster("bus-init-reg")
	if !ok {
		t.Fatal("cluster not found in registry after Init")
	}
	if got.GetProvider() != "nats" {
		t.Errorf("provider = %q, want nats", got.GetProvider())
	}
}

func TestClusterModule_StopUnregisters(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, _ := eventbus.NewClusterModule("bus-stop-unreg", cfg)
	_ = m.Init()
	_ = m.Stop(context.Background())

	_, ok := eventbus.GetCluster("bus-stop-unreg")
	if ok {
		t.Fatal("cluster still in registry after Stop")
	}
}

func TestClusterModule_InitRegistersURIFromNATSURL(t *testing.T) {
	t.Setenv("NATS_URL", "nats://test-host:4222")

	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, _ := eventbus.NewClusterModule("bus-nats-url", cfg)
	_ = m.Init()
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	uri, ok := eventbus.GetBusURI("bus-nats-url")
	if !ok {
		t.Fatal("expected URI in registry when NATS_URL is set")
	}
	if uri != "nats://test-host:4222" {
		t.Errorf("uri = %q, want nats://test-host:4222", uri)
	}
}

func TestClusterModule_InitRegistersURIFromInstanceEnvVar(t *testing.T) {
	t.Setenv("EVENTBUS_BMW_EVENTBUS_URI", "nats://bmw-host:4222")

	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, _ := eventbus.NewClusterModule("bmw-eventbus", cfg)
	_ = m.Init()
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	uri, ok := eventbus.GetBusURI("bmw-eventbus")
	if !ok {
		t.Fatal("expected URI in registry when instance env var is set")
	}
	if uri != "nats://bmw-host:4222" {
		t.Errorf("uri = %q, want nats://bmw-host:4222", uri)
	}
}

func TestClusterModule_InitNoURIWhenEnvNotSet(t *testing.T) {
	// Ensure neither env var is set.
	os.Unsetenv("NATS_URL")
	os.Unsetenv("EVENTBUS_BUS_NO_URI_URI")

	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, _ := eventbus.NewClusterModule("bus-no-uri", cfg)
	_ = m.Init()
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	_, ok := eventbus.GetBusURI("bus-no-uri")
	if ok {
		t.Fatal("expected no URI in registry when env vars are absent")
	}
}
