package eventbus_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
)

// ── ClusterModuleFactory (TypedModuleProvider) ────────────────────────────────

func TestClusterModuleFactory_TypedModuleTypes(t *testing.T) {
	f := &eventbus.ClusterModuleFactory{}
	types := f.TypedModuleTypes()
	if len(types) != 1 || types[0] != "infra.eventbus" {
		t.Errorf("TypedModuleTypes() = %v, want [infra.eventbus]", types)
	}
}

func TestClusterModuleFactory_CreateTypedModule_WrongType(t *testing.T) {
	f := &eventbus.ClusterModuleFactory{}
	_, err := f.CreateTypedModule("infra.eventbus.stream", "x", nil)
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
}

func TestClusterModuleFactory_CreateTypedModule_NilConfig(t *testing.T) {
	f := &eventbus.ClusterModuleFactory{}
	// nil config → ClusterConfig zero value → empty provider → expect error
	_, err := f.CreateTypedModule("infra.eventbus", "bus-factory-nil", nil)
	if err == nil {
		t.Fatal("expected error from NewClusterModule for empty provider")
	}
}

// ── NewClusterModule validation ───────────────────────────────────────────────

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

// ── clusterModule lifecycle ───────────────────────────────────────────────────

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
	m, err := eventbus.NewClusterModule("bus-stop-unreg", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
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
	// Unset both vars only for the duration of this test, restoring any
	// pre-existing value on exit. Bare os.Unsetenv would permanently remove
	// NATS_URL from the process environment, breaking tests that run after.
	if prev, ok := os.LookupEnv("NATS_URL"); ok {
		t.Cleanup(func() { os.Setenv("NATS_URL", prev) })
		os.Unsetenv("NATS_URL")
	}
	if prev, ok := os.LookupEnv("EVENTBUS_BUS_NO_URI_URI"); ok {
		t.Cleanup(func() { os.Setenv("EVENTBUS_BUS_NO_URI_URI", prev) })
		os.Unsetenv("EVENTBUS_BUS_NO_URI_URI")
	}

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

// TestClusterModule_StopEvictsNATSConn verifies that Stop() evicts a connection
// pre-seeded into the cache (simulating a step or trigger that dialled on behalf
// of the module). Wire-level close behaviour is exercised by the integration test;
// here we only verify cache eviction using a nil sentinel entry.
func TestClusterModule_StopEvictsNATSConn(t *testing.T) {
	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	m, err := eventbus.NewClusterModule("bus-conn-evict", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = m.Init()

	// Pre-seed with a nil sentinel (nil is safe: closeNATSConn guards against nil).
	eventbus.RegisterNATSConn("bus-conn-evict", nil)

	_, inCache := eventbus.GetNATSConn("bus-conn-evict")
	if !inCache {
		t.Fatal("expected sentinel in cache before Stop")
	}

	_ = m.Stop(context.Background())

	_, inCacheAfter := eventbus.GetNATSConn("bus-conn-evict")
	if inCacheAfter {
		t.Fatal("expected sentinel evicted from cache after Stop")
	}
}

// ── broker instance registry + LookupRuntime ─────────────────────────────────
//
// Tests of LookupBrokerInstance / LookupRuntime semantics that don't need to
// hand-construct a *clusterModule (which is unexported) live here. Construction-
// dependent tests live in module_internal_test.go alongside the package types.

func TestBrokerInstanceRegistry_LookupNotFound(t *testing.T) {
	if m, ok := eventbus.LookupBrokerInstance("does-not-exist"); ok || m != nil {
		t.Fatalf("expected (nil, false) for unknown name; got (%v, %v)", m, ok)
	}
}

func TestLookupRuntime_NotRegistered(t *testing.T) {
	_, _, err := eventbus.LookupRuntime("not-registered-broker")
	if err == nil {
		t.Fatal("expected error for unregistered broker")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Errorf("error = %q, want substring \"not registered\"", err.Error())
	}
}

// ── Start runtime selection (legacy nats fallback) ───────────────────────────
//
// TestClusterModule_StartSelectsNats verifies the provider==nats branch of
// Start: a NATS URL is required (cfg.Dsn or NATS_URL env fallback). The test
// skips when no NATS broker is reachable so it can run on developer laptops
// without infrastructure.
func TestClusterModule_StartSelectsNats(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		t.Skip("NATS_URL not set; skipping live-broker Start test")
	}

	cfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
		Dsn:          natsURL,
	}
	m, err := eventbus.NewClusterModule("bus-start-nats", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	rt, conn, err := eventbus.LookupRuntime("bus-start-nats")
	if err != nil {
		t.Fatalf("LookupRuntime after Start: %v", err)
	}
	if rt == nil || conn == nil {
		t.Fatal("expected non-nil runtime + conn after Start")
	}
	if got := conn.Provider(); got != "nats" {
		t.Errorf("Connection.Provider() = %q, want \"nats\"", got)
	}
}

// TestClusterModule_StartSelectsPgchannel lives in module_test.go alongside
// the nats counterpart but depends on the per-provider validation relaxation
// (Task 9.5) — pgchannel configs omit deploy_target and require broker_target
// instead. The test is added in the validation-change commit.
