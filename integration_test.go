// Package eventbus_test provides an end-to-end integration test that starts
// the workflow-plugin-eventbus binary as a real subprocess, communicates with
// it over the gRPC transport (via go-plugin), and verifies the full typed-proto
// contract surface: module lifecycle, step execution, contract registry, and
// trigger module creation.
//
// The test:
//  1. Compiles and starts the plugin binary as a subprocess via go-plugin.
//  2. Fetches the manifest and contract registry over gRPC.
//  3. Declares an infra.eventbus cluster module (Create → Init → Start → Stop).
//  4. Creates and attempts to execute step.eventbus.publish — expects a
//     descriptive error about no URI (no live NATS server required).
//  5. Creates and attempts to execute step.eventbus.consume — expects a
//     descriptive error about no consumer registered.
//  6. Creates and attempts to execute step.eventbus.ack — expects a
//     descriptive error about empty ack_token.
//  7. Creates a trigger.eventbus.subscribe module and verifies it can be
//     initialised and stopped without error.
//
// Run with -short to skip (requires the Go toolchain to compile the binary).
package eventbus_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	goplugin "github.com/GoCodeAlone/go-plugin"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	ext "github.com/GoCodeAlone/workflow/plugin/external"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
)

// ── go-plugin bridge ──────────────────────────────────────────────────────────

// testGRPCPlugin is a go-plugin Plugin implementation that dispenses
// pb.PluginServiceClient directly, bypassing the ext.PluginClient wrapper so the
// test can call RPC methods directly without depending on unexported types.
type testGRPCPlugin struct{ goplugin.Plugin }

func (p *testGRPCPlugin) GRPCServer(_ *goplugin.GRPCBroker, _ *grpc.Server) error { return nil }

func (p *testGRPCPlugin) GRPCClient(_ context.Context, _ *goplugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return pb.NewPluginServiceClient(c), nil
}

// ── test infrastructure ───────────────────────────────────────────────────────

// buildBinary compiles the plugin binary into a temp directory and returns its
// path.
func buildBinary(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("runtime.Caller unavailable")
	}
	projectRoot := filepath.Dir(thisFile)

	out := filepath.Join(t.TempDir(), "workflow-plugin-eventbus")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/workflow-plugin-eventbus/")
	cmd.Dir = projectRoot
	cmd.Env = append(os.Environ(), "GOWORK=off")

	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build plugin binary:\n%s\nerror: %v", output, err)
	}
	return out
}

// startPlugin starts the plugin binary as a go-plugin subprocess and returns a
// pb.PluginServiceClient connected to it over gRPC.
func startPlugin(t *testing.T, binaryPath string) pb.PluginServiceClient {
	t.Helper()

	client := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig:  ext.Handshake,
		Plugins:          goplugin.PluginSet{"plugin": &testGRPCPlugin{}},
		Cmd:              exec.Command(binaryPath), //nolint:gosec // G204: test binary
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
	})
	t.Cleanup(client.Kill)

	rpcClient, err := client.Client()
	if err != nil {
		t.Fatalf("connect to plugin subprocess: %v", err)
	}

	raw, err := rpcClient.Dispense("plugin")
	if err != nil {
		t.Fatalf("dispense plugin: %v", err)
	}

	pbClient, ok := raw.(pb.PluginServiceClient)
	if !ok {
		t.Fatalf("dispensed object is not pb.PluginServiceClient (got %T)", raw)
	}
	return pbClient
}

// mustNoRPCErr fatals the test if err != nil or the response error field is set.
func mustNoRPCErr(t *testing.T, label string, err error, respErr string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: gRPC error: %v", label, err)
	}
	if respErr != "" {
		t.Fatalf("%s: plugin error: %s", label, respErr)
	}
}

// ── integration scenario ──────────────────────────────────────────────────────

// TestE2E_EventbusPluginScenario is the canonical end-to-end integration test.
//
// All calls go through real gRPC proto serialisation: the test process packs
// each request as anypb.Any, sends it over a TCP gRPC connection to the plugin
// subprocess, and unpacks the typed response.
//
// No live NATS server is required — the test deliberately exercises the error
// paths that fire when no broker is reachable, verifying that error messages
// are descriptive and the plugin remains stable under those conditions.
//
// Requires the Go toolchain to compile the plugin binary. Run with -short to skip.
func TestE2E_EventbusPluginScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test: requires Go toolchain (run without -short)")
	}

	ctx := context.Background()

	// ── 1. Build and start plugin subprocess ──────────────────────────────────
	binaryPath := buildBinary(t)
	pbClient := startPlugin(t, binaryPath)

	// ── 2. Manifest verification ──────────────────────────────────────────────
	manifest, err := pbClient.GetManifest(ctx, &emptypb.Empty{})
	mustNoRPCErr(t, "GetManifest", err, "")
	if manifest.GetName() != "workflow-plugin-eventbus" {
		t.Errorf("manifest name = %q, want %q", manifest.GetName(), "workflow-plugin-eventbus")
	}
	if manifest.GetAuthor() != "GoCodeAlone" {
		t.Errorf("manifest author = %q, want %q", manifest.GetAuthor(), "GoCodeAlone")
	}

	// ── 3. Contract registry verification ─────────────────────────────────────
	registry, err := pbClient.GetContractRegistry(ctx, &emptypb.Empty{})
	mustNoRPCErr(t, "GetContractRegistry", err, "")

	// Collect type names by kind to verify all expected contracts are present.
	moduleTypes := make(map[string]bool)
	stepTypes := make(map[string]bool)
	triggerTypes := make(map[string]bool)
	for _, c := range registry.GetContracts() {
		switch c.GetKind() {
		case pb.ContractKind_CONTRACT_KIND_MODULE:
			moduleTypes[c.GetModuleType()] = true
		case pb.ContractKind_CONTRACT_KIND_STEP:
			stepTypes[c.GetStepType()] = true
		case pb.ContractKind_CONTRACT_KIND_TRIGGER:
			triggerTypes[c.GetTriggerType()] = true
		}
	}

	for _, want := range []string{"infra.eventbus", "infra.eventbus.stream", "infra.eventbus.consumer"} {
		if !moduleTypes[want] {
			t.Errorf("contract registry missing module type %q", want)
		}
	}
	for _, want := range []string{"step.eventbus.publish", "step.eventbus.consume", "step.eventbus.ack"} {
		if !stepTypes[want] {
			t.Errorf("contract registry missing step type %q", want)
		}
	}
	if !triggerTypes["trigger.eventbus.subscribe"] {
		t.Error("contract registry missing trigger type trigger.eventbus.subscribe")
	}

	// Verify all step contracts carry strict-proto mode.
	for _, c := range registry.GetContracts() {
		if c.GetKind() == pb.ContractKind_CONTRACT_KIND_STEP {
			if c.GetMode() != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
				t.Errorf("contract %q mode = %v, want STRICT_PROTO", c.GetStepType(), c.GetMode())
			}
		}
	}

	// ── 4. Declare infra.eventbus cluster module via gRPC ─────────────────────
	clusterCfg := &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	}
	packedClusterCfg, err := anypb.New(clusterCfg)
	if err != nil {
		t.Fatalf("pack ClusterConfig: %v", err)
	}

	createModResp, err := pbClient.CreateModule(ctx, &pb.CreateModuleRequest{
		Type:        "infra.eventbus",
		Name:        "e2e-bus",
		TypedConfig: packedClusterCfg,
	})
	mustNoRPCErr(t, "CreateModule(infra.eventbus)", err, createModResp.GetError())
	modHandle := createModResp.HandleId

	initResp, err := pbClient.InitModule(ctx, &pb.HandleRequest{HandleId: modHandle})
	mustNoRPCErr(t, "InitModule", err, initResp.GetError())

	startResp, err := pbClient.StartModule(ctx, &pb.HandleRequest{HandleId: modHandle})
	mustNoRPCErr(t, "StartModule", err, startResp.GetError())

	t.Cleanup(func() {
		if resp, err := pbClient.StopModule(ctx, &pb.HandleRequest{HandleId: modHandle}); err != nil {
			t.Logf("StopModule: gRPC error: %v", err)
		} else if resp.GetError() != "" {
			t.Logf("StopModule: plugin error: %s", resp.GetError())
		}
	})

	// ── 5. step.eventbus.publish — no broker URI registered ───────────────────
	// Cluster has no URI (no env var set) → expect a descriptive error from the
	// plugin, not a gRPC transport error.
	createPublishResp, err := pbClient.CreateStep(ctx, &pb.CreateStepRequest{
		Type: "step.eventbus.publish",
		Name: "e2e-publish",
	})
	mustNoRPCErr(t, "CreateStep(publish)", err, createPublishResp.GetError())

	publishInput, err := anypb.New(&eventbusv1.PublishRequest{
		Subject: "BMW.FULFILLMENT.EVENTS",
		Payload: []byte(`{"vin":"WBA3A5C50DF456789","status":"ORDER_PLACED"}`),
	})
	if err != nil {
		t.Fatalf("pack PublishRequest: %v", err)
	}
	execPublishResp, err := pbClient.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		HandleId:   createPublishResp.HandleId,
		TypedInput: publishInput,
	})
	// Transport must succeed; the plugin error lives in the response field.
	if err != nil {
		t.Fatalf("ExecuteStep(publish): gRPC transport error: %v", err)
	}
	if execPublishResp.GetError() == "" {
		t.Error("ExecuteStep(publish): expected plugin error (no NATS URI) but got none")
	}
	t.Logf("ExecuteStep(publish) expected plugin error: %s", execPublishResp.GetError())

	// ── 6. step.eventbus.consume — no consumer registered ────────────────────
	createConsumeResp, err := pbClient.CreateStep(ctx, &pb.CreateStepRequest{
		Type: "step.eventbus.consume",
		Name: "e2e-consume",
	})
	mustNoRPCErr(t, "CreateStep(consume)", err, createConsumeResp.GetError())

	consumeInput, err := anypb.New(&eventbusv1.ConsumeRequest{
		Consumer:  "bmw-fulfillment-handler",
		BatchSize: 10,
	})
	if err != nil {
		t.Fatalf("pack ConsumeRequest: %v", err)
	}
	execConsumeResp, err := pbClient.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		HandleId:   createConsumeResp.HandleId,
		TypedInput: consumeInput,
	})
	if err != nil {
		t.Fatalf("ExecuteStep(consume): gRPC transport error: %v", err)
	}
	if execConsumeResp.GetError() == "" {
		t.Error("ExecuteStep(consume): expected plugin error (consumer not registered) but got none")
	}
	t.Logf("ExecuteStep(consume) expected plugin error: %s", execConsumeResp.GetError())

	// ── 7. step.eventbus.ack — empty ack_token ────────────────────────────────
	createAckResp, err := pbClient.CreateStep(ctx, &pb.CreateStepRequest{
		Type: "step.eventbus.ack",
		Name: "e2e-ack",
	})
	mustNoRPCErr(t, "CreateStep(ack)", err, createAckResp.GetError())

	ackInput, err := anypb.New(&eventbusv1.AckRequest{AckToken: ""})
	if err != nil {
		t.Fatalf("pack AckRequest: %v", err)
	}
	execAckResp, err := pbClient.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		HandleId:   createAckResp.HandleId,
		TypedInput: ackInput,
	})
	if err != nil {
		t.Fatalf("ExecuteStep(ack): gRPC transport error: %v", err)
	}
	if execAckResp.GetError() == "" {
		t.Error("ExecuteStep(ack): expected plugin error (empty ack_token) but got none")
	}
	t.Logf("ExecuteStep(ack) expected plugin error: %s", execAckResp.GetError())

	// ── 8. trigger.eventbus.subscribe — module lifecycle ─────────────────────
	// The trigger is created as a module in the gRPC path. cb is always nil in
	// the subprocess transport; Start is a no-op.
	consumerCfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	packedConsumerCfg, err := anypb.New(consumerCfg)
	if err != nil {
		t.Fatalf("pack ConsumerConfig: %v", err)
	}

	createTrigResp, err := pbClient.CreateModule(ctx, &pb.CreateModuleRequest{
		Type:        "trigger.eventbus.subscribe",
		Name:        "e2e-trigger",
		TypedConfig: packedConsumerCfg,
	})
	mustNoRPCErr(t, "CreateModule(trigger.eventbus.subscribe)", err, createTrigResp.GetError())
	trigHandle := createTrigResp.HandleId

	initTrigResp, err := pbClient.InitModule(ctx, &pb.HandleRequest{HandleId: trigHandle})
	mustNoRPCErr(t, "InitModule(trigger)", err, initTrigResp.GetError())

	startTrigResp, err := pbClient.StartModule(ctx, &pb.HandleRequest{HandleId: trigHandle})
	mustNoRPCErr(t, "StartModule(trigger)", err, startTrigResp.GetError())

	stopTrigResp, err := pbClient.StopModule(ctx, &pb.HandleRequest{HandleId: trigHandle})
	mustNoRPCErr(t, "StopModule(trigger)", err, stopTrigResp.GetError())

	// ── 9. GetModuleTypes / GetStepTypes / GetTriggerTypes ────────────────────
	modTypes, err := pbClient.GetModuleTypes(ctx, &emptypb.Empty{})
	mustNoRPCErr(t, "GetModuleTypes", err, "")
	expectedModTypes := map[string]bool{
		"infra.eventbus":              false,
		"infra.eventbus.stream":       false,
		"infra.eventbus.consumer":     false,
		"trigger.eventbus.subscribe":  false,
	}
	for _, typ := range modTypes.GetTypes() {
		expectedModTypes[typ] = true
	}
	for typ, found := range expectedModTypes {
		if !found {
			t.Errorf("GetModuleTypes: missing %q", typ)
		}
	}

	stepTypeList, err := pbClient.GetStepTypes(ctx, &emptypb.Empty{})
	mustNoRPCErr(t, "GetStepTypes", err, "")
	expectedStepTypes := map[string]bool{
		"step.eventbus.publish": false,
		"step.eventbus.consume": false,
		"step.eventbus.ack":     false,
	}
	for _, typ := range stepTypeList.GetTypes() {
		expectedStepTypes[typ] = true
	}
	for typ, found := range expectedStepTypes {
		if !found {
			t.Errorf("GetStepTypes: missing %q", typ)
		}
	}

	trigTypes, err := pbClient.GetTriggerTypes(ctx, &emptypb.Empty{})
	mustNoRPCErr(t, "GetTriggerTypes", err, "")
	hasTrigger := false
	for _, typ := range trigTypes.GetTypes() {
		if typ == "trigger.eventbus.subscribe" {
			hasTrigger = true
		}
	}
	if !hasTrigger {
		t.Error("GetTriggerTypes: missing trigger.eventbus.subscribe")
	}
}
