// Package eventbus_test provides end-to-end integration tests that start the
// workflow-plugin-eventbus binary as a real subprocess, communicate with it over
// the gRPC transport (via go-plugin), and verify the full typed-proto contract
// surface.
//
// Two test functions:
//
//   - TestE2E_EventbusPluginScenario — always runs; exercises gRPC transport,
//     manifest, contract registry, module lifecycle, and step error paths
//     without a live NATS server.
//
//   - TestE2E_EventbusPluginScenario_WithNATS — gated on INTEGRATION_NATS=1;
//     requires a running NATS server with JetStream (NATS_URL env var).
//     Publishes 10 messages, consumes all 10, and acks each one.
package eventbus_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	goplugin "github.com/GoCodeAlone/go-plugin"
	"github.com/nats-io/nats.go"
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
// pb.PluginServiceClient connected to it over gRPC. The subprocess inherits the
// test process's environment (including NATS_URL when set).
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

// ── helper: declare a module over gRPC (Create → Init → Start) ───────────────

// declareModule sends CreateModule → InitModule → StartModule and registers a
// t.Cleanup that sends StopModule. Returns the module handle.
func declareModule(t *testing.T, ctx context.Context, pbClient pb.PluginServiceClient, typeName, name string, cfg *anypb.Any) string {
	t.Helper()

	createResp, err := pbClient.CreateModule(ctx, &pb.CreateModuleRequest{
		Type:        typeName,
		Name:        name,
		TypedConfig: cfg,
	})
	mustNoRPCErr(t, fmt.Sprintf("CreateModule(%s)", typeName), err, createResp.GetError())
	handle := createResp.HandleId

	initResp, err := pbClient.InitModule(ctx, &pb.HandleRequest{HandleId: handle})
	mustNoRPCErr(t, fmt.Sprintf("InitModule(%s)", typeName), err, initResp.GetError())

	startResp, err := pbClient.StartModule(ctx, &pb.HandleRequest{HandleId: handle})
	mustNoRPCErr(t, fmt.Sprintf("StartModule(%s)", typeName), err, startResp.GetError())

	t.Cleanup(func() {
		resp, err := pbClient.StopModule(ctx, &pb.HandleRequest{HandleId: handle})
		if err != nil {
			t.Logf("StopModule(%s): gRPC error: %v", typeName, err)
		} else if resp.GetError() != "" {
			t.Logf("StopModule(%s): plugin error: %s", typeName, resp.GetError())
		}
	})

	return handle
}

// ── TestE2E_EventbusPluginScenario ───────────────────────────────────────────

// TestE2E_EventbusPluginScenario verifies the full gRPC contract surface without
// a live NATS server. It always runs (no skip gate).
//
// Verifies:
//   - Manifest name and author
//   - Contract registry: all 7 strict-proto descriptors present
//   - eventbus.broker module lifecycle (Create → Init → Start → Stop)
//   - step.eventbus.publish: descriptive error when no broker URI registered
//   - step.eventbus.consume: descriptive error when consumer not registered
//   - step.eventbus.ack: descriptive error when ack_token is empty
//   - trigger.eventbus.subscribe module lifecycle
//   - GetModuleTypes / GetStepTypes / GetTriggerTypes RPC coverage
func TestE2E_EventbusPluginScenario(t *testing.T) {
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
	for _, want := range []string{"eventbus.broker", "eventbus.stream", "eventbus.consumer"} {
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
	for _, c := range registry.GetContracts() {
		if c.GetKind() == pb.ContractKind_CONTRACT_KIND_STEP {
			if c.GetMode() != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
				t.Errorf("contract %q mode = %v, want STRICT_PROTO", c.GetStepType(), c.GetMode())
			}
		}
	}

	// ── 4. Declare eventbus.broker cluster module ─────────────────────────────
	packedClusterCfg, err := anypb.New(&eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	})
	if err != nil {
		t.Fatalf("pack ClusterConfig: %v", err)
	}
	declareModule(t, ctx, pbClient, "eventbus.broker", "e2e-bus", packedClusterCfg)

	// ── 5. step.eventbus.publish — no broker URI registered ───────────────────
	createPublishResp, err := pbClient.CreateStep(ctx, &pb.CreateStepRequest{
		Type: "step.eventbus.publish",
		Name: "e2e-publish",
	})
	mustNoRPCErr(t, "CreateStep(publish)", err, createPublishResp.GetError())

	publishInput, err := anypb.New(&eventbusv1.PublishRequest{
		Subject: "BMW.FULFILLMENT.ORDERS",
		Payload: []byte(`{"vin":"WBA3A5C50DF456789","status":"ORDER_PLACED"}`),
	})
	if err != nil {
		t.Fatalf("pack PublishRequest: %v", err)
	}
	execPublishResp, err := pbClient.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		HandleId:   createPublishResp.HandleId,
		TypedInput: publishInput,
	})
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
	packedConsumerCfg, err := anypb.New(&eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	})
	if err != nil {
		t.Fatalf("pack ConsumerConfig for trigger: %v", err)
	}
	declareModule(t, ctx, pbClient, "trigger.eventbus.subscribe", "e2e-trigger", packedConsumerCfg)

	// ── 9. GetModuleTypes / GetStepTypes / GetTriggerTypes ────────────────────
	modTypes, err := pbClient.GetModuleTypes(ctx, &emptypb.Empty{})
	mustNoRPCErr(t, "GetModuleTypes", err, "")
	wantModTypes := map[string]bool{
		"eventbus.broker":            false,
		"eventbus.stream":            false,
		"eventbus.consumer":          false,
		"trigger.eventbus.subscribe": false,
	}
	for _, typ := range modTypes.GetTypes() {
		wantModTypes[typ] = true
	}
	for typ, found := range wantModTypes {
		if !found {
			t.Errorf("GetModuleTypes: missing %q", typ)
		}
	}

	stepTypeList, err := pbClient.GetStepTypes(ctx, &emptypb.Empty{})
	mustNoRPCErr(t, "GetStepTypes", err, "")
	wantStepTypes := map[string]bool{
		"step.eventbus.publish": false,
		"step.eventbus.consume": false,
		"step.eventbus.ack":     false,
	}
	for _, typ := range stepTypeList.GetTypes() {
		wantStepTypes[typ] = true
	}
	for typ, found := range wantStepTypes {
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

// ── TestE2E_EventbusPluginScenario_WithNATS ───────────────────────────────────

// TestE2E_EventbusPluginScenario_WithNATS exercises the full publish → consume
// → ack pipeline against a live NATS server with JetStream.
//
// Gate: INTEGRATION_NATS=1 must be set. NATS_URL must contain the broker URI.
//
// The test:
//  1. Connects directly to NATS to pre-create the JetStream stream + durable consumer.
//  2. Builds + starts the plugin binary as a subprocess; the subprocess inherits
//     NATS_URL from the test process environment, which the eventbus.broker module
//     resolves during Init.
//  3. Declares eventbus.broker, eventbus.stream, and eventbus.consumer
//     modules via gRPC (Create → Init → Start).
//  4. Publishes 10 messages via step.eventbus.publish.
//  5. Consumes all 10 in a single batch via step.eventbus.consume (batch_size=10).
//  6. Acks each message via step.eventbus.ack using the ack_token from the response.
func TestE2E_EventbusPluginScenario_WithNATS(t *testing.T) {
	if os.Getenv("INTEGRATION_NATS") != "1" {
		t.Skip("skipping NATS integration test: set INTEGRATION_NATS=1 and NATS_URL to run")
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		t.Fatal("INTEGRATION_NATS=1 but NATS_URL is not set")
	}

	const (
		streamName    = "BMW_FULFILLMENT"
		streamSubject = "BMW.FULFILLMENT.>"
		publishSubj   = "BMW.FULFILLMENT.ORDERS"
		consumerName  = "bmw-fulfillment-handler"
		numMessages   = 10
	)

	ctx := context.Background()

	// ── 1. Pre-create JetStream stream + durable consumer in test process ─────
	// The stream and consumer modules only register config in the plugin — they
	// do not provision resources on the broker. We create them directly here so
	// the publish and consume steps work against a live stream.
	nc, err := nats.Connect(natsURL, nats.Timeout(10*time.Second))
	if err != nil {
		t.Fatalf("connect to NATS at %s: %v", natsURL, err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream context: %v", err)
	}

	// Create or update the stream.
	if _, err := js.StreamInfo(streamName); err != nil {
		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubject},
		}); err != nil {
			t.Fatalf("create JetStream stream %q: %v", streamName, err)
		}
	}
	t.Cleanup(func() {
		if err := js.DeleteStream(streamName); err != nil {
			t.Logf("delete stream %q: %v", streamName, err)
		}
	})

	// Create or update the durable consumer.
	if _, err := js.ConsumerInfo(streamName, consumerName); err != nil {
		if _, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:    consumerName,
			AckPolicy:  nats.AckExplicitPolicy,
			MaxDeliver: 3,
		}); err != nil {
			t.Fatalf("create durable consumer %q: %v", consumerName, err)
		}
	}

	// ── 2. Build + start plugin subprocess ────────────────────────────────────
	// NATS_URL is inherited from the test process environment.
	binaryPath := buildBinary(t)
	pbClient := startPlugin(t, binaryPath)

	// ── 3. Declare modules via gRPC ───────────────────────────────────────────
	packedClusterCfg, err := anypb.New(&eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
	})
	if err != nil {
		t.Fatalf("pack ClusterConfig: %v", err)
	}
	declareModule(t, ctx, pbClient, "eventbus.broker", "nats-bus", packedClusterCfg)

	packedStreamCfg, err := anypb.New(&eventbusv1.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamSubject},
	})
	if err != nil {
		t.Fatalf("pack StreamConfig: %v", err)
	}
	declareModule(t, ctx, pbClient, "eventbus.stream", "bmw-fulfillment-stream", packedStreamCfg)

	packedConsumerCfg, err := anypb.New(&eventbusv1.ConsumerConfig{
		Name:       consumerName,
		StreamName: streamName,
	})
	if err != nil {
		t.Fatalf("pack ConsumerConfig: %v", err)
	}
	declareModule(t, ctx, pbClient, "eventbus.consumer", "bmw-fulfillment-consumer", packedConsumerCfg)

	// ── 4. Publish 10 messages via step.eventbus.publish ─────────────────────
	createPublishResp, err := pbClient.CreateStep(ctx, &pb.CreateStepRequest{
		Type: "step.eventbus.publish",
		Name: "nats-publish",
	})
	mustNoRPCErr(t, "CreateStep(publish)", err, createPublishResp.GetError())
	publishHandle := createPublishResp.HandleId

	for i := 1; i <= numMessages; i++ {
		publishInput, err := anypb.New(&eventbusv1.PublishRequest{
			Subject: publishSubj,
			Payload: []byte(fmt.Sprintf(`{"n":%d,"event":"ORDER_PLACED"}`, i)),
		})
		if err != nil {
			t.Fatalf("pack PublishRequest %d: %v", i, err)
		}
		execResp, err := pbClient.ExecuteStep(ctx, &pb.ExecuteStepRequest{
			HandleId:   publishHandle,
			TypedInput: publishInput,
		})
		mustNoRPCErr(t, fmt.Sprintf("ExecuteStep(publish) msg %d", i), err, execResp.GetError())

		var out eventbusv1.PublishResponse
		if err := execResp.GetTypedOutput().UnmarshalTo(&out); err != nil {
			t.Fatalf("unpack PublishResponse msg %d: %v", i, err)
		}
		if out.GetSequence() == "" {
			t.Errorf("publish msg %d: expected non-empty sequence", i)
		}
		if out.GetAckedAt() == "" {
			t.Errorf("publish msg %d: expected non-empty acked_at", i)
		}
	}

	// ── 5. Consume all 10 in a single batch via step.eventbus.consume ─────────
	createConsumeResp, err := pbClient.CreateStep(ctx, &pb.CreateStepRequest{
		Type: "step.eventbus.consume",
		Name: "nats-consume",
	})
	mustNoRPCErr(t, "CreateStep(consume)", err, createConsumeResp.GetError())

	consumeInput, err := anypb.New(&eventbusv1.ConsumeRequest{
		Consumer:  consumerName,
		BatchSize: numMessages,
	})
	if err != nil {
		t.Fatalf("pack ConsumeRequest: %v", err)
	}
	execConsumeResp, err := pbClient.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		HandleId:   createConsumeResp.HandleId,
		TypedInput: consumeInput,
	})
	mustNoRPCErr(t, "ExecuteStep(consume)", err, execConsumeResp.GetError())

	var consumeOut eventbusv1.ConsumeResponse
	if err := execConsumeResp.GetTypedOutput().UnmarshalTo(&consumeOut); err != nil {
		t.Fatalf("unpack ConsumeResponse: %v", err)
	}
	msgs := consumeOut.GetMessages()
	if len(msgs) != numMessages {
		t.Fatalf("consume: got %d messages, want %d", len(msgs), numMessages)
	}

	// ── 6. Ack each message via step.eventbus.ack ─────────────────────────────
	createAckResp, err := pbClient.CreateStep(ctx, &pb.CreateStepRequest{
		Type: "step.eventbus.ack",
		Name: "nats-ack",
	})
	mustNoRPCErr(t, "CreateStep(ack)", err, createAckResp.GetError())
	ackHandle := createAckResp.HandleId

	for i, msg := range msgs {
		if msg.GetAckToken() == "" {
			t.Errorf("message %d: ack_token is empty", i)
			continue
		}
		ackInput, err := anypb.New(&eventbusv1.AckRequest{AckToken: msg.GetAckToken()})
		if err != nil {
			t.Fatalf("pack AckRequest for msg %d: %v", i, err)
		}
		execAckResp, err := pbClient.ExecuteStep(ctx, &pb.ExecuteStepRequest{
			HandleId:   ackHandle,
			TypedInput: ackInput,
		})
		mustNoRPCErr(t, fmt.Sprintf("ExecuteStep(ack) msg %d", i), err, execAckResp.GetError())
	}
	t.Logf("published %d messages, consumed %d, acked %d", numMessages, len(msgs), len(msgs))
}
