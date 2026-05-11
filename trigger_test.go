package eventbus_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	eventbus "github.com/GoCodeAlone/workflow-plugin-eventbus"
	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ── SubscribeTriggerModuleFactory (TypedModuleProvider) ───────────────────────

func TestSubscribeTriggerModuleFactory_TypedModuleTypes(t *testing.T) {
	f := &eventbus.SubscribeTriggerModuleFactory{}
	types := f.TypedModuleTypes()
	if len(types) != 1 || types[0] != "trigger.eventbus.subscribe" {
		t.Errorf("TypedModuleTypes() = %v, want [trigger.eventbus.subscribe]", types)
	}
}

func TestSubscribeTriggerModuleFactory_CreateTypedModule_WrongType(t *testing.T) {
	f := &eventbus.SubscribeTriggerModuleFactory{}
	_, err := f.CreateTypedModule("infra.eventbus", "x", nil)
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
}

func TestSubscribeTriggerModuleFactory_CreateTypedModule_NilConfig(t *testing.T) {
	f := &eventbus.SubscribeTriggerModuleFactory{}
	// nil config → ConsumerConfig zero value → empty name → expect error
	_, err := f.CreateTypedModule("trigger.eventbus.subscribe", "trigger-factory-nil", nil)
	if err == nil {
		t.Fatal("expected error from NewSubscribeTrigger for empty name")
	}
}

// ── NewSubscribeTrigger validation ────────────────────────────────────────────

func TestNewSubscribeTrigger_ValidConfig(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	m, err := eventbus.NewSubscribeTrigger("trigger-valid", cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil module")
	}
}

func TestNewSubscribeTrigger_EmptyName(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		StreamName: "BMW_FULFILLMENT",
	}
	_, err := eventbus.NewSubscribeTrigger("trigger-empty-name", cfg, nil)
	if err == nil {
		t.Fatal("expected error for empty consumer name")
	}
}

func TestNewSubscribeTrigger_EmptyStreamName(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name: "bmw-fulfillment-handler",
	}
	_, err := eventbus.NewSubscribeTrigger("trigger-empty-stream", cfg, nil)
	if err == nil {
		t.Fatal("expected error for empty stream_name")
	}
}

// ── subscribeTrigger lifecycle (nil callback — external plugin path) ──────────

// TestSubscribeTrigger_LifecycleNilCallback verifies that the trigger module
// lifecycle (Init → Start → Stop) works cleanly when cb=nil (the external
// plugin path where the trigger fires nothing but must not panic or error).
func TestSubscribeTrigger_LifecycleNilCallback(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
	}
	m, err := eventbus.NewSubscribeTrigger("trigger-lifecycle-nil", cfg, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// Start with nil callback is a no-op (no goroutine launched).
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Stop must be idempotent and safe even when no goroutine was started.
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestSubscribeTrigger_DoubleStart verifies that calling Start twice returns
// an error without leaking the first goroutine.
func TestSubscribeTrigger_DoubleStart(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-double-start",
		StreamName: "BMW_FULFILLMENT",
	}
	fired := make(chan struct{})
	cb := func(action string, data map[string]any) error {
		close(fired)
		return nil
	}
	m, err := eventbus.NewSubscribeTrigger("trigger-double-start", cfg, cb)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// First Start — succeeds (goroutine launched but will retry with backoff
	// because no bus is registered; that's fine for this test).
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	// Second Start — must return an error.
	if err := m.Start(context.Background()); err == nil {
		t.Error("second Start: expected error for double-start, got nil")
	}
	// Cleanup: Stop cancels the first goroutine cleanly.
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ── embedded NATS server helpers ──────────────────────────────────────────────

// startEmbeddedNATS starts an embedded NATS server with JetStream enabled and
// returns the connection URL. The server is shut down in t.Cleanup.
func startEmbeddedNATS(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      natsserver.RANDOM_PORT,
		JetStream: true,
		NoLog:     true,
		NoSigs:    true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("start embedded NATS: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS not ready within 5s")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// setupNATSStream creates a JetStream stream and durable consumer on the given
// connection and returns the connection (already open; caller must close).
func setupNATSStream(t *testing.T, url, streamName, subject, consumerName string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect to embedded NATS: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatalf("JetStream context: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	}); err != nil {
		nc.Close()
		t.Fatalf("create stream %q: %v", streamName, err)
	}
	if _, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		nc.Close()
		t.Fatalf("create consumer %q: %v", consumerName, err)
	}
	return nc
}

// ── fetchAndFire — callback data contract ─────────────────────────────────────

// TestSubscribeTrigger_FetchAndFire_CallbackData verifies that the trigger
// invokes the callback with a data map whose keys and value types match the
// workflow.plugin.eventbus.v1.Message proto contract:
//
//	"subject"      → string
//	"payload"      → []byte   (not string — proto field is bytes)
//	"headers"      → map[string]string (nil if no headers)
//	"sequence"     → string
//	"published_at" → string
//	"ack_token"    → string
//
// This test exercises the in-process trigger wiring path (cb != nil)
// end-to-end using an embedded NATS server + a real eventbus.broker module
// dispatched through providers.RuntimeBroker.
func TestSubscribeTrigger_FetchAndFire_CallbackData(t *testing.T) {
	const (
		instanceName = "trigger-fetch-test"
		streamName   = "FETCH_TEST"
		subject      = "FETCH_TEST.events"
		consumerName = "fetch-test-consumer"
	)

	natsURL := startEmbeddedNATS(t)
	nc := setupNATSStream(t, natsURL, streamName, subject, consumerName)
	defer nc.Close()

	// Spin up a real eventbus.broker module so LookupRuntimeWithFallback
	// resolves through the RuntimeBroker abstraction. Init + Start populate
	// the broker-instance registry with a live nats runtime + connection.
	bus, err := eventbus.NewClusterModule(instanceName, &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
		Dsn:          natsURL,
	})
	if err != nil {
		t.Fatalf("create broker module: %v", err)
	}
	if err := bus.Init(); err != nil {
		t.Fatalf("Init broker: %v", err)
	}
	if err := bus.Start(context.Background()); err != nil {
		t.Fatalf("Start broker: %v", err)
	}
	t.Cleanup(func() { _ = bus.Stop(context.Background()) })

	// Publish one message with a custom header.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    []byte(`{"vin":"WBA3A5C50DF456789"}`),
		Header:  nats.Header{"X-Trace-Id": []string{"abc123"}},
	}
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wire a callback that captures the data map.
	var (
		mu      sync.Mutex
		gotData map[string]any
		gotOnce sync.Once
		done    = make(chan struct{})
	)
	cb := sdk.TriggerCallback(func(action string, data map[string]any) error {
		mu.Lock()
		defer mu.Unlock()
		gotOnce.Do(func() {
			gotData = data
			close(done)
		})
		return nil
	})

	cfg := &eventbusv1.ConsumerConfig{
		Name:       consumerName,
		StreamName: streamName,
	}
	m, err := eventbus.NewSubscribeTrigger(instanceName, cfg, cb)
	if err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for callback (timeout after 10s).
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("callback not invoked within 10s")
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	mu.Lock()
	d := gotData
	mu.Unlock()

	// ── assert all six Message proto fields are present with correct types ─────

	subject_, ok := d["subject"].(string)
	if !ok {
		t.Errorf("data[subject]: expected string, got %T", d["subject"])
	} else if subject_ != subject {
		t.Errorf("data[subject] = %q, want %q", subject_, subject)
	}

	payload, ok := d["payload"].([]byte)
	if !ok {
		t.Errorf("data[payload]: expected []byte, got %T (value: %v)", d["payload"], d["payload"])
	} else if string(payload) != `{"vin":"WBA3A5C50DF456789"}` {
		t.Errorf("data[payload] = %q, want JSON payload", payload)
	}

	headers, ok := d["headers"].(map[string]string)
	if !ok {
		t.Errorf("data[headers]: expected map[string]string, got %T", d["headers"])
	} else if headers["X-Trace-Id"] != "abc123" {
		t.Errorf("data[headers][X-Trace-Id] = %q, want %q", headers["X-Trace-Id"], "abc123")
	}

	seq, ok := d["sequence"].(string)
	if !ok {
		t.Errorf("data[sequence]: expected string, got %T", d["sequence"])
	} else if seq == "" {
		t.Error("data[sequence] is empty")
	}

	publishedAt, ok := d["published_at"].(string)
	if !ok {
		t.Errorf("data[published_at]: expected string, got %T", d["published_at"])
	} else if publishedAt == "" {
		t.Error("data[published_at] is empty")
	}

	// ack_token is the NATS reply subject — non-empty for JetStream messages.
	ackToken, ok := d["ack_token"].(string)
	if !ok {
		t.Errorf("data[ack_token]: expected string, got %T", d["ack_token"])
	} else if ackToken == "" {
		t.Error("data[ack_token] is empty for JetStream message")
	}

	// Verify no unexpected extra keys beyond the six proto fields.
	wantKeys := map[string]bool{
		"subject": true, "payload": true, "headers": true,
		"sequence": true, "published_at": true, "ack_token": true,
	}
	for k := range d {
		if !wantKeys[k] {
			t.Errorf("data contains unexpected key %q", k)
		}
	}
}

// TestSubscribeTrigger_FetchLoop_ExitsOnCancel verifies that the goroutine
// started by Start exits cleanly when Stop is called (context cancel path).
func TestSubscribeTrigger_FetchLoop_ExitsOnCancel(t *testing.T) {
	const (
		instanceName = "trigger-cancel-test"
		streamName   = "CANCEL_TEST"
		subject      = "CANCEL_TEST.events"
		consumerName = "cancel-test-consumer"
	)

	natsURL := startEmbeddedNATS(t)
	nc := setupNATSStream(t, natsURL, streamName, subject, consumerName)
	defer nc.Close()

	bus, err := eventbus.NewClusterModule(instanceName, &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
		Dsn:          natsURL,
	})
	if err != nil {
		t.Fatalf("create broker module: %v", err)
	}
	if err := bus.Init(); err != nil {
		t.Fatalf("Init broker: %v", err)
	}
	if err := bus.Start(context.Background()); err != nil {
		t.Fatalf("Start broker: %v", err)
	}
	t.Cleanup(func() { _ = bus.Stop(context.Background()) })

	cb := sdk.TriggerCallback(func(string, map[string]any) error { return nil })
	cfg := &eventbusv1.ConsumerConfig{
		Name:       consumerName,
		StreamName: streamName,
	}
	m, err := eventbus.NewSubscribeTrigger(instanceName, cfg, cb)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Stop must return promptly — the goroutine must exit within fetchPollInterval + margin.
	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop(context.Background()) }()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Errorf("Stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return within 5s — goroutine likely leaked")
	}
}

// TestSubscribeTrigger_FetchLoop_RetryOnError verifies that the subscribe
// loop keeps retrying after a transient error and eventually fires the
// callback when the stream becomes available. We simulate "not yet
// available" by publishing after Start rather than before.
func TestSubscribeTrigger_FetchLoop_RetryOnError(t *testing.T) {
	const (
		instanceName = "trigger-retry-test"
		streamName   = "RETRY_TEST"
		subject      = "RETRY_TEST.events"
		consumerName = "retry-test-consumer"
	)

	natsURL := startEmbeddedNATS(t)
	nc := setupNATSStream(t, natsURL, streamName, subject, consumerName)
	defer nc.Close()

	bus, err := eventbus.NewClusterModule(instanceName, &eventbusv1.ClusterConfig{
		Provider:     "nats",
		DeployTarget: "digitalocean.app_platform",
		Dsn:          natsURL,
	})
	if err != nil {
		t.Fatalf("create broker module: %v", err)
	}
	if err := bus.Init(); err != nil {
		t.Fatalf("Init broker: %v", err)
	}
	if err := bus.Start(context.Background()); err != nil {
		t.Fatalf("Start broker: %v", err)
	}
	t.Cleanup(func() { _ = bus.Stop(context.Background()) })

	var (
		mu       sync.Mutex
		received []map[string]any
		done     = make(chan struct{})
	)
	cb := sdk.TriggerCallback(func(action string, data map[string]any) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, data)
		if len(received) == 1 {
			close(done)
		}
		return nil
	})

	cfg := &eventbusv1.ConsumerConfig{
		Name:       consumerName,
		StreamName: streamName,
	}
	m, err := eventbus.NewSubscribeTrigger(instanceName, cfg, cb)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	// Publish the message after Start so the loop polls at least once before receiving.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if _, err := js.Publish(subject, []byte(fmt.Sprintf(`{"retry":true}`))); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("callback not invoked within 15s")
	}

	mu.Lock()
	count := len(received)
	mu.Unlock()
	if count < 1 {
		t.Errorf("expected at least 1 callback invocation, got %d", count)
	}
}
