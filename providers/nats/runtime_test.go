package nats_test

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	natspkg "github.com/GoCodeAlone/workflow-plugin-eventbus/providers/nats"
)

// ── embedded NATS helpers (mirror trigger_test.go to avoid cross-package coupling) ──

// startEmbeddedNATS starts an in-process NATS server with JetStream enabled
// and returns the client URL. The server is shut down in t.Cleanup.
func startEmbeddedNATS(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      natsserver.RANDOM_PORT,
		JetStream: true,
		NoLog:     true,
		NoSigs:    true,
		StoreDir:  t.TempDir(),
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

// ── Connect ───────────────────────────────────────────────────────────────────

func TestNATSRuntime_Connect_WrongProvider(t *testing.T) {
	rb := natspkg.NewRuntime()
	cfg := &eventbusv1.ClusterConfig{
		Provider: "pgchannel",
		Dsn:      "nats://127.0.0.1:1",
	}
	if _, err := rb.Connect(context.Background(), cfg); err == nil {
		t.Fatal("expected error when provider != nats")
	}
}

func TestNATSRuntime_Connect_EmptyDsn(t *testing.T) {
	rb := natspkg.NewRuntime()
	cfg := &eventbusv1.ClusterConfig{Provider: "nats"}
	if _, err := rb.Connect(context.Background(), cfg); err == nil {
		t.Fatal("expected error when dsn is empty")
	}
}

func TestNATSRuntime_Connect_BadURL(t *testing.T) {
	rb := natspkg.NewRuntime()
	cfg := &eventbusv1.ClusterConfig{
		Provider: "nats",
		// Unreachable port; nats.Connect should fail fast.
		Dsn: "nats://127.0.0.1:1",
	}
	if _, err := rb.Connect(context.Background(), cfg); err == nil {
		t.Fatal("expected dial error for unreachable URL")
	}
}

func TestNATSRuntime_Connect_OK(t *testing.T) {
	url := startEmbeddedNATS(t)
	rb := natspkg.NewRuntime()
	conn, err := rb.Connect(context.Background(), &eventbusv1.ClusterConfig{
		Provider: "nats",
		Dsn:      url,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if got := conn.Provider(); got != "nats" {
		t.Errorf("Provider() = %q, want \"nats\"", got)
	}
}

// ── EnsureStream ──────────────────────────────────────────────────────────────

func TestNATSRuntime_EnsureStream_Creates(t *testing.T) {
	url := startEmbeddedNATS(t)
	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cfg := &eventbusv1.StreamConfig{
		Name:     "STREAM_CREATE",
		Subjects: []string{"STREAM_CREATE.events"},
	}
	if err := rb.EnsureStream(ctx, conn, cfg); err != nil {
		t.Fatalf("EnsureStream (create): %v", err)
	}

	// Verify via a side-channel nats.go client that the stream exists.
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}
	if _, err := js.StreamInfo("STREAM_CREATE"); err != nil {
		t.Fatalf("expected stream STREAM_CREATE to exist: %v", err)
	}
}

func TestNATSRuntime_EnsureStream_Idempotent(t *testing.T) {
	url := startEmbeddedNATS(t)
	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cfg := &eventbusv1.StreamConfig{
		Name:     "STREAM_IDEMPOTENT",
		Subjects: []string{"STREAM_IDEMPOTENT.events"},
	}
	if err := rb.EnsureStream(ctx, conn, cfg); err != nil {
		t.Fatalf("EnsureStream (first): %v", err)
	}
	// Second call must be a no-op (no error).
	if err := rb.EnsureStream(ctx, conn, cfg); err != nil {
		t.Fatalf("EnsureStream (second, idempotent): %v", err)
	}
}

func TestNATSRuntime_EnsureStream_UpdatesOnDiff(t *testing.T) {
	url := startEmbeddedNATS(t)
	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	first := &eventbusv1.StreamConfig{
		Name:     "STREAM_UPDATE",
		Subjects: []string{"STREAM_UPDATE.events"},
	}
	if err := rb.EnsureStream(ctx, conn, first); err != nil {
		t.Fatalf("EnsureStream (first): %v", err)
	}
	updated := &eventbusv1.StreamConfig{
		Name:     "STREAM_UPDATE",
		Subjects: []string{"STREAM_UPDATE.events", "STREAM_UPDATE.alerts"},
	}
	if err := rb.EnsureStream(ctx, conn, updated); err != nil {
		t.Fatalf("EnsureStream (update): %v", err)
	}

	// Verify the second subject is now in the stream's config.
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}
	info, err := js.StreamInfo("STREAM_UPDATE")
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	wantSubjects := map[string]bool{
		"STREAM_UPDATE.events": false,
		"STREAM_UPDATE.alerts": false,
	}
	for _, s := range info.Config.Subjects {
		if _, ok := wantSubjects[s]; ok {
			wantSubjects[s] = true
		}
	}
	for s, found := range wantSubjects {
		if !found {
			t.Errorf("subject %q missing from updated stream", s)
		}
	}
}

// ── EnsureConsumer ────────────────────────────────────────────────────────────

// mkStream is a test helper that pre-creates a JetStream stream so EnsureConsumer
// tests do not also have to exercise EnsureStream.
func mkStream(t *testing.T, url, name, subject string) {
	t.Helper()
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.AddStream(&natsgo.StreamConfig{
		Name:     name,
		Subjects: []string{subject},
	}); err != nil {
		t.Fatalf("AddStream(%q): %v", name, err)
	}
}

func TestNATSRuntime_EnsureConsumer_Creates(t *testing.T) {
	url := startEmbeddedNATS(t)
	mkStream(t, url, "CONS_CREATE", "CONS_CREATE.events")

	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cfg := &eventbusv1.ConsumerConfig{
		Name:       "cons-create",
		StreamName: "CONS_CREATE",
	}
	if err := rb.EnsureConsumer(ctx, conn, "CONS_CREATE", cfg); err != nil {
		t.Fatalf("EnsureConsumer (create): %v", err)
	}

	// Verify via side-channel.
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}
	if _, err := js.ConsumerInfo("CONS_CREATE", "cons-create"); err != nil {
		t.Fatalf("expected consumer cons-create on stream CONS_CREATE: %v", err)
	}
}

func TestNATSRuntime_EnsureConsumer_Idempotent(t *testing.T) {
	url := startEmbeddedNATS(t)
	mkStream(t, url, "CONS_IDEM", "CONS_IDEM.events")

	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cfg := &eventbusv1.ConsumerConfig{
		Name:       "cons-idem",
		StreamName: "CONS_IDEM",
	}
	if err := rb.EnsureConsumer(ctx, conn, "CONS_IDEM", cfg); err != nil {
		t.Fatalf("EnsureConsumer (first): %v", err)
	}
	if err := rb.EnsureConsumer(ctx, conn, "CONS_IDEM", cfg); err != nil {
		t.Fatalf("EnsureConsumer (second, idempotent): %v", err)
	}
}

func TestNATSRuntime_EnsureConsumer_UpdatesOnDiff(t *testing.T) {
	url := startEmbeddedNATS(t)
	mkStream(t, url, "CONS_UPD", "CONS_UPD.>")

	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	first := &eventbusv1.ConsumerConfig{
		Name:       "cons-upd",
		StreamName: "CONS_UPD",
	}
	if err := rb.EnsureConsumer(ctx, conn, "CONS_UPD", first); err != nil {
		t.Fatalf("EnsureConsumer (first): %v", err)
	}
	updated := &eventbusv1.ConsumerConfig{
		Name:          "cons-upd",
		StreamName:    "CONS_UPD",
		FilterSubject: "CONS_UPD.events",
		MaxDeliver:    7,
	}
	if err := rb.EnsureConsumer(ctx, conn, "CONS_UPD", updated); err != nil {
		t.Fatalf("EnsureConsumer (update): %v", err)
	}

	// Verify FilterSubject is now set.
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}
	info, err := js.ConsumerInfo("CONS_UPD", "cons-upd")
	if err != nil {
		t.Fatalf("ConsumerInfo: %v", err)
	}
	if info.Config.FilterSubject != "CONS_UPD.events" {
		t.Errorf("FilterSubject = %q, want \"CONS_UPD.events\"", info.Config.FilterSubject)
	}
	if info.Config.MaxDeliver != 7 {
		t.Errorf("MaxDeliver = %d, want 7", info.Config.MaxDeliver)
	}
}

// ── Publish ───────────────────────────────────────────────────────────────────

func TestNATSRuntime_Publish_RoundTrip(t *testing.T) {
	url := startEmbeddedNATS(t)
	const (
		streamName = "PUB_RT"
		subject    = "PUB_RT.events"
	)
	mkStream(t, url, streamName, subject)

	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Side-channel subscriber to receive the published message.
	verifyNC, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(verifyNC.Close)
	verifyJS, err := verifyNC.JetStream()
	if err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}
	sub, err := verifyJS.SubscribeSync(subject, natsgo.BindStream(streamName), natsgo.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe sync: %v", err)
	}
	t.Cleanup(func() { _ = sub.Drain() })

	req := &eventbusv1.PublishRequest{
		Subject:       subject,
		Payload:       []byte(`{"vin":"WBA3A5C50DF456789"}`),
		Headers:       map[string]string{"X-Trace-Id": "abc123"},
		CorrelationId: "corr-xyz",
	}
	resp, err := rb.Publish(ctx, conn, req)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if resp.GetSequence() == "" {
		t.Error("expected non-empty sequence")
	} else if resp.GetSequence() == "0" {
		t.Errorf("expected non-zero sequence, got %q", resp.GetSequence())
	}
	if resp.GetAckedAt() == "" {
		t.Error("expected non-empty acked_at")
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	if got := string(msg.Data); got != `{"vin":"WBA3A5C50DF456789"}` {
		t.Errorf("payload = %q, want JSON body", got)
	}
	if got := msg.Header.Get("X-Trace-Id"); got != "abc123" {
		t.Errorf("X-Trace-Id header = %q, want abc123", got)
	}
	if got := msg.Header.Get("Nats-Correlation-Id"); got != "corr-xyz" {
		t.Errorf("Nats-Correlation-Id header = %q, want corr-xyz", got)
	}
}

func TestNATSRuntime_Publish_EmptySubject(t *testing.T) {
	url := startEmbeddedNATS(t)
	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{}); err == nil {
		t.Fatal("expected error for empty subject")
	}
}
