package nats_test

import (
	"context"
	"errors"
	"strconv"
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

// ── Subscribe ─────────────────────────────────────────────────────────────────

// mkConsumer pre-creates a durable JetStream consumer with explicit-ack policy.
func mkConsumer(t *testing.T, url, streamName, consumerName string) {
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
	if _, err := js.AddConsumer(streamName, &natsgo.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: natsgo.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("AddConsumer(%q): %v", consumerName, err)
	}
}

func TestNATSRuntime_Subscribe_HandlerReceivesMessagesInOrder(t *testing.T) {
	url := startEmbeddedNATS(t)
	const (
		streamName   = "SUB_ORDER"
		subject      = "SUB_ORDER.events"
		consumerName = "sub-order-consumer"
		numMessages  = 3
	)
	mkStream(t, url, streamName, subject)
	mkConsumer(t, url, streamName, consumerName)

	rb := natspkg.NewRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Publish 3 messages via the runtime.
	for i := 1; i <= numMessages; i++ {
		payload := []byte("msg-" + strconv.Itoa(i))
		if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
			Subject: subject,
			Payload: payload,
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	received := make(chan *eventbusv1.Message, numMessages)
	handler := func(_ context.Context, msg *eventbusv1.Message) error {
		received <- msg
		return nil
	}

	subDone := make(chan error, 1)
	go func() {
		subDone <- rb.Subscribe(ctx, conn, streamName, consumerName, handler)
	}()

	// Collect numMessages messages.
	got := make([]*eventbusv1.Message, 0, numMessages)
	timeout := time.After(10 * time.Second)
	for len(got) < numMessages {
		select {
		case m := <-received:
			got = append(got, m)
		case <-timeout:
			t.Fatalf("only received %d/%d messages within 10s", len(got), numMessages)
		}
	}

	// Cancel and wait for Subscribe to return.
	cancel()
	select {
	case err := <-subDone:
		// ctx.Canceled is the expected exit cause.
		if err != nil && err != context.Canceled {
			t.Fatalf("Subscribe returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return within 5s of cancel")
	}

	// Verify order + payloads + ack_token.
	for i, m := range got {
		want := "msg-" + strconv.Itoa(i+1)
		if string(m.GetPayload()) != want {
			t.Errorf("message %d payload = %q, want %q", i, m.GetPayload(), want)
		}
		if m.GetAckToken() == "" {
			t.Errorf("message %d: ack_token is empty (expected JetStream reply subject)", i)
		}
		if m.GetSubject() != subject {
			t.Errorf("message %d subject = %q, want %q", i, m.GetSubject(), subject)
		}
	}
}

func TestNATSRuntime_Subscribe_ExitsOnCtxCancel(t *testing.T) {
	url := startEmbeddedNATS(t)
	const (
		streamName   = "SUB_CANCEL"
		subject      = "SUB_CANCEL.events"
		consumerName = "sub-cancel-consumer"
	)
	mkStream(t, url, streamName, subject)
	mkConsumer(t, url, streamName, consumerName)

	rb := natspkg.NewRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	handler := func(context.Context, *eventbusv1.Message) error { return nil }

	subDone := make(chan error, 1)
	go func() {
		subDone <- rb.Subscribe(ctx, conn, streamName, consumerName, handler)
	}()

	// No messages will arrive — give Subscribe a moment to enter its idle loop,
	// then cancel and ensure it exits promptly.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-subDone:
		if err != nil && err != context.Canceled {
			t.Fatalf("Subscribe returned %v, want nil or context.Canceled", err)
		}
	case <-time.After(subscribeReturnBudget):
		t.Fatal("Subscribe did not exit within budget after ctx cancel")
	}
}

// subscribeReturnBudget is the upper bound for Subscribe's exit latency after
// ctx cancel — one Fetch cycle (subscribeMaxWait inside runtime.go is 2s) plus
// margin for the goroutine to wake.
const subscribeReturnBudget = 5 * time.Second

func TestNATSRuntime_Subscribe_HandlerErrorNaks(t *testing.T) {
	url := startEmbeddedNATS(t)
	const (
		streamName   = "SUB_NAK"
		subject      = "SUB_NAK.events"
		consumerName = "sub-nak-consumer"
	)
	mkStream(t, url, streamName, subject)
	// Create a consumer with MaxDeliver=2 so a nak'd message redelivers exactly
	// once more before the broker stops retrying.
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("setup connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("setup jetstream: %v", err)
	}
	if _, err := js.AddConsumer(streamName, &natsgo.ConsumerConfig{
		Durable:    consumerName,
		AckPolicy:  natsgo.AckExplicitPolicy,
		MaxDeliver: 2,
		AckWait:    250 * time.Millisecond, // short so the redelivery happens quickly
	}); err != nil {
		t.Fatalf("AddConsumer: %v", err)
	}

	rb := natspkg.NewRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
		Subject: subject,
		Payload: []byte("will-nak"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Handler returns an error on first delivery → Subscribe returns the
	// wrapped error after issuing m.Nak(). The broker will redeliver but our
	// Subscribe call has already returned, so we just verify the error path.
	handlerErr := errors.New("handler-deliberate-fail")
	handler := func(context.Context, *eventbusv1.Message) error { return handlerErr }

	gotErr := rb.Subscribe(ctx, conn, streamName, consumerName, handler)
	if gotErr == nil {
		t.Fatal("expected Subscribe to surface handler error, got nil")
	}
	if !errors.Is(gotErr, handlerErr) {
		t.Errorf("Subscribe error chain missing handler error: %v", gotErr)
	}
}

// ── Ack ───────────────────────────────────────────────────────────────────────

func TestNATSRuntime_Ack_EmptyToken(t *testing.T) {
	url := startEmbeddedNATS(t)
	rb := natspkg.NewRuntime()
	ctx := context.Background()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := rb.Ack(ctx, conn, ""); err == nil {
		t.Fatal("expected error for empty ackToken")
	}
}

// TestNATSRuntime_Ack_NoRedeliveryAfterAck verifies the end-to-end flow:
//  1. Publish one message.
//  2. Pull-fetch it via a side-channel subscriber to capture the reply subject.
//  3. Call rb.Ack(token).
//  4. Pull-fetch again with a short MaxWait — must time out (no redelivery).
func TestNATSRuntime_Ack_NoRedeliveryAfterAck(t *testing.T) {
	url := startEmbeddedNATS(t)
	const (
		streamName   = "ACK_NORE"
		subject      = "ACK_NORE.events"
		consumerName = "ack-nore-consumer"
	)
	mkStream(t, url, streamName, subject)
	// Consumer with explicit ack policy and a short AckWait so we can detect
	// no-redelivery quickly.
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("setup connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("setup jetstream: %v", err)
	}
	if _, err := js.AddConsumer(streamName, &natsgo.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: natsgo.AckExplicitPolicy,
		AckWait:   500 * time.Millisecond,
	}); err != nil {
		t.Fatalf("AddConsumer: %v", err)
	}

	rb := natspkg.NewRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{Provider: "nats", Dsn: url})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
		Subject: subject,
		Payload: []byte("ack-me"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Side-channel: pull-fetch the message to capture m.Reply as ack token.
	sub, err := js.PullSubscribe("", consumerName, natsgo.BindStream(streamName))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Drain() })

	msgs, err := sub.Fetch(1, natsgo.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Fetch: got %d messages, want 1", len(msgs))
	}
	ackToken := msgs[0].Reply
	if ackToken == "" {
		t.Fatal("captured message has empty Reply (ack token)")
	}

	// Ack via the runtime.
	if err := rb.Ack(ctx, conn, ackToken); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Wait longer than AckWait so the broker would have redelivered an
	// un-acked message; then fetch again — must time out.
	time.Sleep(1 * time.Second)
	_, err = sub.Fetch(1, natsgo.MaxWait(750*time.Millisecond))
	if !errors.Is(err, natsgo.ErrTimeout) {
		t.Fatalf("expected nats.ErrTimeout on second Fetch (no redelivery), got: %v", err)
	}
}
