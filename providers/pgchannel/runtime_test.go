package pgchannel_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
	pgchannel "github.com/GoCodeAlone/workflow-plugin-eventbus/providers/pgchannel"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/pgchannel/internal/testutil"
)

// startPG is the bring-up shorthand used by every test below: starts the
// container, returns (ctx, rb, conn). The test's deferred cleanup closes
// conn; the container is torn down via t.Cleanup inside MustStartTestPostgres.
func startPG(t *testing.T) (context.Context, providers.RuntimeBroker, providers.Connection) {
	t.Helper()
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	rb := pgchannel.NewRuntime()
	conn, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{
		Provider:     "pgchannel",
		Dsn:          dsn,
		BrokerTarget: "in_process",
		PollInterval: "200ms",
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, rb, conn
}

// ── Connect ────────────────────────────────────────────────────────────────

func TestPGRuntime_Connect_WrongProvider(t *testing.T) {
	rb := pgchannel.NewRuntime()
	_, err := rb.Connect(context.Background(), &eventbusv1.ClusterConfig{Provider: "nats", Dsn: "postgres://x"})
	if err == nil {
		t.Fatal("expected error when provider != pgchannel")
	}
}

func TestPGRuntime_Connect_EmptyDSN(t *testing.T) {
	rb := pgchannel.NewRuntime()
	_, err := rb.Connect(context.Background(), &eventbusv1.ClusterConfig{Provider: "pgchannel"})
	if err == nil {
		t.Fatal("expected error when dsn is empty")
	}
}

func TestPGRuntime_Connect_BadBrokerTarget(t *testing.T) {
	rb := pgchannel.NewRuntime()
	_, err := rb.Connect(context.Background(), &eventbusv1.ClusterConfig{
		Provider:     "pgchannel",
		Dsn:          "postgres://x",
		BrokerTarget: "kubernetes",
	})
	if err == nil {
		t.Fatal("expected error when broker_target != in_process")
	}
}

// ── EnsureStream / EnsureConsumer ─────────────────────────────────────────

func TestPGRuntime_EnsureStream_CreatesAndIdempotent(t *testing.T) {
	ctx, rb, conn := startPG(t)

	cfg := &eventbusv1.StreamConfig{
		Name:     "bmw_fulfillment",
		Subjects: []string{"bmw.>"},
	}
	if err := rb.EnsureStream(ctx, conn, cfg); err != nil {
		t.Fatalf("first EnsureStream: %v", err)
	}
	// Idempotent: a second call with the same config must not error.
	if err := rb.EnsureStream(ctx, conn, cfg); err != nil {
		t.Fatalf("second EnsureStream: %v", err)
	}
	// Updated config (additional subject) → upsert applies.
	cfg.Subjects = []string{"bmw.>", "audit.>"}
	if err := rb.EnsureStream(ctx, conn, cfg); err != nil {
		t.Fatalf("upsert EnsureStream: %v", err)
	}
}

func TestPGRuntime_EnsureConsumer_PreservesPosition(t *testing.T) {
	ctx, rb, conn := startPG(t)

	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: "s1", Subjects: []string{"s1.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	cc := &eventbusv1.ConsumerConfig{
		Name:          "c1",
		FilterSubject: "s1.foo",
		MaxDeliver:    5,
		AckPolicy:     eventbusv1.AckPolicy_ACK_POLICY_EXPLICIT,
	}
	if err := rb.EnsureConsumer(ctx, conn, "s1", cc); err != nil {
		t.Fatalf("EnsureConsumer 1: %v", err)
	}
	// Move position out of band, then call EnsureConsumer again with a
	// changed filter — position must NOT regress.
	pc := conn.(*pgchannel.Connection)
	if _, err := pc.Pool().Exec(ctx, "UPDATE eventbus_consumers SET position = 42 WHERE stream_name = $1 AND name = $2", "s1", "c1"); err != nil {
		t.Fatalf("seed position: %v", err)
	}
	cc.FilterSubject = "s1.bar"
	if err := rb.EnsureConsumer(ctx, conn, "s1", cc); err != nil {
		t.Fatalf("EnsureConsumer 2: %v", err)
	}
	var pos int64
	var filter string
	if err := pc.Pool().QueryRow(ctx, "SELECT position, filter_subject FROM eventbus_consumers WHERE stream_name = $1 AND name = $2", "s1", "c1").Scan(&pos, &filter); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if pos != 42 {
		t.Errorf("position regressed to %d after EnsureConsumer; expected 42", pos)
	}
	if filter != "s1.bar" {
		t.Errorf("filter_subject = %q, want s1.bar", filter)
	}
}

// ── Publish ────────────────────────────────────────────────────────────────

func TestPGRuntime_Publish_PreservesHeadersAndCorrelationID(t *testing.T) {
	ctx, rb, conn := startPG(t)
	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: "hdr_stream", Subjects: []string{"hdr.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	req := &eventbusv1.PublishRequest{
		Subject:       "hdr.x",
		Payload:       []byte("hello"),
		Headers:       map[string]string{"k1": "v1", "k2": "v2"},
		CorrelationId: "corr-123",
	}
	resp, err := rb.Publish(ctx, conn, req)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if resp.GetSequence() == "" {
		t.Error("Publish returned empty sequence")
	}
	if resp.GetAckedAt() == "" {
		t.Error("Publish returned empty acked_at")
	}

	// Read back the row + verify the round-trip.
	pc := conn.(*pgchannel.Connection)
	var subj, corr string
	var hdrJSON []byte
	if err := pc.Pool().QueryRow(ctx,
		"SELECT subject, headers::text, COALESCE(correlation_id, '') FROM eventbus_events WHERE id = $1",
		mustParseInt64(t, resp.GetSequence()),
	).Scan(&subj, &hdrJSON, &corr); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if subj != "hdr.x" {
		t.Errorf("subject = %q, want hdr.x", subj)
	}
	if corr != "corr-123" {
		t.Errorf("correlation_id = %q, want corr-123", corr)
	}
	// JSONB round-trip is canonical — we don't compare the raw bytes but
	// rather decode + check membership.
	if !contains(string(hdrJSON), "k1") || !contains(string(hdrJSON), "v1") {
		t.Errorf("headers JSON = %s, missing k1/v1", string(hdrJSON))
	}
}

func TestPGRuntime_Publish_NoStreamError(t *testing.T) {
	ctx, rb, conn := startPG(t)
	_, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
		Subject: "orphan.subject",
		Payload: []byte("hi"),
	})
	if err == nil {
		t.Fatal("expected error when publishing to a subject with no stream")
	}
}

// ── Subscribe (the big integration) ────────────────────────────────────────

func TestPGRuntime_Subscribe_DeliversAllPublishedInOrder(t *testing.T) {
	ctx, rb, conn := startPG(t)
	const stream = "s_deliver"
	const consumer = "c_deliver"
	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: stream, Subjects: []string{"d.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if err := rb.EnsureConsumer(ctx, conn, stream, &eventbusv1.ConsumerConfig{
		Name:       consumer,
		MaxDeliver: 3,
		AckPolicy:  eventbusv1.AckPolicy_ACK_POLICY_EXPLICIT,
	}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}

	// Publish 3 events.
	for i := range 3 {
		if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
			Subject: "d.msg",
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Subscribe + collect 3 messages with a hard deadline.
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	got := make([]string, 0, 3)
	var mu sync.Mutex
	done := make(chan struct{})
	handler := func(_ context.Context, m *eventbusv1.Message) error {
		mu.Lock()
		got = append(got, string(m.GetPayload()))
		if len(got) == 3 {
			mu.Unlock()
			close(done)
			return nil
		}
		mu.Unlock()
		return nil
	}
	go func() { _ = rb.Subscribe(subCtx, conn, stream, consumer, handler) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		mu.Lock()
		t.Fatalf("timeout waiting for 3 deliveries; got %d: %v", len(got), got)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, want := range []string{"msg-0", "msg-1", "msg-2"} {
		if got[i] != want {
			t.Errorf("got[%d] = %q, want %q (ordering broken)", i, got[i], want)
		}
	}
}

func TestPGRuntime_Subscribe_HandlerErrorBoundedByMaxDeliver(t *testing.T) {
	ctx, rb, conn := startPG(t)
	const stream = "s_md"
	const consumer = "c_md"
	const maxDeliver = 3
	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: stream, Subjects: []string{"md.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if err := rb.EnsureConsumer(ctx, conn, stream, &eventbusv1.ConsumerConfig{
		Name:       consumer,
		MaxDeliver: maxDeliver,
	}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
		Subject: "md.x", Payload: []byte("err-me"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Publish a second, success-eligible event after the failing one so
	// the test can observe position advancing past the failed event.
	if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
		Subject: "md.x", Payload: []byte("ok"),
	}); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}

	var attempts atomic.Int32
	successDone := make(chan struct{})
	handler := func(_ context.Context, m *eventbusv1.Message) error {
		if string(m.GetPayload()) == "err-me" {
			attempts.Add(1)
			return errors.New("simulated handler failure")
		}
		// Once we see "ok" we know position advanced past the failing event.
		close(successDone)
		return nil
	}
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	go func() { _ = rb.Subscribe(subCtx, conn, stream, consumer, handler) }()

	select {
	case <-successDone:
	case <-time.After(20 * time.Second):
		t.Fatalf("timeout: success event not delivered; failing-event attempts=%d", attempts.Load())
	}
	// max_deliver is enforced as a ceiling: attempts should be exactly
	// maxDeliver (failures) before the loop skips past the failing event.
	if got := attempts.Load(); got != int32(maxDeliver) {
		t.Errorf("failing-event attempts = %d, want %d", got, maxDeliver)
	}
}

// ── Ack ────────────────────────────────────────────────────────────────────

func TestPGRuntime_Ack_AdvancesPosition(t *testing.T) {
	ctx, rb, conn := startPG(t)
	const stream = "s_ack"
	const consumer = "c_ack"
	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: stream, Subjects: []string{"a.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if err := rb.EnsureConsumer(ctx, conn, stream, &eventbusv1.ConsumerConfig{Name: consumer, MaxDeliver: 5}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	resp, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{Subject: "a.x", Payload: []byte("p")})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	token := consumer + ":" + resp.GetSequence()
	if err := rb.Ack(ctx, conn, token); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	pc := conn.(*pgchannel.Connection)
	var pos int64
	if err := pc.Pool().QueryRow(ctx, "SELECT position FROM eventbus_consumers WHERE stream_name = $1 AND name = $2", stream, consumer).Scan(&pos); err != nil {
		t.Fatalf("read position: %v", err)
	}
	wantID := mustParseInt64(t, resp.GetSequence())
	if pos != wantID {
		t.Errorf("position = %d, want %d", pos, wantID)
	}
}

func TestPGRuntime_Ack_MalformedToken(t *testing.T) {
	ctx, rb, conn := startPG(t)
	if err := rb.Ack(ctx, conn, "no-colon-here"); err == nil {
		t.Fatal("expected error for malformed ack token")
	}
	if err := rb.Ack(ctx, conn, ""); err == nil {
		t.Fatal("expected error for empty ack token")
	}
	if err := rb.Ack(ctx, conn, "consumer:notanumber"); err == nil {
		t.Fatal("expected error for non-numeric id")
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

func mustParseInt64(t *testing.T, s string) int64 {
	t.Helper()
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return v
}

func contains(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
