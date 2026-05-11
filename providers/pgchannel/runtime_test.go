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

	"google.golang.org/protobuf/types/known/durationpb"

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

	token := stream + ":" + consumer + ":" + resp.GetSequence()
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

// TestPGRuntime_Ack_CrossStreamIsolation pins the schema-PK fix: the
// (stream_name, name) primary key on eventbus_consumers means a consumer
// name is NOT globally unique. Acking c1@s1 must NOT advance c1@s2.
// Pre-fix, AckToken was "<consumer>:<id>" and Ack matched on name alone
// → cross-stream pollution. AckToken now includes stream as the first
// component and Ack filters on stream_name AND name.
func TestPGRuntime_Ack_CrossStreamIsolation(t *testing.T) {
	ctx, rb, conn := startPG(t)
	const consumer = "shared_name"
	const streamA = "iso_a"
	const streamB = "iso_b"

	// Two streams, same consumer name on each.
	for _, s := range []string{streamA, streamB} {
		if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
			Name: s, Subjects: []string{s + ".>"},
		}); err != nil {
			t.Fatalf("EnsureStream %s: %v", s, err)
		}
		if err := rb.EnsureConsumer(ctx, conn, s, &eventbusv1.ConsumerConfig{
			Name:       consumer,
			MaxDeliver: 5,
			AckPolicy:  eventbusv1.AckPolicy_ACK_POLICY_EXPLICIT,
		}); err != nil {
			t.Fatalf("EnsureConsumer %s: %v", s, err)
		}
	}

	// Publish one event to streamA and ack it via streamA's token.
	resp, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
		Subject: streamA + ".x", Payload: []byte("p"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	id := mustParseInt64(t, resp.GetSequence())

	tokenA := streamA + ":" + consumer + ":" + resp.GetSequence()
	if err := rb.Ack(ctx, conn, tokenA); err != nil {
		t.Fatalf("Ack streamA: %v", err)
	}

	// streamA's consumer should have advanced; streamB's must be untouched.
	pc := conn.(*pgchannel.Connection)
	var posA, posB int64
	if err := pc.Pool().QueryRow(ctx, "SELECT position FROM eventbus_consumers WHERE stream_name = $1 AND name = $2", streamA, consumer).Scan(&posA); err != nil {
		t.Fatalf("read posA: %v", err)
	}
	if err := pc.Pool().QueryRow(ctx, "SELECT position FROM eventbus_consumers WHERE stream_name = $1 AND name = $2", streamB, consumer).Scan(&posB); err != nil {
		t.Fatalf("read posB: %v", err)
	}
	if posA != id {
		t.Errorf("streamA consumer position = %d, want %d", posA, id)
	}
	if posB != 0 {
		t.Errorf("streamB consumer position = %d, want 0 (cross-stream pollution!)", posB)
	}

	// Acking a token for a stream that does not own that consumer must
	// fail with a "not found" error rather than silently no-oping or
	// advancing the wrong row.
	bogusToken := "nonexistent_stream:" + consumer + ":" + resp.GetSequence()
	if err := rb.Ack(ctx, conn, bogusToken); err == nil {
		t.Error("expected error acking on stream that owns no such consumer")
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
	// Two-part token (legacy "<consumer>:<id>") is now malformed — the
	// 3-element format is required to scope acks to a stream.
	if err := rb.Ack(ctx, conn, "consumer:42"); err == nil {
		t.Fatal("expected error for 2-part legacy token (want <stream>:<consumer>:<id>)")
	}
	// 3-part token with non-numeric id.
	if err := rb.Ack(ctx, conn, "stream:consumer:notanumber"); err == nil {
		t.Fatal("expected error for non-numeric id")
	}
	// Empty stream component.
	if err := rb.Ack(ctx, conn, ":consumer:42"); err == nil {
		t.Fatal("expected error for empty stream component")
	}
}

// TestPGRuntime_EnsureStream_RejectsUnsafeNames pins the upstream guard:
// stream names containing characters outside [a-zA-Z0-9_] are rejected
// at EnsureStream rather than deep inside runListenSession. Without
// this guard, a bad name (e.g. "BMW-Fulfillment") would create a stream
// row whose Subscribe would fail isSafeIdentifier on every reconnect,
// spinning the LISTEN loop forever.
func TestPGRuntime_EnsureStream_RejectsUnsafeNames(t *testing.T) {
	ctx, rb, conn := startPG(t)
	bad := []string{
		"BMW-Fulfillment",   // hyphen
		"stream.with.dots",  // dots
		"stream name",       // space
		"stream;DROP TABLE", // SQL injection attempt
		"stream$",           // dollar
		"",                  // empty
	}
	for _, name := range bad {
		err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
			Name:     name,
			Subjects: []string{"x.>"},
		})
		if err == nil {
			t.Errorf("EnsureStream(%q) = nil, want validation error", name)
		}
	}
	// Valid names still accepted.
	good := []string{"bmw_fulfillment", "Stream1", "UPPER_CASE_OK", "s"}
	for _, name := range good {
		if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
			Name:     name,
			Subjects: []string{name + ".>"},
		}); err != nil {
			t.Errorf("EnsureStream(%q) = %v, want nil", name, err)
		}
	}
}

// ── Consume ────────────────────────────────────────────────────────────────

// TestPGRuntime_Consume_ReturnsBatch verifies that Consume fetches up to
// batch_size events for the named consumer and returns them with valid
// ack_tokens (the "<stream>:<consumer>:<id>" format).
func TestPGRuntime_Consume_ReturnsBatch(t *testing.T) {
	ctx, rb, conn := startPG(t)
	const (
		stream   = "consume_batch"
		consumer = "consume_batch_c"
		numMsgs  = 3
	)
	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: stream, Subjects: []string{"cb.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if err := rb.EnsureConsumer(ctx, conn, stream, &eventbusv1.ConsumerConfig{
		Name: consumer, MaxDeliver: 5,
	}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	for i := 0; i < numMsgs; i++ {
		if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
			Subject: "cb.x", Payload: []byte(strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	resp, err := rb.Consume(ctx, conn, stream, &eventbusv1.ConsumeRequest{
		Consumer:  consumer,
		BatchSize: numMsgs,
		MaxWait:   durationpb.New(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := len(resp.GetMessages()); got != numMsgs {
		t.Fatalf("Consume returned %d messages, want %d", got, numMsgs)
	}
	wantPrefix := stream + ":" + consumer + ":"
	for i, m := range resp.GetMessages() {
		if m.GetAckToken() == "" {
			t.Errorf("message %d: ack_token is empty", i)
			continue
		}
		if !contains(m.GetAckToken(), wantPrefix) {
			t.Errorf("message %d: ack_token = %q, want prefix %q", i, m.GetAckToken(), wantPrefix)
		}
	}
}

// TestPGRuntime_Consume_EmptyTimeout verifies that Consume returns an empty
// batch (not an error) when no rows arrive within max_wait.
func TestPGRuntime_Consume_EmptyTimeout(t *testing.T) {
	ctx, rb, conn := startPG(t)
	const (
		stream   = "consume_empty"
		consumer = "consume_empty_c"
	)
	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: stream, Subjects: []string{"ce.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if err := rb.EnsureConsumer(ctx, conn, stream, &eventbusv1.ConsumerConfig{
		Name: consumer, MaxDeliver: 5,
	}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}

	resp, err := rb.Consume(ctx, conn, stream, &eventbusv1.ConsumeRequest{
		Consumer:  consumer,
		BatchSize: 5,
		MaxWait:   durationpb.New(500 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := len(resp.GetMessages()); got != 0 {
		t.Fatalf("Consume returned %d messages, want 0", got)
	}
}

// TestPGRuntime_Consume_AckAdvancesPosition verifies the full pull → ack
// roundtrip: Consume returns a row + ack_token, Ack advances position past
// that id, and a follow-up Consume sees no rows.
func TestPGRuntime_Consume_AckAdvancesPosition(t *testing.T) {
	ctx, rb, conn := startPG(t)
	const (
		stream   = "consume_ack"
		consumer = "consume_ack_c"
	)
	if err := rb.EnsureStream(ctx, conn, &eventbusv1.StreamConfig{
		Name: stream, Subjects: []string{"ca.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if err := rb.EnsureConsumer(ctx, conn, stream, &eventbusv1.ConsumerConfig{
		Name: consumer, MaxDeliver: 5,
	}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if _, err := rb.Publish(ctx, conn, &eventbusv1.PublishRequest{
		Subject: "ca.x", Payload: []byte("p"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	resp, err := rb.Consume(ctx, conn, stream, &eventbusv1.ConsumeRequest{
		Consumer:  consumer,
		BatchSize: 1,
		MaxWait:   durationpb.New(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(resp.GetMessages()) != 1 {
		t.Fatalf("Consume: got %d messages, want 1", len(resp.GetMessages()))
	}

	if err := rb.Ack(ctx, conn, resp.GetMessages()[0].GetAckToken()); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Second Consume must return empty: position advanced past the row.
	resp2, err := rb.Consume(ctx, conn, stream, &eventbusv1.ConsumeRequest{
		Consumer:  consumer,
		BatchSize: 1,
		MaxWait:   durationpb.New(250 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("Consume 2: %v", err)
	}
	if got := len(resp2.GetMessages()); got != 0 {
		t.Fatalf("Consume after ack: got %d messages, want 0", got)
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
