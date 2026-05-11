// stream_internal_test.go — Start-time EnsureStream dispatch tests. Lives in
// the internal package so we can pre-seed the broker-instance registry with
// hand-built *clusterModule values + the mockRuntime/mockConn defined in
// module_internal_test.go. External-API tests (factory, validation, Init/Stop
// registry round-trip) remain in stream_test.go.
package eventbus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// ensureRecordingRuntime records every EnsureStream / EnsureConsumer call so
// the Start tests can assert the dispatch happened exactly once with the
// expected config. Methods not exercised by the tests delegate to mockRuntime
// (returning errors) so accidental dispatch fails loudly.
type ensureRecordingRuntime struct {
	mockRuntime
	mu              sync.Mutex
	streamCalls     []*eventbusv1.StreamConfig
	streamErr       error
	consumerCalls   []consumerCall
	consumerErr     error
}

type consumerCall struct {
	streamName string
	cfg        *eventbusv1.ConsumerConfig
}

func (r *ensureRecordingRuntime) EnsureStream(_ context.Context, _ providers.Connection, cfg *eventbusv1.StreamConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.streamCalls = append(r.streamCalls, cfg)
	return r.streamErr
}

func (r *ensureRecordingRuntime) EnsureConsumer(_ context.Context, _ providers.Connection, streamName string, cfg *eventbusv1.ConsumerConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumerCalls = append(r.consumerCalls, consumerCall{streamName: streamName, cfg: cfg})
	return r.consumerErr
}

func (r *ensureRecordingRuntime) snapshotStreamCalls() []*eventbusv1.StreamConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*eventbusv1.StreamConfig, len(r.streamCalls))
	copy(out, r.streamCalls)
	return out
}

func (r *ensureRecordingRuntime) snapshotConsumerCalls() []consumerCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]consumerCall, len(r.consumerCalls))
	copy(out, r.consumerCalls)
	return out
}

// seedBroker registers a *clusterModule under brokerName with the given
// runtime + conn already populated, so LookupRuntime succeeds without going
// through the real Start path (which would require a live NATS server or
// Postgres). Returns the cleanup func.
func seedBroker(t *testing.T, brokerName string, rt providers.RuntimeBroker, conn providers.Connection) {
	t.Helper()
	cm := &clusterModule{
		instanceName: brokerName,
		runtime:      rt,
		conn:         conn,
	}
	RegisterBrokerInstance(brokerName, cm)
	t.Cleanup(func() { UnregisterBrokerInstance(brokerName) })
}

// ── stream Start tests ────────────────────────────────────────────────────────

func TestStreamModule_StartCallsEnsureStream(t *testing.T) {
	rt := &ensureRecordingRuntime{}
	seedBroker(t, "broker-stream-ensure", rt, &mockConn{})

	cfg := &eventbusv1.StreamConfig{
		Name:      "BMW_FULFILLMENT",
		Subjects:  []string{"fulfillment.>"},
		BrokerRef: "broker-stream-ensure",
	}
	m, err := NewStreamModule("stream-ensure", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	calls := rt.snapshotStreamCalls()
	if len(calls) != 1 {
		t.Fatalf("EnsureStream calls = %d, want 1", len(calls))
	}
	if calls[0] != cfg {
		t.Errorf("EnsureStream got cfg pointer %p, want %p", calls[0], cfg)
	}
}

func TestStreamModule_StartRetries(t *testing.T) {
	rt := &ensureRecordingRuntime{}
	const brokerName = "broker-stream-retry"

	// Register broker after ~200ms so the first few LookupRuntime calls fail
	// and the retry loop has to back off + try again. The 10s budget on Start
	// gives ample headroom for the ~200ms delay.
	go func() {
		time.Sleep(200 * time.Millisecond)
		cm := &clusterModule{instanceName: brokerName, runtime: rt, conn: &mockConn{}}
		RegisterBrokerInstance(brokerName, cm)
	}()
	t.Cleanup(func() { UnregisterBrokerInstance(brokerName) })

	cfg := &eventbusv1.StreamConfig{
		Name:      "BMW_RETRY",
		Subjects:  []string{"retry.>"},
		BrokerRef: brokerName,
	}
	m, err := NewStreamModule("stream-retry", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	start := time.Now()
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v (elapsed %v)", err, time.Since(start))
	}
	elapsed := time.Since(start)
	if elapsed < 100*time.Millisecond {
		t.Errorf("Start returned in %v; expected at least ~100ms while waiting for broker", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("Start took %v; expected <2s (broker was up after 200ms)", elapsed)
	}
	if calls := rt.snapshotStreamCalls(); len(calls) != 1 {
		t.Errorf("EnsureStream calls = %d, want 1", len(calls))
	}
}

func TestStreamModule_StartTimesOut(t *testing.T) {
	// Broker is never registered. Use an internal helper to shrink the budget
	// so we don't burn 10s in unit tests — drive the Start logic directly with
	// retryWithBackoff so the test stays under a second.
	cfg := &eventbusv1.StreamConfig{
		Name:      "BMW_TIMEOUT",
		Subjects:  []string{"timeout.>"},
		BrokerRef: "broker-stream-never-registered",
	}
	m, err := NewStreamModule("stream-timeout", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// We invoke the real Start with a context that we cancel quickly so the
	// retry loop exits via ctx instead of waiting the full 10s. Asserting the
	// "not available" wrapper still requires the Start to use retryWithBackoff
	// + the LookupRuntime path; the ctx-cancelled branch returns ctx.Err which
	// the wrapper formats with "not available within 10s: context canceled".
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = m.(*streamModule).Start(ctx)
	if err == nil {
		t.Fatal("expected error when broker never registered")
	}
	if !strings.Contains(err.Error(), "not available within 10s") {
		t.Errorf("error = %q, want substring \"not available within 10s\"", err.Error())
	}
}

func TestStreamModule_StartLegacyNoBrokerRef(t *testing.T) {
	cfg := &eventbusv1.StreamConfig{
		Name:     "BMW_LEGACY",
		Subjects: []string{"legacy.>"},
		// BrokerRef intentionally empty
	}
	m, err := NewStreamModule("stream-legacy", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Must return immediately without contacting any broker.
	start := time.Now()
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("legacy Start took %v; expected near-instant return", elapsed)
	}
}

func TestStreamModule_StartCtxCancelled(t *testing.T) {
	// Broker not registered; ctx is pre-cancelled. Start should return quickly
	// without burning the full 10s budget. The first attempt runs (per
	// retryWithBackoff contract) and fails with "not registered", then the
	// select observes ctx.Done and returns ctx.Err — wrapped by Start as
	// "not available within 10s: context canceled".
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &eventbusv1.StreamConfig{
		Name:      "BMW_CTX",
		Subjects:  []string{"ctx.>"},
		BrokerRef: "broker-stream-ctx-cancelled",
	}
	m, err := NewStreamModule("stream-ctx-cancelled", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	start := time.Now()
	err = m.Start(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error from cancelled ctx")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want errors.Is(context.Canceled)", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("ctx-cancelled Start took %v; expected fast return", elapsed)
	}
}

// TestStreamModule_StartEnsurePropagatesError verifies that a non-nil
// EnsureStream error bubbles back out of Start with the module-instance prefix.
func TestStreamModule_StartEnsurePropagatesError(t *testing.T) {
	sentinel := errors.New("ensure boom")
	rt := &ensureRecordingRuntime{streamErr: sentinel}
	seedBroker(t, "broker-stream-err", rt, &mockConn{})

	cfg := &eventbusv1.StreamConfig{
		Name:      "BMW_ERR",
		Subjects:  []string{"err.>"},
		BrokerRef: "broker-stream-err",
	}
	m, err := NewStreamModule("stream-err", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	err = m.Start(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want errors.Is(sentinel)", err)
	}
	if !strings.Contains(err.Error(), "stream-err") {
		t.Errorf("error %q missing instance-name prefix", err.Error())
	}
}

// ── concurrency probe — Start across many streams sharing a broker ───────────

// TestStreamModule_StartConcurrent makes sure the retry loop + broker lookup
// pair is safe under -race when many stream modules start concurrently against
// the same broker. Mirrors the real workflow runtime where modular boots
// dozens of modules in parallel goroutines.
func TestStreamModule_StartConcurrent(t *testing.T) {
	rt := &ensureRecordingRuntime{}
	seedBroker(t, "broker-stream-concurrent", rt, &mockConn{})

	const n = 32
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cfg := &eventbusv1.StreamConfig{
				Name:      "BMW_CONCURRENT",
				Subjects:  []string{"concurrent.>"},
				BrokerRef: "broker-stream-concurrent",
			}
			m, err := NewStreamModule("stream-concurrent", cfg)
			if err != nil {
				return
			}
			if err := m.Start(context.Background()); err == nil {
				ok.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != int32(n) {
		t.Errorf("%d/%d concurrent Starts succeeded", ok.Load(), n)
	}
	if calls := rt.snapshotStreamCalls(); len(calls) != n {
		t.Errorf("EnsureStream calls = %d, want %d", len(calls), n)
	}
}
