// consumer_internal_test.go — Start-time EnsureConsumer dispatch tests.
// Lives in the internal package so we can pre-seed the broker-instance
// registry with hand-built *clusterModule values + the mockRuntime/mockConn
// helpers defined in module_internal_test.go and reuse the
// ensureRecordingRuntime + seedBroker helpers from stream_internal_test.go.
// External-API tests (factory, validation, Init/Stop registry round-trip)
// remain in consumer_test.go.
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
)

// ── consumer Start tests ──────────────────────────────────────────────────────

func TestConsumerModule_StartCallsEnsureConsumer(t *testing.T) {
	rt := &ensureRecordingRuntime{}
	seedBroker(t, "broker-consumer-ensure", rt, &mockConn{})

	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-fulfillment-handler",
		StreamName: "BMW_FULFILLMENT",
		BrokerRef:  "broker-consumer-ensure",
	}
	m, err := NewConsumerModule("consumer-ensure", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	calls := rt.snapshotConsumerCalls()
	if len(calls) != 1 {
		t.Fatalf("EnsureConsumer calls = %d, want 1", len(calls))
	}
	if calls[0].streamName != "BMW_FULFILLMENT" {
		t.Errorf("EnsureConsumer streamName = %q, want BMW_FULFILLMENT", calls[0].streamName)
	}
	if calls[0].cfg != cfg {
		t.Errorf("EnsureConsumer got cfg pointer %p, want %p", calls[0].cfg, cfg)
	}
}

func TestConsumerModule_StartRetries(t *testing.T) {
	rt := &ensureRecordingRuntime{}
	const brokerName = "broker-consumer-retry"

	go func() {
		time.Sleep(200 * time.Millisecond)
		cm := &clusterModule{instanceName: brokerName, runtime: rt, conn: &mockConn{}}
		RegisterBrokerInstance(brokerName, cm)
	}()
	t.Cleanup(func() { UnregisterBrokerInstance(brokerName) })

	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-retry-handler",
		StreamName: "BMW_RETRY",
		BrokerRef:  brokerName,
	}
	m, err := NewConsumerModule("consumer-retry", cfg)
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
	if calls := rt.snapshotConsumerCalls(); len(calls) != 1 {
		t.Errorf("EnsureConsumer calls = %d, want 1", len(calls))
	}
}

func TestConsumerModule_StartTimesOut(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-timeout-handler",
		StreamName: "BMW_TIMEOUT",
		BrokerRef:  "broker-consumer-never-registered",
	}
	m, err := NewConsumerModule("consumer-timeout", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = m.(*consumerModule).Start(ctx)
	if err == nil {
		t.Fatal("expected error when broker never registered")
	}
	if !strings.Contains(err.Error(), "not available within 10s") {
		t.Errorf("error = %q, want substring \"not available within 10s\"", err.Error())
	}
}

func TestConsumerModule_StartLegacyNoBrokerRef(t *testing.T) {
	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-legacy-handler",
		StreamName: "BMW_LEGACY",
		// BrokerRef intentionally empty
	}
	m, err := NewConsumerModule("consumer-legacy", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	start := time.Now()
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("legacy Start took %v; expected near-instant return", elapsed)
	}
}

func TestConsumerModule_StartCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-ctx-handler",
		StreamName: "BMW_CTX",
		BrokerRef:  "broker-consumer-ctx-cancelled",
	}
	m, err := NewConsumerModule("consumer-ctx-cancelled", cfg)
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

// TestConsumerModule_StartEnsurePropagatesError verifies that a non-nil
// EnsureConsumer error bubbles back out of Start with the module-instance
// prefix.
func TestConsumerModule_StartEnsurePropagatesError(t *testing.T) {
	sentinel := errors.New("ensure boom")
	rt := &ensureRecordingRuntime{consumerErr: sentinel}
	seedBroker(t, "broker-consumer-err", rt, &mockConn{})

	cfg := &eventbusv1.ConsumerConfig{
		Name:       "bmw-err-handler",
		StreamName: "BMW_ERR",
		BrokerRef:  "broker-consumer-err",
	}
	m, err := NewConsumerModule("consumer-err", cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	err = m.Start(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want errors.Is(sentinel)", err)
	}
	if !strings.Contains(err.Error(), "consumer-err") {
		t.Errorf("error %q missing instance-name prefix", err.Error())
	}
}

// ── concurrency probe — Start across many consumers sharing a broker ─────────

// TestConsumerModule_StartConcurrent makes sure the retry loop + broker
// lookup is safe under -race when many consumer modules start concurrently
// against the same broker.
func TestConsumerModule_StartConcurrent(t *testing.T) {
	rt := &ensureRecordingRuntime{}
	seedBroker(t, "broker-consumer-concurrent", rt, &mockConn{})

	const n = 32
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cfg := &eventbusv1.ConsumerConfig{
				Name:       "bmw-concurrent-handler",
				StreamName: "BMW_CONCURRENT",
				BrokerRef:  "broker-consumer-concurrent",
			}
			m, err := NewConsumerModule("consumer-concurrent", cfg)
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
	if calls := rt.snapshotConsumerCalls(); len(calls) != n {
		t.Errorf("EnsureConsumer calls = %d, want %d", len(calls), n)
	}
}
