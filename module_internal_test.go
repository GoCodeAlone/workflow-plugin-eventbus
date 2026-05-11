// module_internal_test.go — tests of the broker-instance registry +
// LookupRuntime that need to construct a *clusterModule directly (the type
// is unexported by design). External-API tests live in module_test.go.
package eventbus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// ── mockRuntime / mockConn — minimal providers.RuntimeBroker / Connection
// implementations sufficient for registry-level tests. They never touch a
// real broker; methods that should not be called in a registry test return
// errors so any accidental dispatch fails loudly.

type mockConn struct{ closed bool }

func (m *mockConn) Close() error    { m.closed = true; return nil }
func (m *mockConn) Provider() string { return "mock" }

type mockRuntime struct{}

func (mockRuntime) Connect(_ context.Context, _ *eventbusv1.ClusterConfig) (providers.Connection, error) {
	return &mockConn{}, nil
}
func (mockRuntime) EnsureStream(_ context.Context, _ providers.Connection, _ *eventbusv1.StreamConfig) error {
	return errors.New("mockRuntime: EnsureStream not expected in registry tests")
}
func (mockRuntime) EnsureConsumer(_ context.Context, _ providers.Connection, _ string, _ *eventbusv1.ConsumerConfig) error {
	return errors.New("mockRuntime: EnsureConsumer not expected in registry tests")
}
func (mockRuntime) Publish(_ context.Context, _ providers.Connection, _ *eventbusv1.PublishRequest) (*eventbusv1.PublishResponse, error) {
	return nil, errors.New("mockRuntime: Publish not expected in registry tests")
}
func (mockRuntime) Subscribe(_ context.Context, _ providers.Connection, _ string, _ string, _ providers.MessageHandler) error {
	return errors.New("mockRuntime: Subscribe not expected in registry tests")
}
func (mockRuntime) Ack(_ context.Context, _ providers.Connection, _ string) error {
	return errors.New("mockRuntime: Ack not expected in registry tests")
}

// TestBrokerInstanceRegistry_RegisterLookup exercises the Register / Lookup /
// Unregister cycle. After Unregister, Lookup must return (nil, false).
func TestBrokerInstanceRegistry_RegisterLookup(t *testing.T) {
	cm := &clusterModule{instanceName: "register-lookup-bus"}
	RegisterBrokerInstance(cm.instanceName, cm)
	t.Cleanup(func() { UnregisterBrokerInstance(cm.instanceName) })

	got, ok := LookupBrokerInstance("register-lookup-bus")
	if !ok {
		t.Fatal("expected to find broker instance after Register")
	}
	if got != cm {
		t.Errorf("LookupBrokerInstance returned different pointer; got %p, want %p", got, cm)
	}

	UnregisterBrokerInstance("register-lookup-bus")
	if _, ok := LookupBrokerInstance("register-lookup-bus"); ok {
		t.Fatal("expected Lookup to return false after Unregister")
	}
}

// TestLookupRuntime_NotStarted: a registered module whose Start has not yet
// run (runtime/conn still nil) must surface a "not yet started" error rather
// than returning a nil runtime that callers would dereference.
func TestLookupRuntime_NotStarted(t *testing.T) {
	cm := &clusterModule{instanceName: "not-started-bus"} // runtime + conn nil
	RegisterBrokerInstance(cm.instanceName, cm)
	t.Cleanup(func() { UnregisterBrokerInstance(cm.instanceName) })

	_, _, err := LookupRuntime("not-started-bus")
	if err == nil {
		t.Fatal("expected error for not-yet-started broker")
	}
	if !strings.Contains(err.Error(), "not yet started") {
		t.Errorf("error = %q, want substring \"not yet started\"", err.Error())
	}
}

// TestClusterModule_StartStopConcurrentLookup is the regression test for the
// runtime/conn race that Group E would have surfaced once stream/consumer
// modules began calling LookupRuntime from their Start hooks. Without the
// sync.RWMutex guard on clusterModule.runtime + conn, the production Stop
// path (which sets both to nil) racing against LookupRuntime would either
//   - panic on nil deref inside the caller dereferencing conn, or
//   - get caught by `go test -race` as a data race on the field pair.
//
// This test spawns one goroutine repeatedly publishing runtime+conn (the
// "Start" side) and clearing them (the "Stop" side) under the mutex,
// concurrently with 100 reader goroutines each calling LookupRuntime in
// a tight loop. With the mutex, neither -race nor a nil deref should fire.
//
// We drive the field writes directly (not via the real Start/Stop, which
// hard-code provider runtimes) so the test stays hermetic — no Postgres
// container, no live NATS broker. The mutex contract being exercised
// is identical: every writer holds m.mu.Lock; every reader holds
// m.mu.RLock; readers only ever see a fully-set or fully-cleared pair.
func TestClusterModule_StartStopConcurrentLookup(t *testing.T) {
	const (
		readers       = 100
		writeCycles   = 500
		readsPerGoroutine = 1000
	)
	cm := &clusterModule{instanceName: "start-stop-race-bus"}
	RegisterBrokerInstance(cm.instanceName, cm)
	t.Cleanup(func() { UnregisterBrokerInstance(cm.instanceName) })

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer goroutine: alternates between "Start" (set fields, register)
	// and "Stop" (unregister, clear fields) under the mutex, mirroring the
	// production code path exactly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		mc := &mockConn{}
		mr := mockRuntime{}
		for i := 0; i < writeCycles; i++ {
			// Start side.
			cm.mu.Lock()
			cm.runtime = mr
			cm.conn = mc
			cm.mu.Unlock()
			RegisterBrokerInstance(cm.instanceName, cm)

			// Stop side: snapshot under lock, clear, then "close" outside.
			UnregisterBrokerInstance(cm.instanceName)
			cm.mu.Lock()
			c := cm.conn
			cm.conn = nil
			cm.runtime = nil
			cm.mu.Unlock()
			_ = c // would be c.Close() in production
			// Re-register so the next reader iteration has a target.
			RegisterBrokerInstance(cm.instanceName, cm)
		}
		close(stop)
	}()

	// Reader goroutines: hammer LookupRuntime. Each lookup must either
	// return (runtime, conn, nil) with BOTH non-nil, or return an error.
	// The forbidden outcome is err==nil with a nil runtime or conn (the
	// torn-read symptom of the unguarded version).
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerGoroutine; i++ {
				select {
				case <-stop:
					return
				default:
				}
				rt, conn, err := LookupRuntime(cm.instanceName)
				if err != nil {
					// "not yet started" or "not registered" — both fine.
					continue
				}
				if rt == nil || conn == nil {
					t.Errorf("torn read: LookupRuntime returned nil pair without error (rt=%v conn=%v)", rt, conn)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestLookupRuntime_Success: a fully-initialised module (runtime + conn set)
// must return the same runtime + conn pointers passed in.
func TestLookupRuntime_Success(t *testing.T) {
	mc := &mockConn{}
	cm := &clusterModule{
		instanceName: "lookup-success-bus",
		runtime:      mockRuntime{},
		conn:         mc,
	}
	RegisterBrokerInstance(cm.instanceName, cm)
	t.Cleanup(func() { UnregisterBrokerInstance(cm.instanceName) })

	rt, conn, err := LookupRuntime("lookup-success-bus")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := rt.(mockRuntime); !ok {
		t.Errorf("LookupRuntime returned wrong runtime type: %T", rt)
	}
	if conn != mc {
		t.Errorf("LookupRuntime returned wrong conn pointer; got %p, want %p", conn, mc)
	}
}
