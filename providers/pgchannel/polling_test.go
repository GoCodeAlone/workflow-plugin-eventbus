package pgchannel

// White-box tests for the polling + advisory-lock helpers. Lives in
// package pgchannel (not pgchannel_test) so we can call the unexported
// pollOnce / tryConsumerLock / advanceConsumerPosition / incrementDeliveryCount
// / loadConsumerState helpers directly.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/pgchannel/internal/testutil"
)

// seedStream + seedConsumer + seedEvent are small helpers shared by the
// polling tests; they exist locally rather than in testutil because the
// rest of the package's tests use the high-level RuntimeBroker path.

func seedStream(t *testing.T, conn *Connection, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.Pool().Exec(ctx,
		"INSERT INTO eventbus_streams (name, subjects) VALUES ($1, $2)",
		name, []string{"test.>"},
	); err != nil {
		t.Fatalf("seed stream %q: %v", name, err)
	}
}

func seedConsumer(t *testing.T, conn *Connection, stream, name, filter string, maxDeliver int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.Pool().Exec(ctx,
		"INSERT INTO eventbus_consumers (stream_name, name, filter_subject, max_deliver) VALUES ($1, $2, $3, $4)",
		stream, name, filter, maxDeliver,
	); err != nil {
		t.Fatalf("seed consumer %q: %v", name, err)
	}
}

func seedEvent(t *testing.T, conn *Connection, stream, subject string, payload []byte) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var id int64
	if err := conn.Pool().QueryRow(ctx,
		"INSERT INTO eventbus_events (stream_name, subject, payload) VALUES ($1, $2, $3) RETURNING id",
		stream, subject, payload,
	).Scan(&id); err != nil {
		t.Fatalf("seed event %q: %v", subject, err)
	}
	return id
}

// TestPolling_SinglePollerDelivery exercises the basic happy path: insert
// 3 events on a stream, call pollOnce, observe all 3 rows returned in id
// ASC order.
func TestPolling_SinglePollerDelivery(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := OpenConnection(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("OpenConnection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	seedStream(t, conn, "test_stream")
	ids := []int64{
		seedEvent(t, conn, "test_stream", "test.a", []byte("one")),
		seedEvent(t, conn, "test_stream", "test.b", []byte("two")),
		seedEvent(t, conn, "test_stream", "test.c", []byte("three")),
	}

	rows, err := pollOnce(ctx, conn, "test_stream", "", 0, 10)
	if err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if got, want := len(rows), 3; got != want {
		t.Fatalf("got %d rows, want %d", got, want)
	}
	for i, r := range rows {
		if r.ID != ids[i] {
			t.Errorf("rows[%d].ID = %d, want %d", i, r.ID, ids[i])
		}
		if r.StreamName != "test_stream" {
			t.Errorf("rows[%d].StreamName = %q, want test_stream", i, r.StreamName)
		}
	}
}

// TestPolling_FilterSQL_PrefixWildcard verifies that pollOnce honours the
// prefix-wildcard filter and skips non-matching subjects.
func TestPolling_FilterSQL_PrefixWildcard(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := OpenConnection(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("OpenConnection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	seedStream(t, conn, "fstream")
	seedEvent(t, conn, "fstream", "test.match.one", []byte("a"))
	seedEvent(t, conn, "fstream", "other.skip", []byte("b"))
	seedEvent(t, conn, "fstream", "test.match.two", []byte("c"))

	rows, err := pollOnce(ctx, conn, "fstream", "test.match.>", 0, 10)
	if err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if got, want := len(rows), 2; got != want {
		t.Fatalf("got %d rows, want %d (filter should drop test.other.skip)", got, want)
	}
	for _, r := range rows {
		if r.Subject == "other.skip" {
			t.Errorf("filter leaked non-matching subject %q", r.Subject)
		}
	}
}

// TestPolling_ThreeConcurrentPollers_AdvisoryLock spins up three goroutines
// that all call tryConsumerLock concurrently. Only ONE may report acquired;
// the others must get acquired=false (no error). Once the holder releases,
// a subsequent caller may acquire.
func TestPolling_ThreeConcurrentPollers_AdvisoryLock(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := OpenConnection(ctx, dsn, 8)
	if err != nil {
		t.Fatalf("OpenConnection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const stream, consumer = "lockstream", "lockcons"
	seedStream(t, conn, stream)
	seedConsumer(t, conn, stream, consumer, "", 5)

	var (
		acquiredCount atomic.Int32
		releaseHolder func()
		releaseOnce   sync.Once
		wg            sync.WaitGroup
	)
	gotLock := make(chan func(), 3)
	wg.Add(3)
	for range 3 {
		go func() {
			defer wg.Done()
			acquired, release, err := tryConsumerLock(ctx, conn, stream, consumer)
			if err != nil {
				t.Errorf("tryConsumerLock: %v", err)
				return
			}
			if acquired {
				acquiredCount.Add(1)
				gotLock <- release
			}
		}()
	}
	wg.Wait()
	close(gotLock)

	for r := range gotLock {
		releaseOnce.Do(func() { releaseHolder = r })
	}

	if got := acquiredCount.Load(); got != 1 {
		t.Fatalf("got %d concurrent acquisitions, want exactly 1", got)
	}

	// Releasing the holder must make the lock available to the next caller.
	if releaseHolder == nil {
		t.Fatal("internal: releaseHolder never set")
	}
	releaseHolder()

	acquired, release, err := tryConsumerLock(ctx, conn, stream, consumer)
	if err != nil {
		t.Fatalf("post-release tryConsumerLock: %v", err)
	}
	if !acquired {
		t.Fatal("post-release tryConsumerLock did not acquire")
	}
	release()
}

// TestPolling_AdvanceConsumerPosition_Monotonic checks the greatest()
// guard: a backward advance is a no-op (silently ignored), forward
// advance succeeds, and an unknown consumer surfaces an error.
func TestPolling_AdvanceConsumerPosition_Monotonic(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := OpenConnection(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("OpenConnection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	seedStream(t, conn, "posstream")
	seedConsumer(t, conn, "posstream", "poscons", "", 5)

	if err := advanceConsumerPosition(ctx, conn, "posstream", "poscons", 10); err != nil {
		t.Fatalf("advance to 10: %v", err)
	}
	pos, _, _, err := loadConsumerState(ctx, conn, "posstream", "poscons")
	if err != nil {
		t.Fatalf("loadConsumerState: %v", err)
	}
	if pos != 10 {
		t.Fatalf("position = %d, want 10", pos)
	}

	// Backward advance — silently no-op via greatest().
	if err := advanceConsumerPosition(ctx, conn, "posstream", "poscons", 5); err != nil {
		t.Fatalf("backward advance: %v", err)
	}
	pos, _, _, _ = loadConsumerState(ctx, conn, "posstream", "poscons")
	if pos != 10 {
		t.Fatalf("position after backward advance = %d, want unchanged 10", pos)
	}

	// Unknown consumer — error.
	if err := advanceConsumerPosition(ctx, conn, "posstream", "missing", 1); err == nil {
		t.Fatal("expected error for unknown consumer, got nil")
	}
}

// TestPolling_IncrementDeliveryCount_MaxDeliver exercises the upsert path:
// each call against the same (stream, consumer, event) increments the
// returned count. Caller can compare the returned count against
// max_deliver to enforce redelivery cap.
func TestPolling_IncrementDeliveryCount_MaxDeliver(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := OpenConnection(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("OpenConnection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const stream, consumer = "dcstream", "dccons"
	seedStream(t, conn, stream)
	seedConsumer(t, conn, stream, consumer, "", 3)
	eventID := seedEvent(t, conn, stream, "test.dc", []byte("p"))

	for i := 1; i <= 4; i++ {
		got, err := incrementDeliveryCount(ctx, conn, stream, consumer, eventID)
		if err != nil {
			t.Fatalf("increment #%d: %v", i, err)
		}
		if got != i {
			t.Fatalf("increment #%d returned %d, want %d", i, got, i)
		}
	}

	// max_deliver = 3 → on the 4th attempt, count is 4, caller knows to
	// skip the event.
	pos, _, maxDeliver, _ := loadConsumerState(ctx, conn, stream, consumer)
	if maxDeliver != 3 {
		t.Errorf("max_deliver = %d, want 3", maxDeliver)
	}
	_ = pos
}

// TestPolling_LoadConsumerState_Roundtrip pins the position + filter +
// max_deliver fields read back match what we seeded.
func TestPolling_LoadConsumerState_Roundtrip(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := OpenConnection(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("OpenConnection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	seedStream(t, conn, "lcs")
	seedConsumer(t, conn, "lcs", "c1", "test.foo.>", 7)
	if err := advanceConsumerPosition(ctx, conn, "lcs", "c1", 42); err != nil {
		t.Fatalf("advance: %v", err)
	}

	pos, filter, maxDeliver, err := loadConsumerState(ctx, conn, "lcs", "c1")
	if err != nil {
		t.Fatalf("loadConsumerState: %v", err)
	}
	if pos != 42 {
		t.Errorf("position = %d, want 42", pos)
	}
	if filter != "test.foo.>" {
		t.Errorf("filter = %q, want test.foo.>", filter)
	}
	if maxDeliver != 7 {
		t.Errorf("max_deliver = %d, want 7", maxDeliver)
	}
}

// TestPolling_ConsumerLockKey_Determinism is a tiny pure unit test
// confirming the hash is stable + non-aliasing across stream/consumer
// boundaries. Runs without Docker.
func TestPolling_ConsumerLockKey_Determinism(t *testing.T) {
	a := consumerLockKey("stream-x", "consumer-y")
	b := consumerLockKey("stream-x", "consumer-y")
	if a != b {
		t.Errorf("consumerLockKey is non-deterministic: %d vs %d", a, b)
	}
	// Boundary aliasing: "stream-xc" || "onsumer-y" must NOT collide with
	// "stream-x" || "consumer-y" — the null separator inside the hash
	// prevents it.
	c := consumerLockKey("stream-xc", "onsumer-y")
	if a == c {
		t.Errorf("consumerLockKey aliases across stream/consumer boundary: %d == %d", a, c)
	}
	// Different consumers on the same stream must hash differently.
	d := consumerLockKey("stream-x", "consumer-z")
	if a == d {
		t.Errorf("consumerLockKey collides on differing consumer: %d == %d", a, d)
	}
	_ = fmt.Sprint(a)
}
