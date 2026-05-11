package pgchannel_test

import (
	"context"
	"testing"
	"time"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	pgchannel "github.com/GoCodeAlone/workflow-plugin-eventbus/providers/pgchannel"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers/pgchannel/internal/testutil"
)

// TestConnection_OpenClose verifies the basic OpenConnection → Provider →
// Close roundtrip against a real Postgres container. Test is SKIPPED when
// Docker is unavailable (see testutil.MustStartTestPostgres).
func TestConnection_OpenClose(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := pgchannel.OpenConnection(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("OpenConnection: %v", err)
	}
	if got, want := c.Provider(), "pgchannel"; got != want {
		t.Errorf("Provider() = %q, want %q", got, want)
	}
	if c.Pool() == nil {
		t.Error("Pool() returned nil")
	}
	if c.DSN() == "" {
		t.Error("DSN() returned empty string")
	}
	if c.PollInterval() <= 0 {
		t.Errorf("PollInterval() = %v, want positive default", c.PollInterval())
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Close is idempotent — second call must also return nil.
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestConnection_OpenConnection_EmptyDSN pins the empty-dsn error path —
// no Postgres needed; runs everywhere.
func TestConnection_OpenConnection_EmptyDSN(t *testing.T) {
	_, err := pgchannel.OpenConnection(context.Background(), "", 4)
	if err == nil {
		t.Fatal("expected error for empty dsn, got nil")
	}
}

// TestConnection_OpenConnection_BadDSN exercises the pgxpool.ParseConfig
// failure path — runs without Docker since parse rejects before any dial.
func TestConnection_OpenConnection_BadDSN(t *testing.T) {
	_, err := pgchannel.OpenConnection(context.Background(), "::not a dsn::", 4)
	if err == nil {
		t.Fatal("expected parse error for malformed dsn, got nil")
	}
}

// TestConnection_Connect_MaxConnsOverride verifies that ClusterConfig.max_conns
// threads through to the pgxpool.MaxConns ceiling, and that zero falls back
// to defaultMaxConns. Pinning the latter prevents accidental regressions of
// the 4→16 bump that closed the multi-Subscribe deadlock hazard.
func TestConnection_Connect_MaxConnsOverride(t *testing.T) {
	dsn := testutil.MustStartTestPostgres(t)
	rb := pgchannel.NewRuntime()

	// Explicit override → pool MaxConns matches.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connExplicit, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{
		Provider:     "pgchannel",
		Dsn:          dsn,
		BrokerTarget: "in_process",
		MaxConns:     32,
	})
	if err != nil {
		t.Fatalf("Connect (explicit): %v", err)
	}
	defer func() { _ = connExplicit.Close() }()
	pc := connExplicit.(*pgchannel.Connection)
	if got, want := pc.Pool().Config().MaxConns, int32(32); got != want {
		t.Errorf("pool MaxConns (explicit) = %d, want %d", got, want)
	}

	// Zero → defaultMaxConns (16). Pins the 4→16 default bump.
	connDefault, err := rb.Connect(ctx, &eventbusv1.ClusterConfig{
		Provider:     "pgchannel",
		Dsn:          dsn,
		BrokerTarget: "in_process",
		// MaxConns unset → 0
	})
	if err != nil {
		t.Fatalf("Connect (default): %v", err)
	}
	defer func() { _ = connDefault.Close() }()
	pcDefault := connDefault.(*pgchannel.Connection)
	if got, want := pcDefault.Pool().Config().MaxConns, int32(16); got != want {
		t.Errorf("pool MaxConns (default) = %d, want %d (the post-fix default)", got, want)
	}
}

// TestConnection_SetPollInterval verifies the explicit override path used
// by runtime.Connect when ClusterConfig.poll_interval is set. Default → 1s;
// override → caller value. Zero/negative override is silently ignored.
func TestConnection_SetPollInterval(t *testing.T) {
	// Build a Connection by hand to avoid needing a live database.
	c := &pgchannel.Connection{}
	if got := c.PollInterval(); got != 0 {
		t.Fatalf("zero-value PollInterval = %v, want 0 before SetPollInterval", got)
	}
	c.SetPollInterval(500 * time.Millisecond)
	if got, want := c.PollInterval(), 500*time.Millisecond; got != want {
		t.Errorf("PollInterval after Set = %v, want %v", got, want)
	}
	c.SetPollInterval(0) // must be ignored
	if got, want := c.PollInterval(), 500*time.Millisecond; got != want {
		t.Errorf("PollInterval after Set(0) = %v, want unchanged %v", got, want)
	}
}
