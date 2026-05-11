// Package testutil — testcontainers helpers for the pgchannel provider tests.
//
// Each helper is no-op-on-skip when Docker is not available; the caller can
// trust that returning from a helper means the test environment is ready.
package testutil

import (
	"context"
	_ "embed"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// schemaSQL is the canonical pgchannel test schema, embedded so tests can run
// without a working directory dependency. The corresponding production
// migrations live in BMW (design §1.5); any drift is a bug.
//
//go:embed schema.sql
var schemaSQL string

// MustStartTestPostgres spins up a fresh Postgres container with the
// pgchannel schema pre-loaded and returns its DSN. The container is torn
// down via t.Cleanup at the end of the test.
//
// If Docker is unavailable in the current environment the test is SKIPPED
// (via testcontainers.SkipIfProviderIsNotHealthy) rather than failed —
// pgchannel tests are not gated on the unit-test runner having Docker.
//
// Callers should treat the returned DSN as opaque and pass it directly into
// pgchannel.OpenConnection.
func MustStartTestPostgres(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Materialise the embedded schema to a temp file so WithInitScripts can
	// mount it into the container's docker-entrypoint-initdb.d directory.
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "01_eventbus_schema.sql")
	if err := os.WriteFile(scriptPath, []byte(schemaSQL), 0o644); err != nil {
		t.Fatalf("write schema fixture: %v", err)
	}

	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("eventbus_test"),
		postgres.WithUsername("eventbus"),
		postgres.WithPassword("eventbus"),
		postgres.WithInitScripts(scriptPath),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		// Allow plenty of time for image teardown on slow CI hosts.
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		_ = container.Terminate(stopCtx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}
