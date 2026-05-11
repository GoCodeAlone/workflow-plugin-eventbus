package pgchannel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// defaultMaxConns is the pool MaxConns ceiling used when the caller passes
// 0 or a negative value to OpenConnection. Four matches the design's
// expected concurrency: at most a handful of poller / LISTEN / publish
// goroutines per workflow-server pod.
const defaultMaxConns int32 = 4

// defaultPollInterval is the fallback poll cadence when ClusterConfig.poll_interval
// is unset or unparseable. Sized to deliver sub-second-ish latency while
// remaining gentle on Postgres if LISTEN notifications are lost.
const defaultPollInterval = 1 * time.Second

// Connection is the pgchannel-specific providers.Connection. It wraps a
// pgxpool.Pool plus the DSN + poll cadence configured at Connect time, and
// is passed back through every RuntimeBroker call as the opaque handle.
//
// Callers in the providers/pgchannel package may use the exported Pool()
// getter to access the underlying pool for SQL operations. Cross-package
// callers should only interact via the providers.RuntimeBroker interface.
type Connection struct {
	pool         *pgxpool.Pool
	dsn          string
	pollInterval time.Duration
}

// OpenConnection opens a new pgxpool.Pool against dsn and returns a
// pgchannel.Connection. maxConns caps the pool size; pass 0 to accept the
// package default (4 — sized for the expected handful of poller / LISTEN /
// publish goroutines per pod).
//
// The returned Connection is safe for concurrent use; Close releases the
// underlying pool.
func OpenConnection(ctx context.Context, dsn string, maxConns int32) (*Connection, error) {
	if dsn == "" {
		return nil, errors.New("pgchannel: OpenConnection: dsn is empty")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgchannel: parse dsn: %w", err)
	}
	if maxConns <= 0 {
		maxConns = defaultMaxConns
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgchannel: pool: %w", err)
	}
	return &Connection{
		pool:         pool,
		dsn:          dsn,
		pollInterval: defaultPollInterval,
	}, nil
}

// Close releases the underlying pgxpool.Pool. Idempotent — pgxpool's own
// Close handles repeat calls safely.
func (c *Connection) Close() error {
	if c == nil || c.pool == nil {
		return nil
	}
	c.pool.Close()
	return nil
}

// Provider returns the static provider identifier "pgchannel".
func (c *Connection) Provider() string { return "pgchannel" }

// Pool exposes the underlying pgxpool.Pool for use by sibling files in
// the providers/pgchannel package (polling.go, runtime.go). Cross-package
// callers should NOT use this; route through providers.RuntimeBroker.
func (c *Connection) Pool() *pgxpool.Pool { return c.pool }

// PollInterval returns the per-consumer polling cadence configured at
// Connect time. Used by the Subscribe loop in runtime.go.
func (c *Connection) PollInterval() time.Duration { return c.pollInterval }

// DSN returns the DSN this connection was opened with. Used by Subscribe
// when it needs a dedicated LISTEN connection outside the shared pool.
func (c *Connection) DSN() string { return c.dsn }

// SetPollInterval overrides the poll cadence after construction. Used by
// runtime.Connect to thread the ClusterConfig.poll_interval value through.
func (c *Connection) SetPollInterval(d time.Duration) {
	if d > 0 {
		c.pollInterval = d
	}
}

// Compile-time assertion that *Connection satisfies providers.Connection.
var _ providers.Connection = (*Connection)(nil)
