package pgchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
)

// EventRow is a single row from eventbus_events as read by pollOnce.
// Headers are kept as raw JSON so the runtime can decide whether to
// decode them (publish path always re-encodes; subscribe path decodes
// once per delivery before invoking the handler).
type EventRow struct {
	ID            int64
	StreamName    string
	Subject       string
	Headers       json.RawMessage
	Payload       []byte
	CorrelationID string
	Ts            time.Time
}

// pollOnce reads the next batch of events for the given consumer. It
// applies the consumer's subject filter (parsed via GenFilter) and
// returns only events with id > position. Rows are taken under
// FOR UPDATE SKIP LOCKED so concurrent pollers cannot read the same
// row, even though the per-consumer advisory lock should already serialise
// them — defence in depth against caller misuse.
//
// The returned slice is ordered by id ASC; callers must process events
// in order to preserve at-least-once delivery semantics.
//
// If filter is empty, no subject predicate is applied — equivalent to
// "match everything on this stream".
func pollOnce(ctx context.Context, conn *Connection, stream, filter string, position int64, limit int) ([]EventRow, error) {
	if conn == nil || conn.pool == nil {
		return nil, errors.New("pgchannel: pollOnce: connection is nil")
	}
	if stream == "" {
		return nil, errors.New("pgchannel: pollOnce: stream is required")
	}
	if limit <= 0 {
		limit = 16
	}

	// $1 = stream, $2 = position, $3 = limit, $4..N = filter args
	args := []any{stream, position, limit}
	filterSQL := ""
	if filter != "" {
		fsql, fargs := GenFilter(filter, "$4")
		filterSQL = " AND " + fsql
		args = append(args, fargs...)
	}

	sql := fmt.Sprintf(
		"SELECT id, stream_name, subject, headers, payload, COALESCE(correlation_id, ''), ts "+
			"FROM eventbus_events "+
			"WHERE stream_name = $1 AND id > $2%s "+
			"ORDER BY id ASC "+
			"FOR UPDATE SKIP LOCKED "+
			"LIMIT $3",
		filterSQL,
	)

	rows, err := conn.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("pgchannel: pollOnce: query: %w", err)
	}
	defer rows.Close()

	out := make([]EventRow, 0, limit)
	for rows.Next() {
		var r EventRow
		var hdrs []byte
		if err := rows.Scan(&r.ID, &r.StreamName, &r.Subject, &hdrs, &r.Payload, &r.CorrelationID, &r.Ts); err != nil {
			return nil, fmt.Errorf("pgchannel: pollOnce: scan: %w", err)
		}
		r.Headers = append(json.RawMessage(nil), hdrs...)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgchannel: pollOnce: rows: %w", err)
	}
	return out, nil
}

// consumerLockKey hashes the (stream, consumer) pair into a single int64
// suitable for pg_try_advisory_lock. FNV-64 is deterministic + collision-
// safe enough for the small cardinality of streams×consumers we expect
// in any single workflow-server deployment (< 10^4).
//
// We deliberately route the unsigned hash through int64 (via two's-complement
// truncation) because pg_advisory_lock's single-arg form is `bigint`.
func consumerLockKey(stream, consumer string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(stream))
	_, _ = h.Write([]byte{0x00}) // null separator to avoid stream/consumer aliasing
	_, _ = h.Write([]byte(consumer))
	return int64(h.Sum64()) //nolint:gosec // two's-complement truncation is intentional
}

// tryConsumerLock attempts to acquire a session-scoped Postgres advisory lock
// keyed on (stream, consumer). Returns acquired=true with a release closure
// when the caller is the sole holder; acquired=false (release will be nil)
// when another holder beat us to it.
//
// The lock is session-scoped: it lives until the holding connection is
// returned to the pool (which release() does explicitly). Callers MUST call
// release() to free the lock + the underlying pool slot, even on the error
// path — failing to do so leaks both.
//
// Per design §1.6 this guarantees that only ONE poller goroutine across all
// workflow-server pods reads from a given consumer at a time.
func tryConsumerLock(ctx context.Context, conn *Connection, stream, consumer string) (bool, func(), error) {
	if conn == nil || conn.pool == nil {
		return false, nil, errors.New("pgchannel: tryConsumerLock: connection is nil")
	}
	if stream == "" || consumer == "" {
		return false, nil, errors.New("pgchannel: tryConsumerLock: stream + consumer are required")
	}
	key := consumerLockKey(stream, consumer)

	// Acquire a dedicated conn so the advisory lock is bound to its lifetime;
	// pool-checked-out conns can otherwise be returned/reused between calls.
	pgConn, err := conn.pool.Acquire(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("pgchannel: tryConsumerLock: acquire: %w", err)
	}

	var acquired bool
	if err := pgConn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		pgConn.Release()
		return false, nil, fmt.Errorf("pgchannel: tryConsumerLock: pg_try_advisory_lock: %w", err)
	}
	if !acquired {
		pgConn.Release()
		return false, nil, nil
	}

	release := func() {
		// Best-effort unlock: if the broker is going down we can't surface
		// errors meaningfully, but we can avoid leaking the pool slot.
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pgConn.Exec(releaseCtx, "SELECT pg_advisory_unlock($1)", key)
		pgConn.Release()
	}
	return true, release, nil
}

// advanceConsumerPosition moves the consumer's position cursor forward.
// The greatest() guard ensures concurrent callers (e.g. an ack racing with
// the polling loop) cannot move the cursor backwards.
//
// Returns an error if no row matched — that indicates the consumer was
// deleted out from under us, which the caller should treat as fatal.
func advanceConsumerPosition(ctx context.Context, conn *Connection, stream, consumer string, newPosition int64) error {
	if conn == nil || conn.pool == nil {
		return errors.New("pgchannel: advanceConsumerPosition: connection is nil")
	}
	tag, err := conn.pool.Exec(ctx,
		"UPDATE eventbus_consumers SET position = greatest(position, $1) WHERE stream_name = $2 AND name = $3",
		newPosition, stream, consumer,
	)
	if err != nil {
		return fmt.Errorf("pgchannel: advanceConsumerPosition: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pgchannel: advanceConsumerPosition: consumer %q on stream %q not found", consumer, stream)
	}
	return nil
}

// incrementDeliveryCount upserts the eventbus_event_deliveries row for
// (stream, consumer, event_id) and returns the resulting delivery_count.
// On first delivery the count is 1; each subsequent call increments by 1
// and stamps last_delivered_at.
//
// Callers compare the returned count against ConsumerConfig.max_deliver
// to enforce the redelivery cap (per design §1.6).
func incrementDeliveryCount(ctx context.Context, conn *Connection, stream, consumer string, eventID int64) (int, error) {
	if conn == nil || conn.pool == nil {
		return 0, errors.New("pgchannel: incrementDeliveryCount: connection is nil")
	}
	var count int
	err := conn.pool.QueryRow(ctx,
		`INSERT INTO eventbus_event_deliveries (stream_name, consumer_name, event_id, delivery_count, first_delivered_at, last_delivered_at)
		 VALUES ($1, $2, $3, 1, NOW(), NOW())
		 ON CONFLICT (stream_name, consumer_name, event_id) DO UPDATE
		   SET delivery_count = eventbus_event_deliveries.delivery_count + 1,
		       last_delivered_at = NOW()
		 RETURNING delivery_count`,
		stream, consumer, eventID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("pgchannel: incrementDeliveryCount: %w", err)
	}
	return count, nil
}

// loadConsumerState reads the position + filter_subject + max_deliver fields
// for a consumer in a single query. Used by Subscribe at startup and at
// the top of each poll iteration so config changes (e.g. a re-issued
// EnsureConsumer) take effect without restart.
func loadConsumerState(ctx context.Context, conn *Connection, stream, consumer string) (position int64, filter string, maxDeliver int, err error) {
	if conn == nil || conn.pool == nil {
		return 0, "", 0, errors.New("pgchannel: loadConsumerState: connection is nil")
	}
	row := conn.pool.QueryRow(ctx,
		"SELECT position, COALESCE(filter_subject, ''), max_deliver FROM eventbus_consumers WHERE stream_name = $1 AND name = $2",
		stream, consumer,
	)
	if err := row.Scan(&position, &filter, &maxDeliver); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, "", 0, fmt.Errorf("pgchannel: loadConsumerState: consumer %q on stream %q not found", consumer, stream)
		}
		return 0, "", 0, fmt.Errorf("pgchannel: loadConsumerState: %w", err)
	}
	return position, filter, maxDeliver, nil
}
