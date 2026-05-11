// Package pgchannel — see subject_match.go for the package doc.
//
// This file is the providers.RuntimeBroker implementation. Connect opens a
// pgxpool against ClusterConfig.dsn; EnsureStream + EnsureConsumer upsert
// the metadata tables; Publish INSERTs into eventbus_events + NOTIFYs the
// per-stream channel; Subscribe acquires a per-consumer advisory lock,
// spawns a LISTEN goroutine, and runs a polling loop that delivers events
// through the MessageHandler with at-least-once semantics + max_deliver
// enforcement; Ack parses the "<consumer>:<id>" token and advances the
// consumer cursor.
package pgchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	eventbusv1 "github.com/GoCodeAlone/workflow-plugin-eventbus/gen"
	"github.com/GoCodeAlone/workflow-plugin-eventbus/providers"
)

// runtime is the providers.RuntimeBroker implementation for pgchannel.
// Per-instance state lives on the returned *Connection; the runtime
// itself is stateless and safe to share across goroutines.
type runtime struct{}

// NewRuntime returns a fresh providers.RuntimeBroker backed by Postgres
// LISTEN/NOTIFY + a polling fallback + advisory locks. Each Connect call
// opens a new pgxpool — runtimes themselves are stateless.
func NewRuntime() providers.RuntimeBroker { return &runtime{} }

// asPG extracts the pgchannel-specific *Connection from an opaque
// providers.Connection. Returns an error when conn originated from a
// different provider or is otherwise unusable.
func asPG(conn providers.Connection) (*Connection, error) {
	if conn == nil {
		return nil, errors.New("pgchannel: connection is nil")
	}
	pc, ok := conn.(*Connection)
	if !ok {
		return nil, fmt.Errorf("pgchannel: connection has provider %q, want \"pgchannel\"", conn.Provider())
	}
	if pc.pool == nil {
		return nil, errors.New("pgchannel: underlying pool is nil")
	}
	return pc, nil
}

// Connect opens a Postgres pool using cfg.dsn. The cfg's provider field
// must be "pgchannel"; broker_target must be "in_process" (other targets
// are deploy-time concerns and have no in-process runtime).
//
// Reads ClusterConfig.poll_interval to seed the per-consumer polling
// cadence; defaults to 1s if unset/unparseable.
func (r *runtime) Connect(ctx context.Context, cfg *eventbusv1.ClusterConfig) (providers.Connection, error) {
	if cfg == nil {
		return nil, errors.New("pgchannel: Connect: cfg is nil")
	}
	if got := cfg.GetProvider(); got != "pgchannel" {
		return nil, fmt.Errorf("pgchannel: Connect: provider = %q, want \"pgchannel\"", got)
	}
	if target := cfg.GetBrokerTarget(); target != "" && target != "in_process" {
		return nil, fmt.Errorf("pgchannel: Connect: broker_target = %q; pgchannel only supports \"in_process\"", target)
	}
	dsn := cfg.GetDsn()
	if dsn == "" {
		return nil, errors.New("pgchannel: Connect: cfg.dsn is empty; populate ClusterConfig.dsn with the Postgres DSN")
	}
	conn, err := OpenConnection(ctx, dsn, defaultMaxConns)
	if err != nil {
		return nil, err
	}
	if raw := cfg.GetPollInterval(); raw != "" {
		if d, perr := time.ParseDuration(raw); perr == nil && d > 0 {
			conn.SetPollInterval(d)
		} else {
			// Don't fail Connect on a malformed poll_interval — default + log.
			// Callers can still recover by re-issuing Connect with a fix; in
			// the meantime polling cadence falls back to defaultPollInterval.
			conn.SetPollInterval(defaultPollInterval)
		}
	}
	return conn, nil
}

// EnsureStream upserts the eventbus_streams row for cfg. Idempotent: a
// repeat call with the same config is a no-op (UPDATE with same values).
//
// max_age maps from cfg.GetMaxAge() (durationpb.Duration) to a BIGINT
// number of seconds; unset → NULL.
func (r *runtime) EnsureStream(ctx context.Context, conn providers.Connection, cfg *eventbusv1.StreamConfig) error {
	pc, err := asPG(conn)
	if err != nil {
		return fmt.Errorf("pgchannel: EnsureStream: %w", err)
	}
	if cfg == nil {
		return errors.New("pgchannel: EnsureStream: cfg is nil")
	}
	if cfg.GetName() == "" {
		return errors.New("pgchannel: EnsureStream: cfg.name is required")
	}
	if len(cfg.GetSubjects()) == 0 {
		return fmt.Errorf("pgchannel: EnsureStream: stream %q has no subjects; at least one subject filter is required", cfg.GetName())
	}
	subjects := append([]string(nil), cfg.GetSubjects()...)

	var maxAgeSeconds any // nil → SQL NULL
	if d := cfg.GetMaxAge(); d != nil && d.IsValid() {
		maxAgeSeconds = int64(d.AsDuration() / time.Second)
	}

	_, err = pc.pool.Exec(ctx,
		`INSERT INTO eventbus_streams (name, subjects, max_age_seconds)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (name) DO UPDATE
		   SET subjects = EXCLUDED.subjects,
		       max_age_seconds = EXCLUDED.max_age_seconds`,
		cfg.GetName(), subjects, maxAgeSeconds,
	)
	if err != nil {
		return fmt.Errorf("pgchannel: EnsureStream %q: %w", cfg.GetName(), err)
	}
	return nil
}

// EnsureConsumer upserts the eventbus_consumers row for (streamName, cfg.name).
// Idempotent: a repeat call updates filter_subject + ack_policy + max_deliver
// but preserves the existing position cursor so an in-flight consumer is not
// rewound.
func (r *runtime) EnsureConsumer(ctx context.Context, conn providers.Connection, streamName string, cfg *eventbusv1.ConsumerConfig) error {
	pc, err := asPG(conn)
	if err != nil {
		return fmt.Errorf("pgchannel: EnsureConsumer: %w", err)
	}
	if streamName == "" {
		return errors.New("pgchannel: EnsureConsumer: streamName is required")
	}
	if cfg == nil {
		return errors.New("pgchannel: EnsureConsumer: cfg is nil")
	}
	if cfg.GetName() == "" {
		return errors.New("pgchannel: EnsureConsumer: cfg.name is required")
	}

	ackPolicy := "explicit"
	switch cfg.GetAckPolicy() {
	case eventbusv1.AckPolicy_ACK_POLICY_NONE:
		ackPolicy = "none"
	case eventbusv1.AckPolicy_ACK_POLICY_ALL:
		ackPolicy = "all"
	}
	maxDeliver := int(cfg.GetMaxDeliver())
	if maxDeliver <= 0 {
		maxDeliver = 5 // schema default; explicit here so EnsureConsumer is round-trippable
	}

	_, err = pc.pool.Exec(ctx,
		`INSERT INTO eventbus_consumers (stream_name, name, filter_subject, ack_policy, max_deliver)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (stream_name, name) DO UPDATE
		   SET filter_subject = EXCLUDED.filter_subject,
		       ack_policy = EXCLUDED.ack_policy,
		       max_deliver = EXCLUDED.max_deliver`,
		streamName, cfg.GetName(), cfg.GetFilterSubject(), ackPolicy, maxDeliver,
	)
	if err != nil {
		return fmt.Errorf("pgchannel: EnsureConsumer %q on stream %q: %w", cfg.GetName(), streamName, err)
	}
	return nil
}

// resolveStreamForSubject finds the stream owning the given subject. Looks
// up by exact-match in eventbus_streams.subjects[] or by prefix-wildcard
// containment. Used by Publish to set stream_name on the event row from a
// PublishRequest that only carries subject.
//
// Returns an error if no stream matches — callers should treat this as a
// configuration bug (publishing to a subject not claimed by any stream).
func resolveStreamForSubject(ctx context.Context, pc *Connection, subject string) (string, error) {
	// Try exact-match first (most common: caller publishes a subject that
	// matches a stream's subjects array literally).
	var name string
	err := pc.pool.QueryRow(ctx,
		`SELECT name FROM eventbus_streams WHERE $1 = ANY(subjects) LIMIT 1`,
		subject,
	).Scan(&name)
	if err == nil {
		return name, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("pgchannel: resolveStreamForSubject %q: exact-match query: %w", subject, err)
	}

	// Fall back to prefix-wildcard: scan all streams; the small cardinality
	// (single-digit streams per deployment) makes a SeqScan acceptable.
	rows, err := pc.pool.Query(ctx, "SELECT name, subjects FROM eventbus_streams")
	if err != nil {
		return "", fmt.Errorf("pgchannel: resolveStreamForSubject %q: wildcard scan: %w", subject, err)
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		var subs []string
		if err := rows.Scan(&n, &subs); err != nil {
			return "", fmt.Errorf("pgchannel: resolveStreamForSubject %q: scan: %w", subject, err)
		}
		for _, pat := range subs {
			if subjectMatchesPattern(subject, pat) {
				return n, nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("pgchannel: resolveStreamForSubject %q: rows: %w", subject, err)
	}
	return "", fmt.Errorf("pgchannel: Publish: no stream claims subject %q (publish before EnsureStream?)", subject)
}

// subjectMatchesPattern is the in-Go counterpart of GenFilter's SQL: it
// returns true when subject matches a stream's subject pattern. Supports
// literal + prefix-wildcard ("pat.>") only (mirrors GenFilter v0.2.0).
func subjectMatchesPattern(subject, pattern string) bool {
	if pattern == subject {
		return true
	}
	if strings.HasSuffix(pattern, ".>") {
		prefix := strings.TrimSuffix(pattern, ".>")
		return strings.HasPrefix(subject, prefix+".")
	}
	return false
}

// notifyChannelName encodes the stream name into a Postgres NOTIFY channel
// identifier. Postgres folds channel names to lowercase; we do the same in
// the publisher so LISTEN side names match.
func notifyChannelName(stream string) string {
	return "eventbus_" + strings.ToLower(stream)
}

// Publish inserts a row into eventbus_events and emits pg_notify on the
// per-stream channel. The insert + notify are wrapped in a single
// transaction so observers cannot wake to a still-uncommitted row.
//
// Returns PublishResponse.Sequence = the assigned BIGINT id, formatted as
// decimal; AckedAt = the server-side ts (so consumers + producers agree on
// ordering even under clock skew between client + DB).
func (r *runtime) Publish(ctx context.Context, conn providers.Connection, req *eventbusv1.PublishRequest) (*eventbusv1.PublishResponse, error) {
	pc, err := asPG(conn)
	if err != nil {
		return nil, fmt.Errorf("pgchannel: Publish: %w", err)
	}
	if req == nil {
		return nil, errors.New("pgchannel: Publish: req is nil")
	}
	if req.GetSubject() == "" {
		return nil, errors.New("pgchannel: Publish: subject is required")
	}

	stream, err := resolveStreamForSubject(ctx, pc, req.GetSubject())
	if err != nil {
		return nil, err
	}

	headers := req.GetHeaders()
	if headers == nil {
		headers = map[string]string{}
	}
	hdrJSON, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("pgchannel: Publish: marshal headers: %w", err)
	}

	var corrID any
	if cid := req.GetCorrelationId(); cid != "" {
		corrID = cid
	}

	tx, err := pc.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgchannel: Publish: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once Commit succeeds

	var (
		id int64
		ts time.Time
	)
	if err := tx.QueryRow(ctx,
		`INSERT INTO eventbus_events (stream_name, subject, headers, payload, correlation_id)
		 VALUES ($1, $2, $3::jsonb, $4, $5)
		 RETURNING id, ts`,
		stream, req.GetSubject(), string(hdrJSON), req.GetPayload(), corrID,
	).Scan(&id, &ts); err != nil {
		return nil, fmt.Errorf("pgchannel: Publish: insert: %w", err)
	}

	// pg_notify is parameterised (channel + payload) so the channel name
	// cannot collide with SQL injection. Payload carries the row id so
	// LISTEN-side wakers know which row to read (informational; the
	// poller still does a real SELECT to honour FOR UPDATE SKIP LOCKED).
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannelName(stream), strconv.FormatInt(id, 10)); err != nil {
		return nil, fmt.Errorf("pgchannel: Publish: pg_notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("pgchannel: Publish: commit: %w", err)
	}

	return &eventbusv1.PublishResponse{
		Sequence: strconv.FormatInt(id, 10),
		AckedAt:  ts.UTC().Format(time.RFC3339),
	}, nil
}

// rowToMessage converts an EventRow + consumer name into the proto Message
// emitted by Subscribe. The ack_token format is "<consumer>:<id>" — Ack
// parses it back out via splitN(":", 2).
func rowToMessage(consumer string, r EventRow) (*eventbusv1.Message, error) {
	msg := &eventbusv1.Message{
		Subject:     r.Subject,
		Payload:     append([]byte(nil), r.Payload...),
		Sequence:    strconv.FormatInt(r.ID, 10),
		PublishedAt: r.Ts.UTC().Format(time.RFC3339),
		AckToken:    consumer + ":" + strconv.FormatInt(r.ID, 10),
	}
	if len(r.Headers) > 0 {
		hdrs := map[string]string{}
		// Empty JSON object → no headers; tolerate "null" / "{}" / etc.
		s := strings.TrimSpace(string(r.Headers))
		if s != "" && s != "null" && s != "{}" {
			if err := json.Unmarshal(r.Headers, &hdrs); err != nil {
				return nil, fmt.Errorf("pgchannel: unmarshal headers for event %d: %w", r.ID, err)
			}
		}
		// Surface correlation_id alongside headers so consumers that read
		// it from the header map (NATS-compatible pattern) still find it.
		if r.CorrelationID != "" {
			hdrs["X-Correlation-Id"] = r.CorrelationID
		}
		if len(hdrs) > 0 {
			msg.Headers = hdrs
		}
	} else if r.CorrelationID != "" {
		msg.Headers = map[string]string{"X-Correlation-Id": r.CorrelationID}
	}
	return msg, nil
}

// subscribeBatchSize caps the per-poll fetch — keeps each iteration's
// transaction short so SKIP LOCKED doesn't accumulate row locks.
const subscribeBatchSize = 32

// listenHeartbeat is the periodic ping interval used to detect a silent
// dead LISTEN connection. 30s matches design §1.6 errata Task 8.
const listenHeartbeat = 30 * time.Second

// Subscribe acquires a per-consumer advisory lock, opens a LISTEN
// connection for the stream's NOTIFY channel, and runs a polling loop
// that delivers events through handler. Blocks until ctx is cancelled.
//
// Delivery semantics:
//   - At-least-once: handler may be invoked more than once per event if
//     the previous attempt errored or the process crashed before
//     advanceConsumerPosition committed.
//   - max_deliver: when delivery_count reaches the configured max, the
//     event is skipped (position advanced past it) and a dead-letter
//     log line is emitted (explicit DLQ deferred per design §1.6).
//   - Filter: applied via GenFilter inside the polling SELECT.
//   - LISTEN: best-effort wake-up signal; missing a notification only
//     delays delivery to the next poll tick.
//
// LISTEN reconnect: if the dedicated LISTEN conn dies (network partition,
// pg restart), Subscribe re-acquires a fresh conn from the pool and
// re-issues LISTEN. Polling continues uninterrupted.
func (r *runtime) Subscribe(ctx context.Context, conn providers.Connection, streamName, consumerName string, handler providers.MessageHandler) error {
	pc, err := asPG(conn)
	if err != nil {
		return fmt.Errorf("pgchannel: Subscribe: %w", err)
	}
	if streamName == "" {
		return errors.New("pgchannel: Subscribe: streamName is required")
	}
	if consumerName == "" {
		return errors.New("pgchannel: Subscribe: consumerName is required")
	}
	if handler == nil {
		return errors.New("pgchannel: Subscribe: handler is nil")
	}

	// 1. Acquire the per-consumer advisory lock. We re-try in a backoff
	// loop while ctx is alive; if another pod already holds the lock we
	// wait rather than fail — Subscribe is a long-lived call.
	var releaseLock func()
	for {
		acquired, release, err := tryConsumerLock(ctx, pc, streamName, consumerName)
		if err != nil {
			return fmt.Errorf("pgchannel: Subscribe: acquire lock: %w", err)
		}
		if acquired {
			releaseLock = release
			break
		}
		// Lock held elsewhere — back off and retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pc.PollInterval()):
		}
	}
	defer releaseLock()

	// 2. Start LISTEN goroutine. Notifications wake the polling loop via
	// notifyCh; on error / cancellation the goroutine returns and the
	// polling loop falls back to interval-only wake-ups.
	notifyCh := make(chan struct{}, 1)
	listenCtx, cancelListen := context.WithCancel(ctx)
	defer cancelListen()
	go runListenLoop(listenCtx, pc, streamName, notifyCh)

	// 3. Polling loop. Each iteration: load consumer state → pollOnce →
	// for each row deliver via handler + bump delivery_count + advance
	// position OR (on error) leave position and re-deliver next tick.
	ticker := time.NewTicker(pc.PollInterval())
	defer ticker.Stop()
	for {
		if err := pollAndDeliver(ctx, pc, streamName, consumerName, handler); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-notifyCh:
			// Wake immediately on notification; the next iteration will
			// re-arm the ticker on the same cadence.
		}
	}
}

// pollAndDeliver runs one iteration of the Subscribe polling loop:
// load state, fetch a batch, deliver each event, advance position. Errors
// are returned to the caller; non-fatal cases (handler errors → nak)
// stay inside the iteration.
func pollAndDeliver(ctx context.Context, pc *Connection, streamName, consumerName string, handler providers.MessageHandler) error {
	position, filter, maxDeliver, err := loadConsumerState(ctx, pc, streamName, consumerName)
	if err != nil {
		return err
	}
	rows, err := pollOnce(ctx, pc, streamName, filter, position, subscribeBatchSize)
	if err != nil {
		return err
	}
	for _, row := range rows {
		// Track delivery count BEFORE invoking the handler so a process
		// crash mid-handler still costs one attempt.
		count, err := incrementDeliveryCount(ctx, pc, streamName, consumerName, row.ID)
		if err != nil {
			return err
		}
		if count > maxDeliver {
			// Dead-letter: advance past it. Explicit DLQ table deferred
			// per design §1.6; the delivery_count row remains as audit
			// trail. (No structured logger in this layer — caller can
			// observe via the deliveries table.)
			if err := advanceConsumerPosition(ctx, pc, streamName, consumerName, row.ID); err != nil {
				return err
			}
			continue
		}
		msg, err := rowToMessage(consumerName, row)
		if err != nil {
			return err
		}
		if hErr := handler(ctx, msg); hErr != nil {
			// Handler nak: leave position; next poll will re-deliver.
			// We intentionally do NOT return hErr here — Subscribe is a
			// long-lived loop and a single handler failure must not
			// unwind it. The delivery_count guard prevents infinite
			// re-delivery.
			return nil
		}
		if err := advanceConsumerPosition(ctx, pc, streamName, consumerName, row.ID); err != nil {
			return err
		}
	}
	return nil
}

// runListenLoop runs the dedicated LISTEN goroutine for Subscribe. It
// acquires a fresh pool conn, issues LISTEN, then alternates between
// WaitForNotification (with a heartbeat-bounded deadline) and a SELECT 1
// probe. On any error it sleeps briefly and reconnects.
//
// The function returns only when ctx is cancelled; intermediate errors
// are absorbed (reconnect) because LISTEN is best-effort — polling is the
// authoritative delivery path.
func runListenLoop(ctx context.Context, pc *Connection, stream string, notifyCh chan<- struct{}) {
	channel := notifyChannelName(stream)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := runListenSession(ctx, pc, channel, notifyCh); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			// Reconnect after a short pause; do not flood pg on a flapping
			// connection.
			select {
			case <-ctx.Done():
				return
			case <-time.After(pc.PollInterval()):
			}
		}
	}
}

// runListenSession holds one LISTEN session against a freshly-acquired
// pool conn. Returns when ctx is cancelled or the underlying connection
// dies; the surrounding runListenLoop handles reconnection.
func runListenSession(ctx context.Context, pc *Connection, channel string, notifyCh chan<- struct{}) error {
	pgConn, err := pc.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer pgConn.Release()

	// LISTEN names are identifiers, not parameters — quote with %q-like
	// encoding. pg.Identifier{}.Sanitize() would be ideal; in absence
	// of a pgx import for that, we constrain channel name format in
	// notifyChannelName ("eventbus_" + lowercase stream) and reject any
	// channel name that contains non-identifier characters defensively.
	if !isSafeIdentifier(channel) {
		return fmt.Errorf("pgchannel: unsafe LISTEN channel %q", channel)
	}
	if _, err := pgConn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}

	rawConn := pgConn.Conn().PgConn()
	for {
		// Bound each Wait to listenHeartbeat so we detect silent connection
		// death even when the channel is idle. On expiry we issue SELECT 1
		// as an explicit health probe.
		waitCtx, cancel := context.WithTimeout(ctx, listenHeartbeat)
		err := rawConn.WaitForNotification(waitCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Could be a deadline (heartbeat) or genuine conn death; in
			// both cases probe the conn. On probe failure surface to caller.
			probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
			_, probeErr := pgConn.Exec(probeCtx, "SELECT 1")
			probeCancel()
			if probeErr != nil {
				return probeErr
			}
			// Healthy after heartbeat; loop and re-arm.
			continue
		}
		// Notification arrived — wake the polling loop, non-blocking.
		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}
}

// isSafeIdentifier checks that s contains only [a-z0-9_]. Used as a
// defence-in-depth guard before splicing channel names into a LISTEN
// statement (which must be an identifier, not a parameter).
func isSafeIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '_':
		default:
			return false
		}
	}
	return true
}

// Ack parses ackToken as "<consumer>:<id>" and advances the consumer's
// position cursor past id. The stream name is looked up from the
// eventbus_consumers row (there is exactly one row per (stream, consumer)
// PK; consumer names are unique within a stream).
//
// Used by step.eventbus.ack for explicit-ack flows. The Subscribe loop's
// own auto-advance bypasses Ack entirely; this path exists only for
// pull-mode callers that ack outside the handler.
func (r *runtime) Ack(ctx context.Context, conn providers.Connection, ackToken string) error {
	pc, err := asPG(conn)
	if err != nil {
		return fmt.Errorf("pgchannel: Ack: %w", err)
	}
	if ackToken == "" {
		return errors.New("pgchannel: Ack: ackToken is required")
	}
	consumer, idStr, ok := strings.Cut(ackToken, ":")
	if !ok || consumer == "" || idStr == "" {
		return fmt.Errorf("pgchannel: Ack: malformed ackToken %q (want \"<consumer>:<id>\")", ackToken)
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return fmt.Errorf("pgchannel: Ack: parse id from %q: %w", ackToken, err)
	}

	// Resolve the stream from the consumer name. If a consumer name is
	// reused across streams (the schema permits it), Ack updates ALL
	// matching rows in one query — caller is responsible for unique
	// naming across streams to avoid that scenario.
	tag, err := pc.pool.Exec(ctx,
		"UPDATE eventbus_consumers SET position = greatest(position, $1) WHERE name = $2",
		id, consumer,
	)
	if err != nil {
		return fmt.Errorf("pgchannel: Ack: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pgchannel: Ack: consumer %q not found", consumer)
	}
	return nil
}

// Compile-time assertion that runtime satisfies providers.RuntimeBroker.
var _ providers.RuntimeBroker = (*runtime)(nil)

// Re-export the pgconn import so go vet doesn't strip it; rawConn typing
// uses *pgconn.PgConn via PgConn().
var _ = (*pgconn.PgConn)(nil)
