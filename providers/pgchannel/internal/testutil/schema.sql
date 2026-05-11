-- pgchannel test schema — mirrors design §1.5 exactly.
--
-- The production migrations live in BMW (PR 3, plan Task 21); this
-- fixture is used only by the testcontainers-backed unit tests in
-- providers/pgchannel. Any drift between this file and BMW's
-- migrations is a bug — keep them in sync.

CREATE TABLE eventbus_streams (
    name TEXT PRIMARY KEY,
    subjects TEXT[] NOT NULL,
    max_age_seconds BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE eventbus_events (
    id BIGSERIAL PRIMARY KEY,
    stream_name TEXT NOT NULL REFERENCES eventbus_streams(name) ON DELETE RESTRICT,
    subject TEXT NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    correlation_id TEXT,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_eventbus_events_stream_id ON eventbus_events (stream_name, id);
CREATE INDEX idx_eventbus_events_subject ON eventbus_events (subject);

CREATE TABLE eventbus_consumers (
    stream_name TEXT NOT NULL REFERENCES eventbus_streams(name) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    position BIGINT NOT NULL DEFAULT 0,
    filter_subject TEXT,
    ack_policy TEXT NOT NULL DEFAULT 'explicit',
    max_deliver INT NOT NULL DEFAULT 5,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_name, name)
);

CREATE TABLE eventbus_event_deliveries (
    stream_name TEXT NOT NULL,
    consumer_name TEXT NOT NULL,
    event_id BIGINT NOT NULL,
    delivery_count INT NOT NULL DEFAULT 1,
    first_delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_name, consumer_name, event_id),
    FOREIGN KEY (stream_name, consumer_name) REFERENCES eventbus_consumers(stream_name, name) ON DELETE RESTRICT,
    FOREIGN KEY (event_id) REFERENCES eventbus_events(id) ON DELETE CASCADE
);
