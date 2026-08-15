-- Courierbox schema migration 001.
-- Targets, versioned signing secrets, events, idempotency records, delivery
-- attempt history and replay operation deduplication. All timestamps are stored
-- as UTC RFC3339 strings with a fixed nine-digit fractional part so that lexical
-- ordering matches chronological ordering.

CREATE TABLE IF NOT EXISTS targets (
    id                     TEXT PRIMARY KEY,
    url                    TEXT NOT NULL,
    current_secret_version INTEGER NOT NULL DEFAULT 1,
    max_concurrency        INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT NOT NULL,
    updated_at             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS target_secrets (
    target_id  TEXT NOT NULL,
    version    INTEGER NOT NULL,
    secret     TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (target_id, version),
    FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,
    target_id       TEXT NOT NULL,
    event_type      TEXT NOT NULL DEFAULT 'event',
    payload         BLOB NOT NULL,
    payload_hash    TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    cycle           INTEGER NOT NULL DEFAULT 1,
    secret_version  INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_dispatch ON events(status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_events_target   ON events(target_id, status, created_at);

CREATE TABLE IF NOT EXISTS idempotency_records (
    target_id    TEXT NOT NULL,
    key          TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (target_id, key)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id              TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL,
    cycle           INTEGER NOT NULL,
    attempt_number  INTEGER NOT NULL,
    status          TEXT NOT NULL,
    response_status INTEGER NOT NULL DEFAULT 0,
    response_body   BLOB,
    error_category  TEXT NOT NULL DEFAULT '',
    error_message   TEXT NOT NULL DEFAULT '',
    secret_version  INTEGER NOT NULL DEFAULT 1,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    next_attempt_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_attempts_event ON delivery_attempts(event_id, cycle, attempt_number);

CREATE TABLE IF NOT EXISTS replay_operations (
    key        TEXT PRIMARY KEY,
    event_id   TEXT NOT NULL,
    new_cycle  INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_replay_event ON replay_operations(event_id);
