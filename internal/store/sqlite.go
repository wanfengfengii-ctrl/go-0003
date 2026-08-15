package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"courierbox/internal/clock"
	"courierbox/internal/model"
	"courierbox/migrations"

	// Pure-Go SQLite driver (no CGO required).
	_ "modernc.org/sqlite"
)

// tsFormat is a fixed-width UTC timestamp format so that lexical string
// ordering matches chronological ordering. Every persisted timestamp uses it.
const tsFormat = "2006-01-02T15:04:05.000000000Z07:00"

func formatTime(t time.Time) string { return t.UTC().Format(tsFormat) }

func parseTime(s string) (time.Time, error) { return time.Parse(tsFormat, s) }

// SQLiteStore is the Store implementation backed by SQLite.
type SQLiteStore struct {
	db    *sql.DB
	clock clock.Clock
}

// Open opens (or creates) the database at dbPath, applies the schema migration
// and returns a ready store. Use ":memory:" for an ephemeral database (note
// that a memory database does not survive a reopen; use a file path for
// persistence across restarts).
func Open(dbPath string, clk clock.Clock) (*SQLiteStore, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// A single connection serialises all access, which avoids "database is
	// locked" errors without forcing the caller to manage write contention.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Best-effort pragmas; journal_mode=WAL is a no-op for :memory: databases.
	for _, p := range []string{
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(p); err != nil {
			// foreign_keys failure would be a real problem; the others are not.
			if p == "PRAGMA foreign_keys=ON" {
				db.Close()
				return nil, fmt.Errorf("pragma %q: %w", p, err)
			}
		}
	}

	if _, err := db.Exec(migrations.Schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	if clk == nil {
		clk = clock.Real{}
	}
	return &SQLiteStore{db: db, clock: clk}, nil
}

// Close closes the underlying database handle.
func (s *SQLiteStore) Close() error { return s.db.Close() }

// now returns the store's clock value.
func (s *SQLiteStore) now() time.Time { return s.clock.Now() }

// --- Targets ---------------------------------------------------------------

// CreateTarget persists a new target and its initial secret version.
func (s *SQLiteStore) CreateTarget(ctx context.Context, t *model.Target) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `INSERT INTO targets (id, url, current_secret_version, max_concurrency, created_at, updated_at) VALUES (?,?,?,?,?,?)`,
		t.ID, t.URL, t.CurrentSecretVer, t.MaxConcurrency, formatTime(t.CreatedAt), formatTime(t.UpdatedAt))
	if err != nil {
		return err
	}
	for _, sec := range t.Secrets {
		_, err = tx.ExecContext(ctx, `INSERT INTO target_secrets (target_id, version, secret, created_at) VALUES (?,?,?,?)`,
			t.ID, sec.Version, sec.Secret, formatTime(t.CreatedAt))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetTarget loads a target with all its secret versions.
func (s *SQLiteStore) GetTarget(ctx context.Context, id string) (*model.Target, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return s.getTargetTx(ctx, tx, id)
}

func (s *SQLiteStore) getTargetTx(ctx context.Context, tx *sql.Tx, id string) (*model.Target, error) {
	var t model.Target
	var createdStr, updatedStr string
	err := tx.QueryRowContext(ctx, `SELECT id, url, current_secret_version, max_concurrency, created_at, updated_at FROM targets WHERE id=?`, id).
		Scan(&t.ID, &t.URL, &t.CurrentSecretVer, &t.MaxConcurrency, &createdStr, &updatedStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.CreatedAt, _ = parseTime(createdStr)
	t.UpdatedAt, _ = parseTime(updatedStr)

	rows, err := tx.QueryContext(ctx, `SELECT version, secret FROM target_secrets WHERE target_id=? ORDER BY version`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		var secret string
		if err := rows.Scan(&v, &secret); err != nil {
			return nil, err
		}
		t.Secrets = append(t.Secrets, model.Secret{Version: v, Secret: secret})
	}
	return &t, nil
}

// UpdateTarget updates the mutable fields of a target. When secret is non-nil a
// new secret version is appended and made current (rotation). When maxConcurrency
// is non-nil it overrides the per-target concurrency limit.
func (s *SQLiteStore) UpdateTarget(ctx context.Context, id string, url *string, secret *string, maxConcurrency *int) (*model.Target, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getTargetTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	now := s.now()
	if url != nil {
		t.URL = *url
	}
	if maxConcurrency != nil {
		t.MaxConcurrency = *maxConcurrency
	}
	if secret != nil {
		t.CurrentSecretVer = t.CurrentSecretVer + 1
	}
	t.UpdatedAt = now

	if _, err := tx.ExecContext(ctx, `UPDATE targets SET url=?, current_secret_version=?, max_concurrency=?, updated_at=? WHERE id=?`,
		t.URL, t.CurrentSecretVer, t.MaxConcurrency, formatTime(t.UpdatedAt), id); err != nil {
		return nil, err
	}
	if secret != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO target_secrets (target_id, version, secret, created_at) VALUES (?,?,?,?)`,
			id, t.CurrentSecretVer, *secret, formatTime(now)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return t, nil
}

// --- Events & idempotency --------------------------------------------------

// SubmitEvent creates an event under an idempotency key or returns the original
// event for a matching key+payload.
func (s *SQLiteStore) SubmitEvent(ctx context.Context, targetID, key, eventType string, payload []byte, now time.Time) (*model.Event, bool, error) {
	if key == "" {
		return nil, false, errors.New("empty idempotency key")
	}
	if eventType == "" {
		eventType = "event"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	target, err := s.getTargetTx(ctx, tx, targetID)
	if err != nil {
		return nil, false, err
	}

	hash := model.HashPayload(payload)

	var existingID, existingHash string
	err = tx.QueryRowContext(ctx, `SELECT event_id, payload_hash FROM idempotency_records WHERE target_id=? AND key=?`, targetID, key).
		Scan(&existingID, &existingHash)
	if err == nil {
		if existingHash == hash {
			ev, gerr := s.getEventTx(ctx, tx, existingID)
			if gerr != nil {
				return nil, false, gerr
			}
			return ev, false, nil
		}
		return nil, false, ErrIdempotencyConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}

	id := model.NewID()
	secretVer := target.CurrentSecretVer
	_, err = tx.ExecContext(ctx, `INSERT INTO events (id, target_id, event_type, payload, payload_hash, status, attempt_count, next_attempt_at, cycle, secret_version, created_at, updated_at) VALUES (?,?,?,?,?,?,?,NULL,1,?,?,?)`,
		id, targetID, eventType, payload, hash, string(model.StatusQueued), 0, secretVer, formatTime(now), formatTime(now))
	if err != nil {
		return nil, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO idempotency_records (target_id, key, event_id, payload_hash, created_at) VALUES (?,?,?,?,?)`,
		targetID, key, id, hash, formatTime(now))
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}

	return &model.Event{
		ID:            id,
		TargetID:      targetID,
		EventType:     eventType,
		Payload:       payload,
		PayloadHash:   hash,
		Status:        model.StatusQueued,
		AttemptCount:  0,
		NextAttemptAt: nil,
		Cycle:         1,
		SecretVersion: secretVer,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, true, nil
}

// GetEvent loads an event by id.
func (s *SQLiteStore) GetEvent(ctx context.Context, id string) (*model.Event, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return s.getEventTx(ctx, tx, id)
}

func (s *SQLiteStore) getEventTx(ctx context.Context, tx *sql.Tx, id string) (*model.Event, error) {
	row := tx.QueryRowContext(ctx, `SELECT id, target_id, event_type, payload, payload_hash, status, attempt_count, next_attempt_at, cycle, secret_version, created_at, updated_at FROM events WHERE id=?`, id)
	ev, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return ev, nil
}

// ListAttempts returns the full attempt history for an event, ordered by cycle
// then attempt number.
func (s *SQLiteStore) ListAttempts(ctx context.Context, eventID string) ([]*model.Attempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, event_id, cycle, attempt_number, status, response_status, response_body, error_category, error_message, secret_version, started_at, finished_at, next_attempt_at FROM delivery_attempts WHERE event_id=? ORDER BY cycle, attempt_number`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Attempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// --- Dispatch --------------------------------------------------------------

// ClaimDueExcluding transactionally claims the next due event whose target is
// not in the blocked list, creates a "started" attempt and moves the event to
// "delivering". Returns (nil, nil) when nothing is claimable.
func (s *SQLiteStore) ClaimDueExcluding(ctx context.Context, now time.Time, blocked []string) (*Claim, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	q := `SELECT id, target_id, event_type, payload, payload_hash, status, attempt_count, next_attempt_at, cycle, secret_version, created_at, updated_at FROM events WHERE status IN ('queued','retry_wait') AND (next_attempt_at IS NULL OR next_attempt_at <= ?)`
	args := []interface{}{formatTime(now)}
	if len(blocked) > 0 {
		q += " AND target_id NOT IN ("
		for i, b := range blocked {
			if i > 0 {
				q += ","
			}
			q += "?"
			args = append(args, b)
		}
		q += ")"
	}
	q += " ORDER BY COALESCE(next_attempt_at, '0'), created_at, id LIMIT 1"

	row := tx.QueryRowContext(ctx, q, args...)
	ev, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	target, err := s.getTargetTx(ctx, tx, ev.TargetID)
	if err != nil {
		return nil, err
	}

	newCount := ev.AttemptCount + 1
	secretVer := target.CurrentSecretVer
	nowStr := formatTime(now)
	if _, err := tx.ExecContext(ctx, `UPDATE events SET status='delivering', attempt_count=?, secret_version=?, updated_at=? WHERE id=?`,
		newCount, secretVer, nowStr, ev.ID); err != nil {
		return nil, err
	}

	attemptID := model.NewID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_attempts (id, event_id, cycle, attempt_number, status, response_status, response_body, error_category, error_message, secret_version, started_at, finished_at, next_attempt_at) VALUES (?,?,?,?,?,0,NULL,'','',?, ?,NULL,NULL)`,
		attemptID, ev.ID, ev.Cycle, newCount, string(model.AttemptStarted), secretVer, nowStr); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	ev.Status = model.StatusDelivering
	ev.AttemptCount = newCount
	ev.SecretVersion = secretVer
	ev.UpdatedAt = now
	attempt := &model.Attempt{
		ID:            attemptID,
		EventID:       ev.ID,
		Cycle:         ev.Cycle,
		AttemptNumber: newCount,
		Status:        model.AttemptStarted,
		SecretVersion: secretVer,
		StartedAt:     now,
	}
	return &Claim{Event: ev, Attempt: attempt, Target: target}, nil
}

// CompleteAttempt finalises an attempt audit record and transitions the event.
func (s *SQLiteStore) CompleteAttempt(ctx context.Context, c Completion) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var attemptNext sql.NullString
	if c.NextAttemptAt != nil {
		attemptNext = sql.NullString{String: formatTime(*c.NextAttemptAt), Valid: true}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_attempts SET status=?, response_status=?, response_body=?, error_category=?, error_message=?, finished_at=?, next_attempt_at=? WHERE id=?`,
		string(c.FinalStatus), c.ResponseStatus, c.ResponseBody, c.ErrorCategory, c.ErrorMessage, formatTime(c.FinishedAt), attemptNext, c.AttemptID); err != nil {
		return err
	}

	var eventNext sql.NullString
	if c.EventStatus == model.StatusRetryWait && c.NextAttemptAt != nil {
		eventNext = sql.NullString{String: formatTime(*c.NextAttemptAt), Valid: true}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET status=?, next_attempt_at=?, updated_at=? WHERE id=?`,
		string(c.EventStatus), eventNext, formatTime(c.FinishedAt), c.EventID); err != nil {
		return err
	}
	return tx.Commit()
}

// RecoverDelivering resets events left in "delivering" to "retry_wait" (due
// immediately) and marks their "started" attempts as "interrupted".
func (s *SQLiteStore) RecoverDelivering(ctx context.Context, now time.Time) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	nowStr := formatTime(now)
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_attempts SET status='interrupted', error_category='interrupted', error_message='process restarted', finished_at=? WHERE status='started'`, nowStr); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE events SET status='retry_wait', next_attempt_at=?, updated_at=? WHERE status='delivering'`, nowStr, nowStr)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(n), nil
}

// --- Dead queue ------------------------------------------------------------

// ListDead returns dead events, optionally filtered by target, newest first.
func (s *SQLiteStore) ListDead(ctx context.Context, targetID string, limit, offset int) ([]*model.Event, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT id, target_id, event_type, payload, payload_hash, status, attempt_count, next_attempt_at, cycle, secret_version, created_at, updated_at FROM events WHERE status='dead'`
	args := []interface{}{}
	if targetID != "" {
		q += " AND target_id=?"
		args = append(args, targetID)
	}
	q += " ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// DiscardDead permanently discards a dead event.
func (s *SQLiteStore) DiscardDead(ctx context.Context, eventID string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE events SET status='discarded', next_attempt_at=NULL, updated_at=? WHERE id=? AND status='dead'`, formatTime(s.now()), eventID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var id string
		err := s.db.QueryRowContext(ctx, `SELECT id FROM events WHERE id=?`, eventID).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return ErrNotDead
	}
	return nil
}

// ReplayDead starts a new delivery cycle for a dead event, deduplicating by the
// operation key. The returned bool is true when a new cycle was created.
func (s *SQLiteStore) ReplayDead(ctx context.Context, eventID, opKey string, now time.Time) (*model.Event, bool, error) {
	if opKey == "" {
		return nil, false, errors.New("empty operation key")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	var existingID string
	var existingCycle int
	err = tx.QueryRowContext(ctx, `SELECT event_id, new_cycle FROM replay_operations WHERE key=?`, opKey).Scan(&existingID, &existingCycle)
	if err == nil {
		ev, gerr := s.getEventTx(ctx, tx, existingID)
		if gerr != nil {
			return nil, false, gerr
		}
		return ev, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}

	ev, err := s.getEventTx(ctx, tx, eventID)
	if err != nil {
		return nil, false, err
	}
	if ev.Status != model.StatusDead {
		return nil, false, ErrNotDead
	}
	target, err := s.getTargetTx(ctx, tx, ev.TargetID)
	if err != nil {
		return nil, false, err
	}

	newCycle := ev.Cycle + 1
	nowStr := formatTime(now)
	if _, err := tx.ExecContext(ctx, `UPDATE events SET status='queued', attempt_count=0, next_attempt_at=NULL, cycle=?, secret_version=?, updated_at=? WHERE id=?`,
		newCycle, target.CurrentSecretVer, nowStr, eventID); err != nil {
		return nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO replay_operations (key, event_id, new_cycle, created_at) VALUES (?,?,?,?)`, opKey, eventID, newCycle, nowStr); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}

	ev.Status = model.StatusQueued
	ev.AttemptCount = 0
	ev.NextAttemptAt = nil
	ev.Cycle = newCycle
	ev.SecretVersion = target.CurrentSecretVer
	ev.UpdatedAt = now
	return ev, true, nil
}

// ReplayDeadBatch replays every dead event for a target, deduplicating each via
// a derived operation key.
func (s *SQLiteStore) ReplayDeadBatch(ctx context.Context, targetID, opKey string, now time.Time) ([]*model.Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM events WHERE target_id=? AND status='dead' ORDER BY created_at, id`, targetID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]*model.Event, 0, len(ids))
	for _, id := range ids {
		ev, _, err := s.ReplayDead(ctx, id, opKey+":"+id, now)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

// --- scanners --------------------------------------------------------------

type scanner interface {
	Scan(dest ...interface{}) error
}

func scanEvent(s scanner) (*model.Event, error) {
	var ev model.Event
	var status string
	var nextAttempt sql.NullString
	var createdStr, updatedStr string
	err := s.Scan(&ev.ID, &ev.TargetID, &ev.EventType, &ev.Payload, &ev.PayloadHash, &status, &ev.AttemptCount, &nextAttempt, &ev.Cycle, &ev.SecretVersion, &createdStr, &updatedStr)
	if err != nil {
		return nil, err
	}
	ev.Status = model.Status(status)
	if nextAttempt.Valid {
		if t, err := parseTime(nextAttempt.String); err == nil {
			ev.NextAttemptAt = &t
		}
	}
	ev.CreatedAt, _ = parseTime(createdStr)
	ev.UpdatedAt, _ = parseTime(updatedStr)
	return &ev, nil
}

func scanAttempt(s scanner) (*model.Attempt, error) {
	var a model.Attempt
	var status string
	var startedStr string
	var finishedStr, nextAttempt sql.NullString
	var respBody []byte
	err := s.Scan(&a.ID, &a.EventID, &a.Cycle, &a.AttemptNumber, &status, &a.ResponseStatus, &respBody, &a.ErrorCategory, &a.ErrorMessage, &a.SecretVersion, &startedStr, &finishedStr, &nextAttempt)
	if err != nil {
		return nil, err
	}
	a.Status = model.AttemptStatus(status)
	a.ResponseBody = respBody
	if t, err := parseTime(startedStr); err == nil {
		a.StartedAt = t
	}
	if finishedStr.Valid {
		if t, err := parseTime(finishedStr.String); err == nil {
			a.FinishedAt = &t
		}
	}
	if nextAttempt.Valid {
		if t, err := parseTime(nextAttempt.String); err == nil {
			a.NextAttemptAt = &t
		}
	}
	return &a, nil
}
