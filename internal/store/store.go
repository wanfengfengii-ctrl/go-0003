// Package store defines the persistence interface for Courierbox and a SQLite
// implementation. The store owns all SQL: targets, events, idempotency records,
// delivery-attempt history and replay deduplication. Every state transition of
// the delivery state machine is persisted through a transactional method here so
// that the engine never holds in-memory state that survives a crash.
package store

import (
	"context"
	"errors"
	"time"

	"courierbox/internal/model"
)

// Sentinel errors mapped by the API layer to HTTP status codes.
var (
	// ErrNotFound is returned when a referenced resource does not exist.
	ErrNotFound = errors.New("resource not found")
	// ErrIdempotencyConflict is returned when an idempotency key is reused with
	// a different payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with different payload")
	// ErrNotDead is returned when a dead-queue operation targets an event that
	// is not in the dead state.
	ErrNotDead = errors.New("event is not in the dead queue")
)

// Claim is the result of transactionally claiming a due event: the event
// (already moved to "delivering" with an incremented attempt count), the
// freshly created attempt audit record, and the target to deliver to.
type Claim struct {
	Event   *model.Event
	Attempt *model.Attempt
	Target  *model.Target
}

// Completion records the outcome of a delivery attempt and the resulting event
// state transition. The store applies it atomically: the attempt audit record is
// finalised and the event status/next_attempt_at is updated.
type Completion struct {
	AttemptID      string
	EventID        string
	FinalStatus    model.AttemptStatus
	EventStatus    model.Status
	ResponseStatus int
	ResponseBody   []byte
	ErrorCategory  string
	ErrorMessage   string
	// NextAttemptAt is set only when EventStatus is retry_wait.
	NextAttemptAt *time.Time
	FinishedAt    time.Time
}

// Store is the persistence interface used by the engine and API.
type Store interface {
	// Targets.
	CreateTarget(ctx context.Context, t *model.Target) error
	GetTarget(ctx context.Context, id string) (*model.Target, error)
	UpdateTarget(ctx context.Context, id string, url *string, secret *string, maxConcurrency *int) (*model.Target, error)

	// Events and idempotency. SubmitEvent returns the event and true if it was
	// newly created; when an idempotency key matches a prior submission with the
	// same payload it returns the original event and false. A key reused with a
	// different payload yields ErrIdempotencyConflict.
	SubmitEvent(ctx context.Context, targetID, key, eventType string, payload []byte, now time.Time) (*model.Event, bool, error)
	GetEvent(ctx context.Context, id string) (*model.Event, error)
	ListAttempts(ctx context.Context, eventID string) ([]*model.Attempt, error)

	// Dispatch. ClaimDueExcluding claims one due event whose target is not in the
	// blocked list, creating a "started" attempt record. It returns (nil, nil)
	// when no event is claimable.
	ClaimDueExcluding(ctx context.Context, now time.Time, blocked []string) (*Claim, error)
	CompleteAttempt(ctx context.Context, c Completion) error

	// RecoverDelivering resets events left in "delivering" from a prior process
	// to "retry_wait" and marks their "started" attempts as "interrupted".
	// Returns the number of events recovered.
	RecoverDelivering(ctx context.Context, now time.Time) (int, error)

	// Dead queue.
	ListDead(ctx context.Context, targetID string, limit, offset int) ([]*model.Event, error)
	DiscardDead(ctx context.Context, eventID string) error
	ReplayDead(ctx context.Context, eventID, opKey string, now time.Time) (*model.Event, bool, error)
	ReplayDeadBatch(ctx context.Context, targetID, opKey string, now time.Time) ([]*model.Event, error)

	Close() error
}
