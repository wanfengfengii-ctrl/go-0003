// Package model defines the domain types shared by the store, engine and API
// layers: targets, events, delivery attempts and the status enums that form the
// delivery state machine.
package model

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Status is the lifecycle state of an event.
type Status string

const (
	// StatusQueued: accepted, waiting to be claimed.
	StatusQueued Status = "queued"
	// StatusDelivering: claimed, an HTTP attempt is in flight.
	StatusDelivering Status = "delivering"
	// StatusRetryWait: a retryable failure occurred; waiting until next_attempt_at.
	StatusRetryWait Status = "retry_wait"
	// StatusSucceeded: a delivery attempt returned 2xx.
	StatusSucceeded Status = "succeeded"
	// StatusDead: max attempts exceeded or a non-retryable failure.
	StatusDead Status = "dead"
	// StatusDiscarded: an operator permanently discarded a dead event.
	StatusDiscarded Status = "discarded"
)

// IsTerminal reports whether a status will not transition further without an
// explicit operator action (replay).
func (s Status) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusDead || s == StatusDiscarded
}

// AttemptStatus is the lifecycle state of a single delivery attempt.
type AttemptStatus string

const (
	// AttemptStarted: the attempt record was created and the request is in flight.
	AttemptStarted AttemptStatus = "started"
	// AttemptSucceeded: the attempt returned 2xx.
	AttemptSucceeded AttemptStatus = "succeeded"
	// AttemptFailedRetryable: the attempt failed but will be retried.
	AttemptFailedRetryable AttemptStatus = "failed_retryable"
	// AttemptFailedDead: the attempt failed and pushed the event to the dead queue.
	AttemptFailedDead AttemptStatus = "failed_dead"
	// AttemptInterrupted: the attempt was abandoned (process restart or shutdown).
	AttemptInterrupted AttemptStatus = "interrupted"
)

// Secret is one version of a target's signing secret.
type Secret struct {
	Version int
	Secret  string
}

// Target is a registered delivery destination.
type Target struct {
	ID               string
	URL              string
	CurrentSecretVer int
	Secrets          []Secret
	// MaxConcurrency overrides the engine default when greater than zero.
	MaxConcurrency int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// SecretForVersion returns the secret for a given version.
func (t *Target) SecretForVersion(v int) (Secret, bool) {
	for _, s := range t.Secrets {
		if s.Version == v {
			return s, true
		}
	}
	return Secret{}, false
}

// CurrentSecret returns the target's active secret.
func (t *Target) CurrentSecret() (Secret, bool) {
	return t.SecretForVersion(t.CurrentSecretVer)
}

// Event is a persisted webhook payload awaiting or having undergone delivery.
type Event struct {
	ID            string
	TargetID      string
	EventType     string
	Payload       []byte
	PayloadHash   string
	Status        Status
	AttemptCount  int
	NextAttemptAt *time.Time
	Cycle         int
	SecretVersion int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Attempt is one delivery attempt's audit record.
type Attempt struct {
	ID             string
	EventID        string
	Cycle          int
	AttemptNumber  int
	Status         AttemptStatus
	ResponseStatus int
	ResponseBody   []byte
	ErrorCategory  string
	ErrorMessage   string
	SecretVersion  int
	StartedAt      time.Time
	FinishedAt     *time.Time
	NextAttemptAt  *time.Time
}

// NewID returns a random 16-byte hex identifier. It is suitable for event,
// target and attempt IDs and does not depend on wall-clock monotonicity.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// HashPayload returns the SHA-256 hex digest of body, used for idempotency
// comparison and deduplication.
func HashPayload(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
