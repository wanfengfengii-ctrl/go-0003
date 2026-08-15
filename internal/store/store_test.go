package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"courierbox/internal/clock"
	"courierbox/internal/model"
)

func newTestStore(t *testing.T) (*SQLiteStore, *clock.Manual) {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s, err := Open(":memory:", clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, clk
}

func mustCreateTarget(t *testing.T, s *SQLiteStore, id, url, secret string, maxc int) *model.Target {
	t.Helper()
	now := s.now()
	tg := &model.Target{
		ID:               id,
		URL:              url,
		CurrentSecretVer: 1,
		Secrets:          []model.Secret{{Version: 1, Secret: secret}},
		MaxConcurrency:   maxc,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := s.CreateTarget(context.Background(), tg); err != nil {
		t.Fatalf("create target: %v", err)
	}
	return tg
}

func TestSubmitEventIdempotency(t *testing.T) {
	s, clk := newTestStore(t)
	mustCreateTarget(t, s, "t1", "http://example", "secret", 0)
	ctx := context.Background()
	payload := []byte(`{"hello":"world"}`)

	ev1, created, err := s.SubmitEvent(ctx, "t1", "key-1", "order", payload, clk.Now())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !created {
		t.Fatal("first submit should create")
	}

	// Same key + same payload -> same event, not created.
	ev2, created2, err := s.SubmitEvent(ctx, "t1", "key-1", "order", payload, clk.Now())
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if created2 {
		t.Fatal("resubmit should not create")
	}
	if ev1.ID != ev2.ID {
		t.Fatalf("id mismatch: %s vs %s", ev1.ID, ev2.ID)
	}

	// Same key + different payload -> conflict.
	_, _, err = s.SubmitEvent(ctx, "t1", "key-1", "order", []byte(`{"hello":"other"}`), clk.Now())
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestSubmitEventIdempotencyScopedPerTarget(t *testing.T) {
	s, clk := newTestStore(t)
	mustCreateTarget(t, s, "t1", "http://a", "s", 0)
	mustCreateTarget(t, s, "t2", "http://b", "s", 0)
	ctx := context.Background()
	payload := []byte(`{}`)

	a1, _, err := s.SubmitEvent(ctx, "t1", "shared-key", "e", payload, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	a2, created, err := s.SubmitEvent(ctx, "t2", "shared-key", "e", payload, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("same key under a different target should create a new event")
	}
	if a1.ID == a2.ID {
		t.Fatal("different targets should get different event ids")
	}
}

func TestClaimDueAndComplete(t *testing.T) {
	s, clk := newTestStore(t)
	mustCreateTarget(t, s, "t1", "http://example", "secret", 0)
	ctx := context.Background()
	ev, _, err := s.SubmitEvent(ctx, "t1", "k", "e", []byte(`{}`), clk.Now())
	if err != nil {
		t.Fatal(err)
	}

	cl, err := s.ClaimDueExcluding(ctx, clk.Now(), nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if cl == nil {
		t.Fatal("expected a claim")
	}
	if cl.Event.ID != ev.ID {
		t.Fatalf("claimed wrong event: %s", cl.Event.ID)
	}
	if cl.Event.Status != model.StatusDelivering {
		t.Fatalf("status = %s", cl.Event.Status)
	}
	if cl.Event.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d", cl.Event.AttemptCount)
	}
	if cl.Attempt.AttemptNumber != 1 {
		t.Fatalf("attempt number = %d", cl.Attempt.AttemptNumber)
	}

	// No more due events.
	cl2, err := s.ClaimDueExcluding(ctx, clk.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if cl2 != nil {
		t.Fatal("expected no claim")
	}

	// Complete as retry.
	next := clk.Now().Add(2 * time.Second)
	err = s.CompleteAttempt(ctx, Completion{
		AttemptID: cl.Attempt.ID, EventID: ev.ID,
		FinalStatus: model.AttemptFailedRetryable, EventStatus: model.StatusRetryWait,
		ResponseStatus: 500, ResponseBody: []byte("err"), ErrorCategory: "retryable_http",
		NextAttemptAt: &next, FinishedAt: clk.Now(),
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}

	got, err := s.GetEvent(ctx, ev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.StatusRetryWait {
		t.Fatalf("status = %s, want retry_wait", got.Status)
	}
	if got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(next) {
		t.Fatalf("next_attempt_at = %v, want %v", got.NextAttemptAt, next)
	}

	// Not yet due.
	cl3, err := s.ClaimDueExcluding(ctx, clk.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if cl3 != nil {
		t.Fatal("should not claim before next_attempt_at")
	}

	// Due after advancing.
	clk.Advance(2 * time.Second)
	cl4, err := s.ClaimDueExcluding(ctx, clk.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if cl4 == nil || cl4.Attempt.AttemptNumber != 2 {
		t.Fatalf("expected claim #2, got %+v", cl4)
	}
}

func TestClaimExcludesBlockedTargets(t *testing.T) {
	s, clk := newTestStore(t)
	mustCreateTarget(t, s, "t1", "http://a", "s", 0)
	mustCreateTarget(t, s, "t2", "http://b", "s", 0)
	ctx := context.Background()
	_, _, _ = s.SubmitEvent(ctx, "t1", "k1", "e", []byte(`1`), clk.Now())
	_, _, _ = s.SubmitEvent(ctx, "t2", "k2", "e", []byte(`2`), clk.Now())

	cl, err := s.ClaimDueExcluding(ctx, clk.Now(), []string{"t1"})
	if err != nil {
		t.Fatal(err)
	}
	if cl == nil || cl.Event.TargetID != "t2" {
		t.Fatalf("expected t2 claimed, got %+v", cl)
	}
}

func TestRecoverDelivering(t *testing.T) {
	s, clk := newTestStore(t)
	mustCreateTarget(t, s, "t1", "http://example", "secret", 0)
	ctx := context.Background()
	ev, _, err := s.SubmitEvent(ctx, "t1", "k", "e", []byte(`{}`), clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	cl, err := s.ClaimDueExcluding(ctx, clk.Now(), nil)
	if err != nil || cl == nil {
		t.Fatalf("claim: %v %v", cl, err)
	}
	// Simulate a crash: leave the event in "delivering".

	n, err := s.RecoverDelivering(ctx, clk.Now())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered %d, want 1", n)
	}
	got, err := s.GetEvent(ctx, ev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.StatusRetryWait {
		t.Fatalf("status = %s, want retry_wait", got.Status)
	}
	// The interrupted attempt should be recorded.
	atts, err := s.ListAttempts(ctx, ev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(atts) != 1 || atts[0].Status != model.AttemptInterrupted {
		t.Fatalf("attempts = %+v", atts)
	}
	// It should now be claimable again, producing attempt #2.
	cl2, err := s.ClaimDueExcluding(ctx, clk.Now(), nil)
	if err != nil || cl2 == nil {
		t.Fatalf("claim after recover: %v %v", cl2, err)
	}
	if cl2.Attempt.AttemptNumber != 2 {
		t.Fatalf("attempt number = %d, want 2 (continuous)", cl2.Attempt.AttemptNumber)
	}
}

func TestReplayDeadDedup(t *testing.T) {
	s, clk := newTestStore(t)
	mustCreateTarget(t, s, "t1", "http://example", "secret", 0)
	ctx := context.Background()
	ev, _, err := s.SubmitEvent(ctx, "t1", "k", "e", []byte(`{}`), clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Force into dead.
	if err := s.CompleteAttempt(ctx, Completion{
		AttemptID: "a1", EventID: ev.ID, FinalStatus: model.AttemptFailedDead,
		EventStatus: model.StatusDead, ResponseStatus: 400, ErrorCategory: "non_retryable_http",
		FinishedAt: clk.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	r1, created, err := s.ReplayDead(ctx, ev.ID, "op-1", clk.Now())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !created || r1.Cycle != 2 {
		t.Fatalf("replay created=%v cycle=%d", created, r1.Cycle)
	}
	r2, created2, err := s.ReplayDead(ctx, ev.ID, "op-1", clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatal("duplicate replay should not create")
	}
	if r2.Cycle != r1.Cycle {
		t.Fatalf("cycle mismatch: %d vs %d", r2.Cycle, r1.Cycle)
	}

	// Replay of a non-dead event fails.
	if _, _, err := s.ReplayDead(ctx, ev.ID, "op-2", clk.Now()); !errors.Is(err, ErrNotDead) {
		t.Fatalf("expected ErrNotDead, got %v", err)
	}
}

func TestListDeadAndDiscard(t *testing.T) {
	s, clk := newTestStore(t)
	mustCreateTarget(t, s, "t1", "http://example", "secret", 0)
	ctx := context.Background()
	ev, _, _ := s.SubmitEvent(ctx, "t1", "k", "e", []byte(`{}`), clk.Now())
	s.CompleteAttempt(ctx, Completion{
		AttemptID: "a1", EventID: ev.ID, FinalStatus: model.AttemptFailedDead,
		EventStatus: model.StatusDead, ResponseStatus: 400, FinishedAt: clk.Now(),
	})

	list, err := s.ListDead(ctx, "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("dead list len = %d", len(list))
	}
	if err := s.DiscardDead(ctx, ev.ID); err != nil {
		t.Fatalf("discard: %v", err)
	}
	list2, _ := s.ListDead(ctx, "", 10, 0)
	if len(list2) != 0 {
		t.Fatalf("dead list after discard = %d", len(list2))
	}
	if err := s.DiscardDead(ctx, ev.ID); !errors.Is(err, ErrNotDead) {
		t.Fatalf("re-discard = %v, want ErrNotDead", err)
	}
}
