package engine_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"courierbox/internal/clock"
	"courierbox/internal/engine"
	"courierbox/internal/model"
	"courierbox/internal/store"
)

func newEngineStore(t *testing.T, clk *clock.Manual) *store.SQLiteStore {
	t.Helper()
	s, err := store.Open(":memory:", clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func createTarget(t *testing.T, s *store.SQLiteStore, clk *clock.Manual, id, url, secret string, maxc int) {
	t.Helper()
	now := clk.Now()
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
}

var testTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// blockingServer returns a test server whose handler blocks until release is
// closed, and reports peak concurrency.
func blockingServer(t *testing.T, release <-chan struct{}) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var cur, peak int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&cur, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		defer atomic.AddInt32(&cur, -1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, &cur, &peak
}

func closedChan() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func TestEngineDispatchSucceeds(t *testing.T) {
	clk := clock.NewManual(testTime)
	s := newEngineStore(t, clk)
	srv, _, _ := blockingServer(t, closedChan())
	createTarget(t, s, clk, "t1", srv.URL, "secret", 0)
	ev, _, err := s.SubmitEvent(context.Background(), "t1", "k", "e", []byte(`{"a":1}`), clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(s, clk, engine.Config{TestMode: true, MaxAttempts: 3})
	if err := eng.Dispatch(context.Background(), true); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	got, _ := s.GetEvent(context.Background(), ev.ID)
	if got.Status != model.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded", got.Status)
	}
	atts, _ := s.ListAttempts(context.Background(), ev.ID)
	if len(atts) != 1 || atts[0].Status != model.AttemptSucceeded {
		t.Fatalf("attempts = %+v", atts)
	}
}

func TestEngineNonRetryableGoesDead(t *testing.T) {
	clk := clock.NewManual(testTime)
	s := newEngineStore(t, clk)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	createTarget(t, s, clk, "t1", srv.URL, "secret", 0)
	ev, _, _ := s.SubmitEvent(context.Background(), "t1", "k", "e", []byte(`{}`), clk.Now())
	eng := engine.New(s, clk, engine.Config{TestMode: true, MaxAttempts: 3})
	_ = eng.Dispatch(context.Background(), true)
	got, _ := s.GetEvent(context.Background(), ev.ID)
	if got.Status != model.StatusDead {
		t.Fatalf("status = %s, want dead", got.Status)
	}
}

func TestEngineConcurrencyCapsAndHotRefresh(t *testing.T) {
	clk := clock.NewManual(testTime)
	s := newEngineStore(t, clk)
	release := make(chan struct{})
	srv, cur, peak := blockingServer(t, release)
	createTarget(t, s, clk, "A", srv.URL+"/a", "s", 1)
	createTarget(t, s, clk, "B", srv.URL+"/b", "s", 2)
	eng := engine.New(s, clk, engine.Config{TestMode: true, GlobalConcurrency: 3, DefaultTargetConcurrency: 10, MaxAttempts: 3})

	for i := 0; i < 4; i++ {
		s.SubmitEvent(context.Background(), "A", "a"+itoa(i), "e", []byte(`{}`), clk.Now())
		s.SubmitEvent(context.Background(), "B", "b"+itoa(i), "e", []byte(`{}`), clk.Now())
	}
	_ = eng.Dispatch(context.Background(), false)
	waitFor(t, func() bool { return atomic.LoadInt32(cur) >= 3 }, 3*time.Second)

	// Global cap reached; peak must not exceed 3.
	if p := atomic.LoadInt32(peak); p > 3 {
		t.Fatalf("global peak = %d, must not exceed 3", p)
	}
	if p := atomic.LoadInt32(peak); p != 3 {
		t.Fatalf("global peak = %d, want 3", p)
	}
	if eng.InFlight() != 3 {
		t.Fatalf("in flight = %d, want 3", eng.InFlight())
	}

	// Hot-refresh target A's limit up; must NOT cancel in-flight requests.
	eng.RefreshTarget("A", 5)
	if eng.InFlight() != 3 {
		t.Fatalf("in flight changed after hot refresh: %d", eng.InFlight())
	}

	// Release and complete everything.
	close(release)
	if err := eng.Dispatch(context.Background(), true); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if eng.InFlight() != 0 {
		t.Fatalf("in flight = %d after completion, want 0", eng.InFlight())
	}
}

func TestEngineRecoveryRestartsDelivering(t *testing.T) {
	clk := clock.NewManual(testTime)
	dbPath := filepath.Join(t.TempDir(), "rec.db")

	s1, err := store.Open(dbPath, clk)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	srv, cur, _ := blockingServer(t, release)
	createTarget(t, s1, clk, "t1", srv.URL, "secret", 0)
	ev, _, _ := s1.SubmitEvent(context.Background(), "t1", "k", "e", []byte(`{}`), clk.Now())
	eng1 := engine.New(s1, clk, engine.Config{TestMode: true, MaxAttempts: 5})
	_ = eng1.Dispatch(context.Background(), false)
	waitFor(t, func() bool { return atomic.LoadInt32(cur) >= 1 }, 3*time.Second)

	eng1.Halt()
	s1.Close()

	// Reopen same DB; recovery must reset the delivering event.
	s2, err := store.Open(dbPath, clk)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	eng2 := engine.New(s2, clk, engine.Config{TestMode: true, MaxAttempts: 5})
	if n, err := eng2.Recover(context.Background()); err != nil || n != 1 {
		t.Fatalf("recover: n=%d err=%v", n, err)
	}
	got, _ := s2.GetEvent(context.Background(), ev.ID)
	if got.Status == model.StatusDelivering {
		t.Fatal("event still delivering after recovery")
	}

	close(release)
	if err := eng2.Dispatch(context.Background(), true); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	got, _ = s2.GetEvent(context.Background(), ev.ID)
	if got.Status != model.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded", got.Status)
	}
	atts, _ := s2.ListAttempts(context.Background(), ev.ID)
	if len(atts) != 2 {
		t.Fatalf("attempts = %d, want 2 (interrupted + succeeded)", len(atts))
	}
	if atts[0].Status != model.AttemptInterrupted || atts[0].AttemptNumber != 1 {
		t.Fatalf("attempt 1 = %+v", atts[0])
	}
	if atts[1].Status != model.AttemptSucceeded || atts[1].AttemptNumber != 2 {
		t.Fatalf("attempt 2 = %+v", atts[1])
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
