// Package acceptance exercises Courierbox end-to-end through the public HTTP
// API, the deterministic test control plane and a scriptable local receiver.
// No test depends on real wall-clock sleeps for its deterministic assertions.
package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"courierbox/internal/app"
	"courierbox/internal/clock"
	"courierbox/internal/config"
	"courierbox/internal/signing"
	"courierbox/testutil/receiver"
)

// testTime is the fixed epoch all manual clocks start at, so repeated runs are
// byte-for-byte identical in their timestamps.
var testTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// --- response shapes -------------------------------------------------------

type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type targetResp struct {
	ID               string `json:"id"`
	URL              string `json:"url"`
	CurrentSecretVer int    `json:"current_secret_version"`
	MaxConcurrency   int    `json:"max_concurrency"`
}

type eventResp struct {
	ID            string  `json:"id"`
	TargetID      string  `json:"target_id"`
	EventType     string  `json:"event_type"`
	Status        string  `json:"status"`
	AttemptCount  int     `json:"attempt_count"`
	Cycle         int     `json:"cycle"`
	SecretVersion int     `json:"secret_version"`
	NextAttemptAt *string `json:"next_attempt_at"`
}

type attemptResp struct {
	ID             string  `json:"id"`
	Cycle          int     `json:"cycle"`
	AttemptNumber  int     `json:"attempt_number"`
	Status         string  `json:"status"`
	ResponseStatus int     `json:"response_status"`
	ResponseBody   string  `json:"response_body"`
	ErrorCategory  string  `json:"error_category"`
	ErrorMessage   string  `json:"error_message"`
	SecretVersion  int     `json:"secret_version"`
	NextAttemptAt  *string `json:"next_attempt_at"`
}

type replayResp struct {
	EventID string `json:"event_id"`
	Cycle   int    `json:"cycle"`
	CycleID string `json:"cycle_id"`
}

// --- harness ---------------------------------------------------------------

type harness struct {
	t      *testing.T
	clk    *clock.Manual
	app    *app.App
	srv    *httptest.Server
	recv   *receiver.Receiver
	client *http.Client
}

func newHarness(t *testing.T, modify func(*config.Config)) *harness {
	t.Helper()
	clk := clock.NewManual(testTime)
	cfg := config.Config{
		DBPath:                   ":memory:",
		TestMode:                 true,
		GlobalConcurrency:        4,
		DefaultTargetConcurrency: 2,
		BaseDelay:                "1s",
		MaxDelay:                 "10s",
		MaxAttempts:              3,
		RetryAfterMax:            "1m",
		HTTPTimeout:              "30s",
		MaxPayloadSize:           1 << 20,
		MaxResponseBody:          8192,
		PollInterval:             "200ms",
		ShutdownTimeout:          "5s",
	}
	if modify != nil {
		modify(&cfg)
	}
	a, err := app.New(app.Options{Config: cfg, ManualClock: clk})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	srv := httptest.NewServer(a.Handler())
	recv := receiver.New()
	h := &harness{
		t:      t,
		clk:    clk,
		app:    a,
		srv:    srv,
		recv:   recv,
		client: &http.Client{Timeout: 30 * time.Second},
	}
	t.Cleanup(func() {
		srv.Close()
		recv.Close()
		a.Close()
	})
	return h
}

func (h *harness) url(path string) string { return h.srv.URL + path }

func (h *harness) do(method, path string, body []byte, headers map[string]string) (int, []byte) {
	h.t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, h.url(path), rdr)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("do request %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (h *harness) createTarget(url, secret string, maxc int) targetResp {
	h.t.Helper()
	body := map[string]interface{}{"url": url, "secret": secret}
	if maxc > 0 {
		body["max_concurrency"] = maxc
	}
	b, _ := json.Marshal(body)
	status, resp := h.do("POST", "/v1/targets", b, nil)
	if status != http.StatusCreated {
		h.t.Fatalf("create target: status %d body %s", status, resp)
	}
	var tr targetResp
	mustJSON(h.t, resp, &tr)
	return tr
}

func (h *harness) submitEvent(targetID, key string, payload []byte, eventType string) (int, eventResp, apiError) {
	h.t.Helper()
	headers := map[string]string{"Idempotency-Key": key}
	if eventType != "" {
		headers["X-Courierbox-Event-Type"] = eventType
	}
	status, b := h.do("POST", "/v1/targets/"+targetID+"/events", payload, headers)
	if status >= 200 && status < 300 {
		var ev eventResp
		mustJSON(h.t, b, &ev)
		return status, ev, apiError{}
	}
	var e apiError
	_ = json.Unmarshal(b, &e)
	return status, eventResp{}, e
}

func (h *harness) getEvent(id string) eventResp {
	h.t.Helper()
	_, b := h.do("GET", "/v1/events/"+id, nil, nil)
	var ev eventResp
	mustJSON(h.t, b, &ev)
	return ev
}

func (h *harness) getAttempts(id string) []attemptResp {
	h.t.Helper()
	_, b := h.do("GET", "/v1/events/"+id+"/attempts", nil, nil)
	var wrap struct {
		Attempts []attemptResp `json:"attempts"`
	}
	mustJSON(h.t, b, &wrap)
	return wrap.Attempts
}

func (h *harness) listDead(targetID string) []eventResp {
	h.t.Helper()
	path := "/v1/dead?limit=50"
	if targetID != "" {
		path += "&target_id=" + targetID
	}
	_, b := h.do("GET", path, nil, nil)
	var wrap struct {
		Dead []eventResp `json:"dead"`
	}
	mustJSON(h.t, b, &wrap)
	return wrap.Dead
}

func (h *harness) replay(id, opKey string) (int, replayResp) {
	h.t.Helper()
	b, _ := json.Marshal(map[string]string{"operation_key": opKey})
	status, resp := h.do("POST", "/v1/dead/"+id+"/replay", b, nil)
	if status >= 300 {
		h.t.Fatalf("replay: status %d body %s", status, resp)
	}
	var r replayResp
	mustJSON(h.t, resp, &r)
	return status, r
}

func (h *harness) dispatch(wait bool) {
	h.t.Helper()
	b, _ := json.Marshal(map[string]bool{"wait": wait})
	status, resp := h.do("POST", "/_test/dispatch", b, nil)
	if status != http.StatusOK {
		h.t.Fatalf("dispatch(wait=%v): status %d body %s", wait, status, resp)
	}
}

func (h *harness) advance(d string) {
	h.t.Helper()
	b, _ := json.Marshal(map[string]string{"duration": d})
	status, resp := h.do("POST", "/_test/advance", b, nil)
	if status != http.StatusOK {
		h.t.Fatalf("advance(%s): status %d body %s", d, status, resp)
	}
}

func mustJSON(t *testing.T, b []byte, v interface{}) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode %T: %v; body=%s", v, err, b)
	}
}

// waitUntil polls fn until true or timeout. Uses real time only for test
// synchronization, never as part of the deterministic scenario logic.
func waitUntil(t *testing.T, fn func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", msg)
}

// --- acceptance: idempotency ------------------------------------------------

func TestIdempotencyAcceptance(t *testing.T) {
	h := newHarness(t, nil)
	tgt := h.createTarget(h.recv.URL()+"/A", "secret", 0)

	payload := []byte(`{"event":"order.created","order_id":42}`)

	// Submit returns 202 + stable event id.
	status, ev1, _ := h.submitEvent(tgt.ID, "key-1", payload, "order.created")
	if status != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202", status)
	}
	if ev1.ID == "" {
		t.Fatal("empty event id")
	}
	if ev1.EventType != "order.created" {
		t.Fatalf("event type = %s", ev1.EventType)
	}

	// Same target, key, byte-identical payload -> same id.
	status, ev2, _ := h.submitEvent(tgt.ID, "key-1", payload, "order.created")
	if status != http.StatusOK {
		t.Fatalf("resubmit status = %d, want 200", status)
	}
	if ev2.ID != ev1.ID {
		t.Fatalf("id changed: %s vs %s", ev2.ID, ev1.ID)
	}

	// Deliver once.
	h.recv.SetResponses(receiver.Response{Status: http.StatusOK, Body: `{"ok":true}`})
	h.dispatch(true)
	if c := h.recv.CaptureCount(); c != 1 {
		t.Fatalf("delivered %d times, want 1", c)
	}

	// Reuse key with a different payload -> 409 IDEMPOTENCY_CONFLICT.
	status, _, e := h.submitEvent(tgt.ID, "key-1", []byte(`{"different":true}`), "order.created")
	if status != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", status)
	}
	if e.Code != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("error code = %q, want IDEMPOTENCY_CONFLICT", e.Code)
	}
}

// --- acceptance: signature --------------------------------------------------

func TestSignatureAcceptance(t *testing.T) {
	h := newHarness(t, nil)
	const secret = "super-secret-key"
	tgt := h.createTarget(h.recv.URL()+"/sig", secret, 0)

	payload := []byte(`{"fixed":"payload","n":7}`)
	_, ev, _ := h.submitEvent(tgt.ID, "sig-key", payload, "order.created")

	h.recv.SetResponses(receiver.Response{Status: http.StatusOK})
	h.dispatch(true)

	cap := h.recv.LastCapture()
	if cap == nil {
		t.Fatal("no capture")
	}
	// Independently compute HMAC over raw body + timestamp.
	ts, err := strconv.ParseInt(cap.Timestamp, 10, 64)
	if err != nil {
		t.Fatalf("timestamp header %q: %v", cap.Timestamp, err)
	}
	expected := signing.Sign(secret, ts, cap.Body)
	if cap.Signature != expected {
		t.Fatalf("signature mismatch:\n got %q\nwant %q", cap.Signature, expected)
	}
	if cap.Event != "order.created" {
		t.Fatalf("event header = %q", cap.Event)
	}
	// Delivery header must match an attempt id.
	atts := h.getAttempts(ev.ID)
	found := false
	for _, a := range atts {
		if a.ID == cap.Delivery {
			found = true
		}
	}
	if !found {
		t.Fatalf("delivery header %q does not match any attempt id", cap.Delivery)
	}
	if cap.Timestamp == "" {
		t.Fatal("missing timestamp header")
	}
	// The delivered body must be byte-identical to the submitted payload.
	if !bytes.Equal(cap.Body, payload) {
		t.Fatalf("body mismatch:\n got %q\nwant %q", cap.Body, payload)
	}
}

// --- acceptance: retry timing ----------------------------------------------

func TestRetryTimingAcceptance(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.BaseDelay = "1s"
		c.MaxDelay = "10s"
	})
	tgt := h.createTarget(h.recv.URL()+"/retry", "s", 0)
	h.recv.SetResponses(
		receiver.Response{Status: 500},
		receiver.Response{Status: 500},
		receiver.Response{Status: 204},
	)
	_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")

	// First dispatch -> exactly one request (attempt 1, 500).
	h.dispatch(true)
	if c := h.recv.CaptureCount(); c != 1 {
		t.Fatalf("after first dispatch: %d requests, want 1", c)
	}

	// Advance 999ms -> not due, no new request.
	h.advance("999ms")
	if c := h.recv.CaptureCount(); c != 1 {
		t.Fatalf("after 999ms: %d requests, want 1", c)
	}

	// Advance 1ms (total 1s) -> second request.
	h.advance("1ms")
	if c := h.recv.CaptureCount(); c != 2 {
		t.Fatalf("after +1ms: %d requests, want 2", c)
	}

	// Advance 2s -> third request, succeeded.
	h.advance("2s")
	if c := h.recv.CaptureCount(); c != 3 {
		t.Fatalf("after +2s: %d requests, want 3", c)
	}
	got := h.getEvent(ev.ID)
	if got.Status != "succeeded" {
		t.Fatalf("status = %s, want succeeded", got.Status)
	}
	if got.AttemptCount != 3 {
		t.Fatalf("attempt count = %d, want 3", got.AttemptCount)
	}
}

// --- acceptance: dead queue -------------------------------------------------

func TestDeadQueueAcceptance(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.MaxAttempts = 3
	})
	tgt := h.createTarget(h.recv.URL()+"/dead", "s", 0)
	h.recv.SetDefault(receiver.Response{Status: 503}) // always 503
	_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")

	// Three attempts, advancing the clock between each.
	h.dispatch(true) // attempt 1
	h.advance("1s")  // attempt 2
	h.advance("2s")  // attempt 3 -> dead

	got := h.getEvent(ev.ID)
	if got.Status != "dead" {
		t.Fatalf("status = %s, want dead", got.Status)
	}

	atts := h.getAttempts(ev.ID)
	if len(atts) != 3 {
		t.Fatalf("attempts = %d, want 3", len(atts))
	}
	for i, a := range atts {
		if a.AttemptNumber != i+1 {
			t.Fatalf("attempt %d number = %d", i, a.AttemptNumber)
		}
		if a.ResponseStatus != 503 {
			t.Fatalf("attempt %d status code = %d, want 503", i, a.ResponseStatus)
		}
	}
	// Attempts 1 and 2 are retryable and carry a scheduled next_attempt_at; the
	// final dead attempt carries none ("next attempt is empty").
	if atts[0].Status != "failed_retryable" || atts[0].NextAttemptAt == nil {
		t.Fatalf("attempt 1 = %+v, want failed_retryable with next_attempt_at", atts[0])
	}
	if atts[1].Status != "failed_retryable" || atts[1].NextAttemptAt == nil {
		t.Fatalf("attempt 2 = %+v, want failed_retryable with next_attempt_at", atts[1])
	}
	if atts[2].Status != "failed_dead" || atts[2].NextAttemptAt != nil {
		t.Fatalf("last attempt = %+v, want failed_dead with nil next_attempt_at", atts[2])
	}

	// Dead queue contains the event, next attempt empty.
	dead := h.listDead("")
	if len(dead) != 1 || dead[0].ID != ev.ID {
		t.Fatalf("dead queue = %+v", dead)
	}
	if dead[0].NextAttemptAt != nil {
		t.Fatal("dead event should have nil next_attempt_at")
	}

	// Further dispatch sends no request.
	before := h.recv.CaptureCount()
	h.advance("10s")
	if h.recv.CaptureCount() != before {
		t.Fatalf("dead event produced new requests: %d -> %d", before, h.recv.CaptureCount())
	}
}

// --- acceptance: non-retryable & Retry-After -------------------------------

func TestNonRetryableAndRetryAfterAcceptance(t *testing.T) {
	t.Run("400_is_dead_immediately", func(t *testing.T) {
		h := newHarness(t, nil)
		tgt := h.createTarget(h.recv.URL()+"/400", "s", 0)
		h.recv.SetResponses(receiver.Response{Status: 400})
		_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")
		h.dispatch(true)
		got := h.getEvent(ev.ID)
		if got.Status != "dead" {
			t.Fatalf("status = %s, want dead", got.Status)
		}
		atts := h.getAttempts(ev.ID)
		if len(atts) != 1 {
			t.Fatalf("attempts = %d, want 1", len(atts))
		}
		if atts[0].Status != "failed_dead" || atts[0].ErrorCategory != "non_retryable_http" {
			t.Fatalf("attempt = %+v", atts[0])
		}
	})

	t.Run("429_retry_after_7s", func(t *testing.T) {
		h := newHarness(t, nil)
		tgt := h.createTarget(h.recv.URL()+"/429", "s", 0)
		h.recv.SetResponses(
			receiver.Response{Status: 429, RetryAfter: "7"},
			receiver.Response{Status: 204},
		)
		_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")
		h.dispatch(true) // attempt 1: 429, retry after 7s
		if c := h.recv.CaptureCount(); c != 1 {
			t.Fatalf("after dispatch: %d, want 1", c)
		}
		// 6s -> not yet due.
		h.advance("6s")
		if c := h.recv.CaptureCount(); c != 1 {
			t.Fatalf("after 6s: %d, want 1", c)
		}
		// +1s (total 7s) -> second attempt, 204, succeeded.
		h.advance("1s")
		if c := h.recv.CaptureCount(); c != 2 {
			t.Fatalf("after 7s: %d, want 2", c)
		}
		got := h.getEvent(ev.ID)
		if got.Status != "succeeded" {
			t.Fatalf("status = %s, want succeeded", got.Status)
		}
	})
}

// --- acceptance: replay dedup -----------------------------------------------

func TestReplayAcceptance(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.MaxAttempts = 3
	})
	tgt := h.createTarget(h.recv.URL()+"/rep", "s", 0)
	h.recv.SetDefault(receiver.Response{Status: 503})
	_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")

	// Drive to dead.
	h.dispatch(true)
	h.advance("1s")
	h.advance("2s")
	if h.getEvent(ev.ID).Status != "dead" {
		t.Fatal("expected dead")
	}

	// Replay twice with the same op key -> same new cycle id.
	_, r1 := h.replay(ev.ID, "op-1")
	_, r2 := h.replay(ev.ID, "op-1")
	if r1.CycleID != r2.CycleID {
		t.Fatalf("replay cycle ids differ: %s vs %s", r1.CycleID, r2.CycleID)
	}
	if r1.Cycle != 2 {
		t.Fatalf("new cycle = %d, want 2", r1.Cycle)
	}

	// Receiver back to 204, advance+dispatch -> exactly one new request.
	h.recv.SetDefault(receiver.Response{Status: 204})
	before := h.recv.CaptureCount()
	h.advance("0s")
	h.dispatch(true)
	if c := h.recv.CaptureCount(); c != before+1 {
		t.Fatalf("after replay: %d new requests, want 1", c-before)
	}
	got := h.getEvent(ev.ID)
	if got.Status != "succeeded" {
		t.Fatalf("status = %s, want succeeded", got.Status)
	}
	if got.Cycle != 2 {
		t.Fatalf("cycle = %d, want 2", got.Cycle)
	}

	// Original failure history (cycle 1) is still queryable.
	atts := h.getAttempts(ev.ID)
	var cycle1, cycle2 int
	for _, a := range atts {
		switch a.Cycle {
		case 1:
			cycle1++
		case 2:
			cycle2++
		}
	}
	if cycle1 != 3 {
		t.Fatalf("cycle 1 attempts = %d, want 3", cycle1)
	}
	if cycle2 != 1 {
		t.Fatalf("cycle 2 attempts = %d, want 1", cycle2)
	}
}

// --- acceptance: concurrency ------------------------------------------------

func TestConcurrencyAcceptance(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.GlobalConcurrency = 3
		c.DefaultTargetConcurrency = 10
	})
	tgtA := h.createTarget(h.recv.URL()+"/A", "s", 1)
	tgtB := h.createTarget(h.recv.URL()+"/B", "s", 2)

	// Block every request at the gate.
	h.recv.CloseGate()

	submitN := func(tid string, n int) {
		for i := 0; i < n; i++ {
			h.submitEvent(tid, fmt.Sprintf("%s-%d", tid, i), []byte(`{}`), "e")
		}
	}
	submitN(tgtA.ID, 4)
	submitN(tgtB.ID, 4)

	// Dispatch without waiting; requests block at the receiver.
	h.dispatch(false)
	waitUntil(t, func() bool { return h.recv.InFlight() >= 3 }, 5*time.Second, "in-flight reaches 3")

	if p := h.recv.PeakInFlight(); p > 3 {
		t.Fatalf("global peak = %d, must not exceed 3", p)
	}
	if p := h.recv.PeakInFlightFor("/A"); p > 1 {
		t.Fatalf("target A peak = %d, must not exceed 1", p)
	}
	if p := h.recv.PeakInFlightFor("/B"); p > 2 {
		t.Fatalf("target B peak = %d, must not exceed 2", p)
	}
	if p := h.recv.PeakInFlight(); p < 3 {
		t.Fatalf("global peak = %d, expected to reach the cap of 3", p)
	}
	if p := h.recv.PeakInFlightFor("/A"); p != 1 {
		t.Fatalf("target A peak = %d, expected 1", p)
	}
	if p := h.recv.PeakInFlightFor("/B"); p != 2 {
		t.Fatalf("target B peak = %d, expected 2", p)
	}

	// Release the gate and let everything finish.
	h.recv.OpenGate()
	h.recv.SetDefault(receiver.Response{Status: 204})
	h.dispatch(true)

	if c := h.recv.CaptureCount(); c != 8 {
		t.Fatalf("total requests = %d, want 8", c)
	}
}

// --- acceptance: recovery ---------------------------------------------------

func TestRecoveryAcceptance(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	clk := clock.NewManual(testTime)

	cfg := config.Config{
		DBPath:                   dbPath,
		TestMode:                 true,
		GlobalConcurrency:        4,
		DefaultTargetConcurrency: 2,
		BaseDelay:                "1s",
		MaxDelay:                 "10s",
		MaxAttempts:              5,
		RetryAfterMax:            "1m",
		HTTPTimeout:              "30s",
		MaxPayloadSize:           1 << 20,
		MaxResponseBody:          8192,
		PollInterval:             "200ms",
		ShutdownTimeout:          "5s",
	}

	recv := receiver.New()
	defer recv.Close()

	// App 1: register target, submit, start a delivery that blocks.
	a1, err := app.New(app.Options{Config: cfg, ManualClock: clk})
	if err != nil {
		t.Fatalf("app1: %v", err)
	}
	srv1 := httptest.NewServer(a1.Handler())
	defer srv1.Close()

	// Register target via app1's API.
	client := &http.Client{Timeout: 30 * time.Second}
	targetBody, _ := json.Marshal(map[string]interface{}{"url": recv.URL() + "/rec", "secret": "s"})
	req, _ := http.NewRequest("POST", srv1.URL+"/v1/targets", bytes.NewReader(targetBody))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	var tgt targetResp
	json.NewDecoder(resp.Body).Decode(&tgt)
	resp.Body.Close()

	// Block the receiver so the delivery stays in flight.
	recv.CloseGate()
	evBody := []byte(`{"recover":true}`)
	req, _ = http.NewRequest("POST", srv1.URL+"/v1/targets/"+tgt.ID+"/events", bytes.NewReader(evBody))
	req.Header.Set("Idempotency-Key", "rec-1")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	var ev eventResp
	json.NewDecoder(resp.Body).Decode(&ev)
	resp.Body.Close()

	// Dispatch (no wait) to start the delivery, then wait until it is in flight.
	postJSON := func(path string, body interface{}) {
		b, _ := json.Marshal(body)
		r, _ := http.NewRequest("POST", srv1.URL+path, bytes.NewReader(b))
		res, err := client.Do(r)
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		res.Body.Close()
	}
	postJSON("/_test/dispatch", map[string]bool{"wait": false})
	waitUntil(t, func() bool { return recv.InFlight() >= 1 }, 5*time.Second, "delivery in flight")

	// Abrupt crash: halt and close app1, releasing the DB.
	a1.Halt()
	a1.Close()

	// App 2: same DB and clock. Recovery should reset the delivering event.
	a2, err := app.New(app.Options{Config: cfg, ManualClock: clk})
	if err != nil {
		t.Fatalf("app2: %v", err)
	}
	srv2 := httptest.NewServer(a2.Handler())
	defer srv2.Close()

	getEvent := func(id string) eventResp {
		r, _ := http.NewRequest("GET", srv2.URL+"/v1/events/"+id, nil)
		res, _ := client.Do(r)
		var e eventResp
		json.NewDecoder(res.Body).Decode(&e)
		res.Body.Close()
		return e
	}

	// The event must not be stuck in delivering.
	if e := getEvent(ev.ID); e.Status == "delivering" {
		t.Fatal("event is stuck in delivering after recovery")
	}

	// Open the gate, return 204, and dispatch to complete the delivery.
	recv.OpenGate()
	recv.SetDefault(receiver.Response{Status: 204})
	b, _ := json.Marshal(map[string]bool{"wait": true})
	r, _ := http.NewRequest("POST", srv2.URL+"/_test/dispatch", bytes.NewReader(b))
	res, err := client.Do(r)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	res.Body.Close()

	got := getEvent(ev.ID)
	if got.Status != "succeeded" {
		t.Fatalf("status = %s, want succeeded", got.Status)
	}

	// Attempt numbers continuous, no duplicate audit records.
	r, _ = http.NewRequest("GET", srv2.URL+"/v1/events/"+ev.ID+"/attempts", nil)
	res, _ = client.Do(r)
	var wrap struct {
		Attempts []attemptResp `json:"attempts"`
	}
	json.NewDecoder(res.Body).Decode(&wrap)
	res.Body.Close()

	atts := wrap.Attempts
	if len(atts) != 2 {
		t.Fatalf("attempts = %d, want 2 (interrupted + succeeded)", len(atts))
	}
	if atts[0].AttemptNumber != 1 || atts[0].Status != "interrupted" {
		t.Fatalf("attempt 1 = %+v", atts[0])
	}
	if atts[1].AttemptNumber != 2 || atts[1].Status != "succeeded" {
		t.Fatalf("attempt 2 = %+v", atts[1])
	}
	// No duplicate audit records: distinct ids.
	if atts[0].ID == atts[1].ID {
		t.Fatal("duplicate attempt ids")
	}
}

// --- acceptance: error responses -------------------------------------------

func TestErrorResponsesAcceptance(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.MaxPayloadSize = 64
	})

	// Oversized payload -> 413.
	big := make([]byte, 200)
	for i := range big {
		big[i] = 'x'
	}
	tgt := h.createTarget(h.recv.URL()+"/e", "s", 0)
	status, _, e := h.submitEvent(tgt.ID, "k", big, "e")
	if status != 413 {
		t.Fatalf("oversized: status = %d, want 413", status)
	}
	if e.Code != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("oversized code = %q", e.Code)
	}

	// Unknown field -> 400.
	body, _ := json.Marshal(map[string]interface{}{"url": h.recv.URL() + "/u", "secret": "s", "bogus": 1})
	status, b := h.do("POST", "/v1/targets", body, nil)
	if status != 400 {
		t.Fatalf("unknown field: status = %d, want 400", status)
	}
	_ = json.Unmarshal(b, &e)
	if e.Code != "UNKNOWN_FIELD" {
		t.Fatalf("unknown field code = %q, want UNKNOWN_FIELD", e.Code)
	}

	// Invalid URL -> 422.
	body, _ = json.Marshal(map[string]interface{}{"url": "not-a-url", "secret": "s"})
	status, b = h.do("POST", "/v1/targets", body, nil)
	if status != 422 {
		t.Fatalf("invalid url: status = %d, want 422", status)
	}
	_ = json.Unmarshal(b, &e)
	if e.Code != "INVALID_URL" {
		t.Fatalf("invalid url code = %q, want INVALID_URL", e.Code)
	}

	// Missing idempotency key -> 400.
	status, b = h.do("POST", "/v1/targets/"+tgt.ID+"/events", []byte(`{}`), nil)
	if status != 400 {
		t.Fatalf("missing key: status = %d, want 400", status)
	}
	_ = json.Unmarshal(b, &e)
	if e.Code != "MISSING_IDEMPOTENCY_KEY" {
		t.Fatalf("missing key code = %q, want MISSING_IDEMPOTENCY_KEY", e.Code)
	}

	// Non-existent resource -> 404.
	status, b = h.do("GET", "/v1/events/does-not-exist", nil, nil)
	if status != 404 {
		t.Fatalf("not found: status = %d, want 404", status)
	}
	_ = json.Unmarshal(b, &e)
	if e.Code != "NOT_FOUND" {
		t.Fatalf("not found code = %q, want NOT_FOUND", e.Code)
	}
}

func TestTransportErrorAcceptance(t *testing.T) {
	h := newHarness(t, nil)
	// Target points at a closed port -> connection refused.
	closedURL := closedPortURL(t)
	tgt := h.createTarget(closedURL, "s", 0)
	_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")

	// Must not panic the worker; recorded as retryable transport error.
	h.dispatch(true)
	got := h.getEvent(ev.ID)
	if got.Status != "retry_wait" {
		t.Fatalf("status = %s, want retry_wait", got.Status)
	}
	atts := h.getAttempts(ev.ID)
	if len(atts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(atts))
	}
	if atts[0].ErrorCategory != "network_error" {
		t.Fatalf("error category = %q, want network_error", atts[0].ErrorCategory)
	}
}

func closedPortURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return "http://" + addr + "/closed"
}

// --- acceptance: graceful shutdown -----------------------------------------

func TestShutdownAcceptance(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.ShutdownTimeout = "3s"
	})
	tgt := h.createTarget(h.recv.URL()+"/sd", "s", 0)
	h.recv.CloseGate()
	_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")

	h.dispatch(false)
	waitUntil(t, func() bool { return h.recv.InFlight() >= 1 }, 5*time.Second, "delivery in flight")

	// Shutdown with a short grace period; the blocked request cannot finish, so
	// it is cancelled and persisted as a recoverable retry.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := h.app.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	got := h.getEvent(ev.ID)
	if got.Status != "retry_wait" {
		t.Fatalf("status = %s, want retry_wait (recoverable)", got.Status)
	}
	if got.Status == "delivering" {
		t.Fatal("event left in delivering after shutdown")
	}
	// Attempt recorded as a (retryable) failure, not left "started".
	atts := h.getAttempts(ev.ID)
	if len(atts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(atts))
	}
	if atts[0].Status == "started" {
		t.Fatal("attempt left in started state after shutdown")
	}
}

// --- acceptance: test mode gating ------------------------------------------

func TestTestModeGatingAcceptance(t *testing.T) {
	// Production mode (test mode off): /_test/* must 404.
	clk := clock.NewManual(testTime)
	cfg := config.Config{
		DBPath: ":memory:", TestMode: false,
		GlobalConcurrency: 4, DefaultTargetConcurrency: 2,
		BaseDelay: "1s", MaxDelay: "10s", MaxAttempts: 3,
		RetryAfterMax: "1m", HTTPTimeout: "30s",
		MaxPayloadSize: 1 << 20, MaxResponseBody: 8192,
		PollInterval: "200ms", ShutdownTimeout: "5s",
	}
	a, err := app.New(app.Options{Config: cfg, ManualClock: clk})
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	defer a.Close()
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	for _, path := range []string{"/_test/dispatch", "/_test/advance", "/_test/status"} {
		req, _ := http.NewRequest("POST", srv.URL+path, bytes.NewReader([]byte(`{}`)))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404 (test mode off)", path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

// --- acceptance: determinism ------------------------------------------------

func TestDeterminismAcceptance(t *testing.T) {
	run := func(t *testing.T) []attemptResp {
		h := newHarness(t, func(c *config.Config) {
			c.BaseDelay = "1s"
			c.MaxDelay = "10s"
		})
		tgt := h.createTarget(h.recv.URL()+"/det", "s", 0)
		h.recv.SetResponses(
			receiver.Response{Status: 500},
			receiver.Response{Status: 500},
			receiver.Response{Status: 204},
		)
		_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{"det":true}`), "e")
		h.dispatch(true)
		h.advance("999ms")
		h.advance("1ms")
		h.advance("2s")
		if h.getEvent(ev.ID).Status != "succeeded" {
			t.Fatal("expected succeeded")
		}
		return h.getAttempts(ev.ID)
	}

	first := run(t)
	second := run(t)

	if len(first) != len(second) {
		t.Fatalf("attempt counts differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i], second[i]
		if a.AttemptNumber != b.AttemptNumber || a.Status != b.Status ||
			a.ResponseStatus != b.ResponseStatus || a.ErrorCategory != b.ErrorCategory {
			t.Fatalf("attempt %d differs:\n %+v\n %+v", i, a, b)
		}
		if (a.NextAttemptAt == nil) != (b.NextAttemptAt == nil) {
			t.Fatalf("attempt %d next_attempt_at nil mismatch", i)
		}
		if a.NextAttemptAt != nil && *a.NextAttemptAt != *b.NextAttemptAt {
			t.Fatalf("attempt %d next_attempt_at differs: %s vs %s", i, *a.NextAttemptAt, *b.NextAttemptAt)
		}
	}
}

// --- extra: signing header uniqueness / concurrency race-free stress --------

func TestNoWorkerCrashOnBadTarget(t *testing.T) {
	h := newHarness(t, nil)
	// A URL that resolves to a host that refuses / errors.
	tgt := h.createTarget("http://127.0.0.1:1/bad", "s", 0)
	_, ev, _ := h.submitEvent(tgt.ID, "k", []byte(`{}`), "e")
	h.dispatch(true)
	// Service still responsive after a failed delivery.
	if _, b := h.do("GET", "/healthz", nil, nil); !bytes.Contains(b, []byte("ok")) {
		t.Fatalf("service not responsive after bad delivery: %s", b)
	}
	got := h.getEvent(ev.ID)
	if got.Status != "retry_wait" {
		t.Fatalf("status = %s, want retry_wait", got.Status)
	}
}
