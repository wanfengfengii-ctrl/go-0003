package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"courierbox/internal/api"
	"courierbox/internal/clock"
	"courierbox/internal/engine"
	"courierbox/internal/store"
)

type apiFixture struct {
	srv    *httptest.Server
	store  *store.SQLiteStore
	clk    *clock.Manual
	client *http.Client
}

func newAPIFixture(t *testing.T, testMode bool) *apiFixture {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s, err := store.Open(":memory:", clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	eng := engine.New(s, clk, engine.Config{TestMode: testMode, MaxAttempts: 3})
	srv := api.New(s, eng, clk, api.Config{TestMode: testMode, MaxPayloadSize: 64})
	h := httptest.NewServer(srv.Handler())
	t.Cleanup(h.Close)
	return &apiFixture{srv: h, store: s, clk: clk, client: &http.Client{Timeout: 5 * time.Second}}
}

func (f *apiFixture) do(method, path string, body []byte, headers map[string]string) (int, []byte, http.Header) {
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	f.srv.Config.Handler.ServeHTTP(w, r)
	return w.Code, w.Body.Bytes(), w.Header()
}

func mustCreateTarget(t *testing.T, f *apiFixture) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"url": "http://127.0.0.1:9/hook", "secret": "s"})
	code, b, _ := f.do("POST", "/v1/targets", body, nil)
	if code != http.StatusCreated {
		t.Fatalf("create target: %d %s", code, b)
	}
	var resp map[string]interface{}
	json.Unmarshal(b, &resp)
	return resp["id"].(string)
}

func errCode(b []byte) string {
	var e struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(b, &e)
	return e.Code
}

func TestAPIHealthAndReady(t *testing.T) {
	f := newAPIFixture(t, false)
	code, b, _ := f.do("GET", "/healthz", nil, nil)
	if code != 200 || !bytes.Contains(b, []byte("ok")) {
		t.Fatalf("healthz: %d %s", code, b)
	}
	code, b, _ = f.do("GET", "/readyz", nil, nil)
	if code != 200 || !bytes.Contains(b, []byte("ready")) {
		t.Fatalf("readyz: %d %s", code, b)
	}
}

func TestAPIRequestIDPropagated(t *testing.T) {
	f := newAPIFixture(t, false)
	_, _, h := f.do("GET", "/healthz", nil, map[string]string{"X-Courierbox-Request-Id": "rid-123"})
	if got := h.Get("X-Courierbox-Request-Id"); got != "rid-123" {
		t.Fatalf("request id = %q, want rid-123", got)
	}
}

func TestAPIErrorCodes(t *testing.T) {
	f := newAPIFixture(t, false)

	// Invalid URL -> 422 INVALID_URL.
	body, _ := json.Marshal(map[string]string{"url": "not-a-url", "secret": "s"})
	code, b, _ := f.do("POST", "/v1/targets", body, nil)
	if code != 422 || errCode(b) != "INVALID_URL" {
		t.Fatalf("invalid url: %d %s", code, b)
	}

	// Unknown field -> 400 UNKNOWN_FIELD.
	body, _ = json.Marshal(map[string]interface{}{"url": "http://x/y", "secret": "s", "x": 1})
	code, b, _ = f.do("POST", "/v1/targets", body, nil)
	if code != 400 || errCode(b) != "UNKNOWN_FIELD" {
		t.Fatalf("unknown field: %d %s", code, b)
	}

	// Non-existent resource -> 404 NOT_FOUND.
	code, b, _ = f.do("GET", "/v1/events/nope", nil, nil)
	if code != 404 || errCode(b) != "NOT_FOUND" {
		t.Fatalf("not found: %d %s", code, b)
	}

	// Missing idempotency key -> 400 MISSING_IDEMPOTENCY_KEY.
	tgt := mustCreateTarget(t, f)
	code, b, _ = f.do("POST", "/v1/targets/"+tgt+"/events", []byte(`{}`), nil)
	if code != 400 || errCode(b) != "MISSING_IDEMPOTENCY_KEY" {
		t.Fatalf("missing key: %d %s", code, b)
	}

	// Oversized payload -> 413 PAYLOAD_TOO_LARGE (max 64).
	big := make([]byte, 200)
	for i := range big {
		big[i] = 'x'
	}
	code, b, _ = f.do("POST", "/v1/targets/"+tgt+"/events", big, map[string]string{"Idempotency-Key": "k"})
	if code != 413 || errCode(b) != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("oversized: %d %s", code, b)
	}
}

func TestAPIIdempotencyConflict(t *testing.T) {
	f := newAPIFixture(t, true)
	tgt := mustCreateTarget(t, f)
	payload := []byte(`{"p":1}`)
	h := map[string]string{"Idempotency-Key": "k"}
	code, _, _ := f.do("POST", "/v1/targets/"+tgt+"/events", payload, h)
	if code != 202 {
		t.Fatalf("first submit: %d", code)
	}
	code, _, _ = f.do("POST", "/v1/targets/"+tgt+"/events", payload, h)
	if code != 200 {
		t.Fatalf("resubmit: %d, want 200", code)
	}
	code, b, _ := f.do("POST", "/v1/targets/"+tgt+"/events", []byte(`{"p":2}`), h)
	if code != 409 || errCode(b) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflict: %d %s", code, b)
	}
}

func TestAPITestModeGating(t *testing.T) {
	// Production: /_test/* -> 404.
	f := newAPIFixture(t, false)
	for _, p := range []string{"/_test/dispatch", "/_test/advance", "/_test/status"} {
		code, _, _ := f.do("POST", p, []byte(`{}`), nil)
		if code != 404 {
			t.Fatalf("prod %s: %d, want 404", p, code)
		}
	}

	// Test mode: /_test/status works.
	f2 := newAPIFixture(t, true)
	code, b, _ := f2.do("GET", "/_test/status", nil, nil)
	if code != 200 {
		t.Fatalf("test status: %d %s", code, b)
	}
}

// TestAPIRejectsMalformedJSON guards the documented error contract: a body that
// is not exactly one JSON value must be rejected with INVALID_JSON rather than
// silently processing the leading value. json.Decoder.Decode only consumes the
// first value, so trailing data (a second value or stray bytes) used to be
// accepted. Valid requests, the size limit and unknown-field rejection must be
// preserved.
func TestAPIRejectsMalformedJSON(t *testing.T) {
	f := newAPIFixture(t, false)
	tgt := mustCreateTarget(t, f)

	// Trailing data after a valid object must be rejected, not processed.
	trailing := [][]byte{
		[]byte(`{"url":"http://x/y","secret":"s"}garbage`),
		[]byte(`{"url":"http://x/y","secret":"s"}{"a":1}`),
		[]byte(`{"url":"http://x/y","secret":"s"}  {"b":2}`),
	}
	for i, body := range trailing {
		code, b, _ := f.do("POST", "/v1/targets", body, nil)
		if code != 400 || errCode(b) != "INVALID_JSON" {
			t.Fatalf("trailing case %d: got %d %s, want 400 INVALID_JSON", i, code, b)
		}
	}

	// Completely broken JSON is still rejected with INVALID_JSON.
	code, b, _ := f.do("POST", "/v1/targets", []byte(`{not json`), nil)
	if code != 400 || errCode(b) != "INVALID_JSON" {
		t.Fatalf("broken json: got %d %s, want 400 INVALID_JSON", code, b)
	}

	// PATCH with trailing data must not mutate the target.
	code, b, _ = f.do("PATCH", "/v1/targets/"+tgt, []byte(`{"url":"http://updated/y"}extra`), nil)
	if code != 400 || errCode(b) != "INVALID_JSON" {
		t.Fatalf("patch trailing: got %d %s, want 400 INVALID_JSON", code, b)
	}
	code, b, _ = f.do("GET", "/v1/targets/"+tgt, nil, nil)
	if code != 200 {
		t.Fatalf("get target after rejected patch: %d %s", code, b)
	}
	var resp map[string]interface{}
	json.Unmarshal(b, &resp)
	if resp["url"] == "http://updated/y" {
		t.Fatalf("target url mutated despite rejected patch: %v", resp["url"])
	}

	// Replay and batch replay decode the body before touching the store, so
	// trailing data is rejected before any state change.
	code, b, _ = f.do("POST", "/v1/dead/"+tgt+"/replay", []byte(`{"operation_key":"k"}extra`), nil)
	if code != 400 || errCode(b) != "INVALID_JSON" {
		t.Fatalf("replay trailing: got %d %s, want 400 INVALID_JSON", code, b)
	}
	code, b, _ = f.do("POST", "/v1/dead/replay", []byte(`{"target_id":"t","operation_key":"k"}extra`), nil)
	if code != 400 || errCode(b) != "INVALID_JSON" {
		t.Fatalf("replay batch trailing: got %d %s, want 400 INVALID_JSON", code, b)
	}

	// Preserved: a clean, valid request is still accepted.
	code, b, _ = f.do("POST", "/v1/targets", []byte(`{"url":"http://ok/y","secret":"s"}`), nil)
	if code != 201 {
		t.Fatalf("valid create: got %d %s, want 201", code, b)
	}
}

func TestAPISubmitAndDispatch(t *testing.T) {
	f := newAPIFixture(t, true)
	// Target points at a closed port so deliveries fail fast (retryable).
	tgt := mustCreateTarget(t, f)
	payload := []byte(`{"x":1}`)
	code, b, _ := f.do("POST", "/v1/targets/"+tgt+"/events", payload, map[string]string{"Idempotency-Key": "k", "X-Courierbox-Event-Type": "order"})
	if code != 202 {
		t.Fatalf("submit: %d %s", code, b)
	}
	var ev struct {
		ID        string `json:"id"`
		EventType string `json:"event_type"`
		Status    string `json:"status"`
	}
	json.Unmarshal(b, &ev)
	if ev.EventType != "order" {
		t.Fatalf("event type = %s", ev.EventType)
	}
	// Dispatch; the closed port yields a retryable network error.
	code, _, _ = f.do("POST", "/_test/dispatch", []byte(`{"wait":true}`), nil)
	if code != 200 {
		t.Fatalf("dispatch: %d", code)
	}
	// Event should now be in retry_wait.
	code, b, _ = f.do("GET", "/v1/events/"+ev.ID, nil, nil)
	if code != 200 {
		t.Fatalf("get event: %d", code)
	}
	var got struct {
		Status string `json:"status"`
	}
	json.Unmarshal(b, &got)
	if got.Status != "retry_wait" {
		t.Fatalf("status = %s, want retry_wait", got.Status)
	}
	// Attempt history is non-empty.
	code, b, _ = f.do("GET", "/v1/events/"+ev.ID+"/attempts", nil, nil)
	var wrap struct {
		Attempts []struct {
			ErrorCategory string `json:"error_category"`
		} `json:"attempts"`
	}
	json.Unmarshal(b, &wrap)
	if len(wrap.Attempts) != 1 || wrap.Attempts[0].ErrorCategory != "network_error" {
		t.Fatalf("attempts = %+v", wrap.Attempts)
	}
}
