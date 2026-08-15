package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"courierbox/internal/api"
	"courierbox/internal/clock"
	"courierbox/internal/engine"
	"courierbox/internal/model"
	"courierbox/internal/store"
)

type countingStore struct {
	store.Store
	createTargetCalls int
}

func (s *countingStore) CreateTarget(ctx context.Context, target *model.Target) error {
	s.createTargetCalls++
	return s.Store.CreateTarget(ctx, target)
}

type apiFixture struct {
	srv           *httptest.Server
	store         *store.SQLiteStore
	countingStore *countingStore
	clk           *clock.Manual
	client        *http.Client
}

func newAPIFixture(t *testing.T, testMode bool) *apiFixture {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s, err := store.Open(":memory:", clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	counting := &countingStore{Store: s}
	eng := engine.New(counting, clk, engine.Config{TestMode: testMode, MaxAttempts: 3})
	srv := api.New(counting, eng, clk, api.Config{TestMode: testMode, MaxPayloadSize: 64})
	h := httptest.NewServer(srv.Handler())
	t.Cleanup(h.Close)
	return &apiFixture{srv: h, store: s, countingStore: counting, clk: clk, client: &http.Client{Timeout: 5 * time.Second}}
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

func TestCreateTargetRejectsTrailingJSON(t *testing.T) {
	valid := []byte(`{"url":"http://x","secret":"s"}`)
	tests := []struct {
		name        string
		body        []byte
		wantStatus  int
		wantCode    string
		wantCreates int
	}{
		{name: "valid object", body: valid, wantStatus: http.StatusCreated, wantCreates: 1},
		{name: "trailing garbage", body: append(append([]byte(nil), valid...), []byte("garbage")...), wantStatus: http.StatusBadRequest, wantCode: "INVALID_JSON"},
		{name: "additional JSON value", body: append(append([]byte(nil), valid...), []byte(` {}`)...), wantStatus: http.StatusBadRequest, wantCode: "INVALID_JSON"},
		{name: "unknown field", body: []byte(`{"url":"http://x","secret":"s","extra":true}`), wantStatus: http.StatusBadRequest, wantCode: "UNKNOWN_FIELD"},
		{name: "oversized body", body: bytes.Repeat([]byte(" "), 65), wantStatus: http.StatusRequestEntityTooLarge, wantCode: "PAYLOAD_TOO_LARGE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newAPIFixture(t, false)
			status, body, _ := f.do(http.MethodPost, "/v1/targets", tt.body, nil)
			if status != tt.wantStatus || (tt.wantCode != "" && errCode(body) != tt.wantCode) {
				t.Errorf("response = %d %s, want status %d code %q", status, body, tt.wantStatus, tt.wantCode)
			}
			if got := f.countingStore.createTargetCalls; got != tt.wantCreates {
				t.Errorf("CreateTarget calls = %d, want %d", got, tt.wantCreates)
			}
		})
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
