package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"courierbox/internal/clock"
	"courierbox/internal/engine"
	"courierbox/internal/model"
	"courierbox/internal/store"
)

// Sentinel errors bridging the store layer to the API error mapper.
var (
	errNotFound            = store.ErrNotFound
	errIdempotencyConflict = store.ErrIdempotencyConflict
	errNotDead             = store.ErrNotDead
)

const (
	// maxDefaultPayload is the default request body limit for event submission.
	maxDefaultPayload = 1 << 20 // 1 MiB
	// headerRequestID is the correlation id header.
	headerRequestID = "X-Courierbox-Request-Id"
)

// Config configures the API server.
type Config struct {
	MaxPayloadSize int
	TestMode       bool
	// ManualClock is the clock advanced by /_test/advance. Required when TestMode
	// is true.
	ManualClock *clock.Manual
}

// Server is the Courierbox HTTP API.
type Server struct {
	store  store.Store
	engine *engine.Engine
	clk    clock.Clock
	cfg    Config
}

// New creates a Server.
func New(s store.Store, eng *engine.Engine, clk clock.Clock, cfg Config) *Server {
	if cfg.MaxPayloadSize <= 0 {
		cfg.MaxPayloadSize = maxDefaultPayload
	}
	return &Server{store: s, engine: eng, clk: clk, cfg: cfg}
}

// Handler returns the configured HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	mux.HandleFunc("POST /v1/targets", s.handleCreateTarget)
	mux.HandleFunc("GET /v1/targets/{id}", s.handleGetTarget)
	mux.HandleFunc("PATCH /v1/targets/{id}", s.handleUpdateTarget)

	mux.HandleFunc("POST /v1/targets/{id}/events", s.handleSubmitEvent)
	mux.HandleFunc("GET /v1/events/{id}", s.handleGetEvent)
	mux.HandleFunc("GET /v1/events/{id}/attempts", s.handleAttempts)

	mux.HandleFunc("GET /v1/dead", s.handleListDead)
	mux.HandleFunc("POST /v1/dead/{id}/replay", s.handleReplay)
	mux.HandleFunc("POST /v1/dead/replay", s.handleReplayBatch)
	mux.HandleFunc("DELETE /v1/dead/{id}", s.handleDiscard)

	if s.cfg.TestMode {
		mux.HandleFunc("POST /_test/dispatch", s.handleTestDispatch)
		mux.HandleFunc("POST /_test/advance", s.handleTestAdvance)
		mux.HandleFunc("GET /_test/status", s.handleTestStatus)
	}

	return s.withMiddleware(mux)
}

// withMiddleware attaches correlation id and panic recovery.
func (s *Server) withMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(headerRequestID) == "" {
			var b [8]byte
			_, _ = rand.Read(b[:])
			r.Header.Set(headerRequestID, hex.EncodeToString(b[:]))
		}
		w.Header().Set(headerRequestID, r.Header.Get(headerRequestID))
		defer func() {
			if rec := recover(); rec != nil {
				writeError(w, r, CodeInternal, "internal panic")
			}
		}()
		h.ServeHTTP(w, r)
	})
}

func requestID(r *http.Request) string { return r.Header.Get(headerRequestID) }

// --- health ---------------------------------------------------------------

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// Ready once the store answers a trivial query.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.store.ListDead(ctx, "", 1, 0); err != nil {
		writeError(w, r, CodeInternal, "store not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// decodeJSON decodes a JSON body with unknown-field rejection and a size cap.
func decodeJSON(w http.ResponseWriter, r *http.Request, max int64, v interface{}) bool {
	body := http.MaxBytesReader(w, r.Body, max)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		switch {
		case maxBytesError(err):
			writeError(w, r, CodePayloadTooLarge, "request body too large")
		case strings.Contains(err.Error(), "unknown field"):
			writeError(w, r, CodeUnknownField, err.Error())
		default:
			writeError(w, r, CodeInvalidJSON, "invalid JSON: "+err.Error())
		}
		return false
	}
	// A request body must contain exactly one JSON value. Decoder.Decode only
	// consumes the leading value, so a second Decode must hit EOF; anything
	// else (a trailing value or stray bytes) is malformed JSON and must be
	// rejected rather than silently processing the leading value.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		switch {
		case maxBytesError(err):
			writeError(w, r, CodePayloadTooLarge, "request body too large")
		default:
			writeError(w, r, CodeInvalidJSON, "invalid JSON: unexpected trailing data")
		}
		return false
	}
	return true
}

// maxBytesError reports whether err is an http.MaxBytesError (a *http.MaxBytesError
// implements the error interface).
func maxBytesError(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// --- targets ---------------------------------------------------------------

type createTargetReq struct {
	URL            string `json:"url"`
	Secret         string `json:"secret"`
	MaxConcurrency int    `json:"max_concurrency"`
}

type targetResp struct {
	ID               string    `json:"id"`
	URL              string    `json:"url"`
	CurrentSecretVer int       `json:"current_secret_version"`
	MaxConcurrency   int       `json:"max_concurrency"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func toTargetResp(t *model.Target) targetResp {
	return targetResp{
		ID: t.ID, URL: t.URL, CurrentSecretVer: t.CurrentSecretVer,
		MaxConcurrency: t.MaxConcurrency, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
	}
}

func validateURL(raw string) error {
	if raw == "" {
		return errors.New("url is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("url scheme must be http or https")
	}
	if u.Host == "" {
		return errors.New("url host is required")
	}
	return nil
}

func (s *Server) handleCreateTarget(w http.ResponseWriter, r *http.Request) {
	var req createTargetReq
	if !decodeJSON(w, r, int64(s.cfg.MaxPayloadSize), &req) {
		return
	}
	if err := validateURL(req.URL); err != nil {
		writeError(w, r, CodeInvalidURL, err.Error())
		return
	}
	if req.Secret == "" {
		writeError(w, r, CodeBadRequest, "secret is required")
		return
	}
	now := s.clk.Now()
	t := &model.Target{
		ID:               model.NewID(),
		URL:              req.URL,
		CurrentSecretVer: 1,
		Secrets:          []model.Secret{{Version: 1, Secret: req.Secret}},
		MaxConcurrency:   req.MaxConcurrency,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := s.store.CreateTarget(r.Context(), t); err != nil {
		writeError(w, r, CodeInternal, "create target: "+err.Error())
		return
	}
	s.engine.RefreshTarget(t.ID, t.MaxConcurrency)
	writeJSON(w, http.StatusCreated, toTargetResp(t))
}

func (s *Server) handleGetTarget(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, err := s.store.GetTarget(r.Context(), id)
	if err != nil {
		storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTargetResp(t))
}

type updateTargetReq struct {
	URL            *string `json:"url,omitempty"`
	Secret         *string `json:"secret,omitempty"`
	MaxConcurrency *int    `json:"max_concurrency,omitempty"`
}

func (s *Server) handleUpdateTarget(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req updateTargetReq
	if !decodeJSON(w, r, int64(s.cfg.MaxPayloadSize), &req) {
		return
	}
	if req.URL != nil {
		if err := validateURL(*req.URL); err != nil {
			writeError(w, r, CodeInvalidURL, err.Error())
			return
		}
	}
	t, err := s.store.UpdateTarget(r.Context(), id, req.URL, req.Secret, req.MaxConcurrency)
	if err != nil {
		storeError(w, r, err)
		return
	}
	s.engine.RefreshTarget(t.ID, t.MaxConcurrency)
	writeJSON(w, http.StatusOK, toTargetResp(t))
}

// --- events ----------------------------------------------------------------

type eventResp struct {
	ID            string     `json:"id"`
	TargetID      string     `json:"target_id"`
	EventType     string     `json:"event_type"`
	Status        string     `json:"status"`
	AttemptCount  int        `json:"attempt_count"`
	Cycle         int        `json:"cycle"`
	SecretVersion int        `json:"secret_version"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func toEventResp(e *model.Event) eventResp {
	return eventResp{
		ID: e.ID, TargetID: e.TargetID, EventType: e.EventType, Status: string(e.Status),
		AttemptCount: e.AttemptCount, Cycle: e.Cycle, SecretVersion: e.SecretVersion,
		NextAttemptAt: e.NextAttemptAt, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt,
	}
}

func (s *Server) handleSubmitEvent(w http.ResponseWriter, r *http.Request) {
	targetID := r.PathValue("id")
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, r, CodeMissingIdempotencyKey, "Idempotency-Key header is required")
		return
	}
	body := http.MaxBytesReader(w, r.Body, int64(s.cfg.MaxPayloadSize))
	payload, err := io.ReadAll(body)
	if err != nil {
		if maxBytesError(err) {
			writeError(w, r, CodePayloadTooLarge, "event payload too large")
			return
		}
		writeError(w, r, CodeBadRequest, "could not read body: "+err.Error())
		return
	}
	eventType := r.Header.Get("X-Courierbox-Event-Type")
	if eventType == "" {
		eventType = "event"
	}
	ev, created, err := s.store.SubmitEvent(r.Context(), targetID, key, eventType, payload, s.clk.Now())
	if err != nil {
		storeError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, toEventResp(ev))
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ev, err := s.store.GetEvent(r.Context(), id)
	if err != nil {
		storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEventResp(ev))
}

type attemptResp struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	Cycle          int        `json:"cycle"`
	AttemptNumber  int        `json:"attempt_number"`
	Status         string     `json:"status"`
	ResponseStatus int        `json:"response_status"`
	ResponseBody   string     `json:"response_body"`
	ErrorCategory  string     `json:"error_category"`
	ErrorMessage   string     `json:"error_message"`
	SecretVersion  int        `json:"secret_version"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
}

func toAttemptResp(a *model.Attempt) attemptResp {
	return attemptResp{
		ID: a.ID, EventID: a.EventID, Cycle: a.Cycle, AttemptNumber: a.AttemptNumber,
		Status: string(a.Status), ResponseStatus: a.ResponseStatus,
		ResponseBody: string(a.ResponseBody), ErrorCategory: a.ErrorCategory,
		ErrorMessage: a.ErrorMessage, SecretVersion: a.SecretVersion, StartedAt: a.StartedAt,
		FinishedAt: a.FinishedAt, NextAttemptAt: a.NextAttemptAt,
	}
}

func (s *Server) handleAttempts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	atts, err := s.store.ListAttempts(r.Context(), id)
	if err != nil {
		storeError(w, r, err)
		return
	}
	if atts == nil {
		atts = []*model.Attempt{}
	}
	out := make([]attemptResp, 0, len(atts))
	for _, a := range atts {
		out = append(out, toAttemptResp(a))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"attempts": out})
}

// --- dead queue ------------------------------------------------------------

func (s *Server) handleListDead(w http.ResponseWriter, r *http.Request) {
	targetID := r.URL.Query().Get("target_id")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	events, err := s.store.ListDead(r.Context(), targetID, limit, offset)
	if err != nil {
		storeError(w, r, err)
		return
	}
	if events == nil {
		events = []*model.Event{}
	}
	out := make([]eventResp, 0, len(events))
	for _, e := range events {
		out = append(out, toEventResp(e))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"dead": out})
}

type replayReq struct {
	OperationKey string `json:"operation_key"`
}

type replayResp struct {
	EventID string `json:"event_id"`
	Cycle   int    `json:"cycle"`
	CycleID string `json:"cycle_id"`
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req replayReq
	if !decodeJSON(w, r, int64(s.cfg.MaxPayloadSize), &req) {
		return
	}
	if req.OperationKey == "" {
		writeError(w, r, CodeBadRequest, "operation_key is required")
		return
	}
	ev, _, err := s.store.ReplayDead(r.Context(), id, req.OperationKey, s.clk.Now())
	if err != nil {
		storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, replayResp{
		EventID: ev.ID, Cycle: ev.Cycle, CycleID: ev.ID + ":" + strconv.Itoa(ev.Cycle),
	})
}

func (s *Server) handleReplayBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TargetID     string `json:"target_id"`
		OperationKey string `json:"operation_key"`
	}
	if !decodeJSON(w, r, int64(s.cfg.MaxPayloadSize), &req) {
		return
	}
	if req.TargetID == "" || req.OperationKey == "" {
		writeError(w, r, CodeBadRequest, "target_id and operation_key are required")
		return
	}
	events, err := s.store.ReplayDeadBatch(r.Context(), req.TargetID, req.OperationKey, s.clk.Now())
	if err != nil {
		storeError(w, r, err)
		return
	}
	out := make([]replayResp, 0, len(events))
	for _, ev := range events {
		out = append(out, replayResp{EventID: ev.ID, Cycle: ev.Cycle, CycleID: ev.ID + ":" + strconv.Itoa(ev.Cycle)})
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{"replayed": out})
}

func (s *Server) handleDiscard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.DiscardDead(r.Context(), id); err != nil {
		storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "discarded"})
}

// --- test control plane ----------------------------------------------------

func (s *Server) handleTestDispatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Wait *bool `json:"wait,omitempty"`
	}
	_ = decodeJSON(w, r, 1<<16, &req)
	wait := true
	if req.Wait != nil {
		wait = *req.Wait
	}
	if err := s.engine.Dispatch(r.Context(), wait); err != nil {
		writeError(w, r, CodeInternal, "dispatch: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.testStatusMap())
}

func (s *Server) handleTestAdvance(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ManualClock == nil {
		writeError(w, r, CodeInternal, "test mode requires a manual clock")
		return
	}
	var req struct {
		Duration string `json:"duration"`
	}
	if !decodeJSON(w, r, 1<<16, &req) {
		return
	}
	if req.Duration == "" {
		writeError(w, r, CodeBadRequest, "duration is required")
		return
	}
	d, err := time.ParseDuration(req.Duration)
	if err != nil {
		writeError(w, r, CodeBadRequest, "invalid duration: "+err.Error())
		return
	}
	now := s.cfg.ManualClock.Advance(d)
	if err := s.engine.Dispatch(r.Context(), true); err != nil {
		writeError(w, r, CodeInternal, "dispatch: "+err.Error())
		return
	}
	resp := s.testStatusMap()
	resp["now"] = now.UTC().Format(time.RFC3339Nano)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleTestStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.testStatusMap())
}

func (s *Server) testStatusMap() map[string]interface{} {
	m := map[string]interface{}{
		"in_flight": s.engine.InFlight(),
	}
	if s.cfg.ManualClock != nil {
		m["now"] = s.cfg.ManualClock.Now().UTC().Format(time.RFC3339Nano)
	}
	return m
}
