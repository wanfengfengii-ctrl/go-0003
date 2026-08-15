// Package engine drives the Courierbox delivery state machine. It claims due
// events from the store, delivers them over HTTP with signing headers, enforces
// global and per-target concurrency limits, applies the retry policy, persists
// every transition, recovers in-flight deliveries on startup and supports
// graceful shutdown.
//
// In production the engine runs an autonomous claim loop. In test mode the loop
// is disabled and the caller drives progress explicitly through Dispatch and the
// test control plane, so behaviour is deterministic and never depends on real
// wall-clock sleeps.
package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"courierbox/internal/clock"
	"courierbox/internal/model"
	"courierbox/internal/retry"
	"courierbox/internal/signing"
	"courierbox/internal/store"
)

// Config configures an Engine.
type Config struct {
	// GlobalConcurrency is the maximum number of in-flight deliveries across all
	// targets.
	GlobalConcurrency int
	// DefaultTargetConcurrency is the per-target cap when a target has no
	// explicit override.
	DefaultTargetConcurrency int
	// BaseDelay is the first retry backoff.
	BaseDelay time.Duration
	// MaxDelay caps the exponential backoff.
	MaxDelay time.Duration
	// MaxAttempts is the maximum number of attempts before an event is dead.
	MaxAttempts int
	// RetryAfterMax caps a 429 Retry-After value. Zero means uncapped.
	RetryAfterMax time.Duration
	// HTTPTimeout is the per-request timeout.
	HTTPTimeout time.Duration
	// MaxResponseBody is the largest response body stored in an audit record.
	MaxResponseBody int
	// PollInterval is how often the production claim loop runs.
	PollInterval time.Duration
	// ShutdownDrainTimeout bounds how long Shutdown waits for results to arrive
	// after cancelling in-flight requests.
	ShutdownDrainTimeout time.Duration
	// TestMode disables the autonomous loop and enables Dispatch/Halt semantics.
	TestMode bool
}

// Defaults applied when Config fields are zero.
func (c Config) withDefaults() Config {
	if c.GlobalConcurrency <= 0 {
		c.GlobalConcurrency = 16
	}
	if c.DefaultTargetConcurrency <= 0 {
		c.DefaultTargetConcurrency = 4
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = time.Second
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 5 * time.Minute
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 25
	}
	if c.HTTPTimeout <= 0 {
		c.HTTPTimeout = 30 * time.Second
	}
	if c.MaxResponseBody <= 0 {
		c.MaxResponseBody = 8 * 1024
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.ShutdownDrainTimeout <= 0 {
		c.ShutdownDrainTimeout = 5 * time.Second
	}
	return c
}

type targetInfo struct {
	limit int
}

type attemptResult struct {
	claim   *store.Claim
	outcome retry.Outcome
	status  int
	body    []byte
	err     error
}

// Engine is the delivery engine.
type Engine struct {
	store store.Store
	clk   clock.Clock
	cfg   Config
	cli   *http.Client

	mu           sync.Mutex
	inFlight     int
	perTarget    map[string]int
	targetInfo   map[string]targetInfo
	shuttingDown bool
	halted       bool

	results chan attemptResult

	baseCtx    context.Context
	cancelBase context.CancelFunc

	deliverCtx    context.Context
	cancelDeliver context.CancelFunc

	runWG sync.WaitGroup
}

// New creates an Engine. The engine is not started; call Start to begin the
// autonomous loop (production) or Dispatch to drive it (test mode).
func New(s store.Store, clk clock.Clock, cfg Config) *Engine {
	cfg = cfg.withDefaults()
	if clk == nil {
		clk = clock.Real{}
	}
	e := &Engine{
		store:      s,
		clk:        clk,
		cfg:        cfg,
		cli:        &http.Client{Timeout: cfg.HTTPTimeout},
		perTarget:  make(map[string]int),
		targetInfo: make(map[string]targetInfo),
		results:    make(chan attemptResult, 4096),
	}
	e.baseCtx, e.cancelBase = context.WithCancel(context.Background())
	e.deliverCtx, e.cancelDeliver = context.WithCancel(context.Background())
	return e
}

// Recover should be called at startup to reset deliveries left in "delivering"
// from a prior process into a retryable state.
func (e *Engine) Recover(ctx context.Context) (int, error) {
	return e.store.RecoverDelivering(ctx, e.clk.Now())
}

// Start begins the autonomous claim loop. It is a no-op in test mode.
func (e *Engine) Start(ctx context.Context) {
	if e.cfg.TestMode {
		return
	}
	e.runWG.Add(1)
	go e.run(ctx)
}

// run is the production claim/result loop.
func (e *Engine) run(ctx context.Context) {
	defer e.runWG.Done()
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-e.results:
			e.handleResult(ctx, r)
			e.tryClaim(ctx)
		case <-ticker.C:
			e.tryClaim(ctx)
		}
	}
}

// IsShuttingDown reports whether Shutdown has been initiated.
func (e *Engine) IsShuttingDown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shuttingDown
}

// InFlight returns the number of in-flight deliveries.
func (e *Engine) InFlight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.inFlight
}

// RefreshTarget updates the cached per-target concurrency limit. Hot updates do
// not cancel in-flight requests; they only affect future claims.
func (e *Engine) RefreshTarget(id string, maxConcurrency int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	limit := maxConcurrency
	if limit <= 0 {
		limit = e.cfg.DefaultTargetConcurrency
	}
	e.targetInfo[id] = targetInfo{limit: limit}
}

func (e *Engine) targetLimit(id string) int {
	if info, ok := e.targetInfo[id]; ok {
		return info.limit
	}
	return e.cfg.DefaultTargetConcurrency
}

func (e *Engine) blockedTargets() []string {
	var blocked []string
	for id, count := range e.perTarget {
		if count >= e.targetLimit(id) {
			blocked = append(blocked, id)
		}
	}
	return blocked
}

// tryClaim claims and dispatches due events while capacity is available.
func (e *Engine) tryClaim(ctx context.Context) {
	for {
		if e.IsShuttingDown() {
			return
		}
		claimed, err := e.claim(ctx)
		if err != nil {
			return
		}
		if !claimed {
			return
		}
	}
}

// claim attempts to claim and dispatch a single due event respecting the
// concurrency limits. It returns false when nothing is claimable or capacity is
// exhausted.
func (e *Engine) claim(ctx context.Context) (bool, error) {
	e.mu.Lock()
	if e.shuttingDown {
		e.mu.Unlock()
		return false, nil
	}
	if e.inFlight >= e.cfg.GlobalConcurrency {
		e.mu.Unlock()
		return false, nil
	}
	blocked := e.blockedTargets()
	e.mu.Unlock()

	cl, err := e.store.ClaimDueExcluding(ctx, e.clk.Now(), blocked)
	if err != nil || cl == nil {
		return false, err
	}

	e.mu.Lock()
	if _, ok := e.targetInfo[cl.Target.ID]; !ok {
		limit := cl.Target.MaxConcurrency
		if limit <= 0 {
			limit = e.cfg.DefaultTargetConcurrency
		}
		e.targetInfo[cl.Target.ID] = targetInfo{limit: limit}
	}
	e.inFlight++
	e.perTarget[cl.Target.ID]++
	e.mu.Unlock()

	go e.deliver(cl)
	return true, nil
}

// deliver performs a single HTTP delivery and queues its result.
func (e *Engine) deliver(cl *store.Claim) {
	ctx, cancel := context.WithCancel(e.deliverCtx)
	defer cancel()
	res := e.doDeliver(ctx, cl)

	e.mu.Lock()
	halted := e.halted
	e.mu.Unlock()
	if halted {
		// Simulate process death: do not record the outcome anywhere.
		return
	}
	e.results <- res
}

// doDeliver builds the signed request and executes it.
func (e *Engine) doDeliver(ctx context.Context, cl *store.Claim) attemptResult {
	secret, ok := cl.Target.SecretForVersion(cl.Attempt.SecretVersion)
	if !ok {
		secret, ok = cl.Target.CurrentSecret()
	}
	if !ok {
		// No signing secret at all: deliver unsigned but record a soft error.
		return attemptResult{
			claim:   cl,
			outcome: retry.Outcome{Category: retry.CategoryNetworkError, Retryable: true, ErrorMessage: "no signing secret configured"},
			err:     errors.New("no signing secret configured"),
		}
	}

	ts := e.clk.Now().Unix()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cl.Target.URL, bytes.NewReader(cl.Event.Payload))
	if err != nil {
		return attemptResult{claim: cl, outcome: retry.ClassifyError(err), err: err}
	}
	for k, v := range signing.Headers(cl.Event.EventType, cl.Attempt.ID, ts, cl.Event.Payload, secret.Secret) {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "courierbox/1.0")

	resp, err := e.cli.Do(req)
	if err != nil {
		return attemptResult{claim: cl, outcome: retry.ClassifyError(err), err: err}
	}
	defer resp.Body.Close()

	limit := int64(e.cfg.MaxResponseBody) + 1
	body, _ := io.ReadAll(io.LimitReader(resp.Body, limit))
	if int64(len(body)) > int64(e.cfg.MaxResponseBody) {
		body = body[:e.cfg.MaxResponseBody]
	}
	return attemptResult{
		claim:   cl,
		outcome: retry.ClassifyResponse(resp),
		status:  resp.StatusCode,
		body:    body,
	}
}

// processAvailableResults drains the results channel without blocking.
func (e *Engine) processAvailableResults(ctx context.Context) {
	for {
		select {
		case r := <-e.results:
			e.handleResult(ctx, r)
		default:
			return
		}
	}
}

// handleResult persists an attempt outcome and updates the in-flight counters.
func (e *Engine) handleResult(ctx context.Context, r attemptResult) {
	defer func() {
		e.mu.Lock()
		e.inFlight--
		if e.inFlight < 0 {
			e.inFlight = 0
		}
		if r.claim != nil {
			e.perTarget[r.claim.Target.ID]--
			if e.perTarget[r.claim.Target.ID] <= 0 {
				delete(e.perTarget, r.claim.Target.ID)
			}
		}
		e.mu.Unlock()
	}()

	finishedAt := e.clk.Now()
	cl := r.claim

	if cl == nil {
		return
	}

	var completion store.Completion
	completion.AttemptID = cl.Attempt.ID
	completion.EventID = cl.Event.ID
	completion.FinishedAt = finishedAt
	completion.ResponseStatus = r.status
	completion.ResponseBody = r.body

	if r.outcome.Category == retry.CategorySuccess {
		completion.FinalStatus = model.AttemptSucceeded
		completion.EventStatus = model.StatusSucceeded
		completion.ErrorCategory = string(retry.CategorySuccess)
		if err := e.store.CompleteAttempt(ctx, completion); err != nil {
			// On persistence failure we cannot do better than leave the attempt
			// as-is; the event stays "delivering" and will be recovered on next
			// startup.
			return
		}
		return
	}

	completion.ErrorCategory = string(r.outcome.Category)
	completion.ErrorMessage = r.outcome.ErrorMessage
	completion.ResponseStatus = r.status

	// Non-retryable failure or attempts exhausted -> dead.
	if !r.outcome.Retryable || cl.Event.AttemptCount >= e.cfg.MaxAttempts {
		completion.FinalStatus = model.AttemptFailedDead
		completion.EventStatus = model.StatusDead
		if err := e.store.CompleteAttempt(ctx, completion); err != nil {
			return
		}
		return
	}

	// Retryable: schedule next attempt with bounded backoff.
	delay := retry.ComputeDelay(cl.Event.AttemptCount, e.cfg.BaseDelay, e.cfg.MaxDelay, r.outcome.RetryAfter, e.cfg.RetryAfterMax)
	nextAt := e.clk.Now().Add(delay)
	completion.FinalStatus = model.AttemptFailedRetryable
	completion.EventStatus = model.StatusRetryWait
	completion.NextAttemptAt = &nextAt
	_ = e.store.CompleteAttempt(ctx, completion)
}

// Dispatch drives a single dispatch cycle. When wait is false it claims and
// dispatches all currently claimable work and returns immediately, leaving
// in-flight requests running. When wait is true it runs until no work is due and
// no requests are in flight.
func (e *Engine) Dispatch(ctx context.Context, wait bool) error {
	e.processAvailableResults(ctx)
	e.tryClaim(ctx)
	if !wait {
		return nil
	}
	return e.RunStable(ctx)
}

// RunStable claims and processes results until the engine is quiescent: no due
// events are claimable and no requests are in flight.
func (e *Engine) RunStable(ctx context.Context) error {
	for {
		e.processAvailableResults(ctx)
		e.tryClaim(ctx)
		if e.InFlight() == 0 {
			return nil
		}
		select {
		case r := <-e.results:
			e.handleResult(ctx, r)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Shutdown stops claiming new work, waits for in-flight requests to finish
// within ctx's deadline, and on timeout cancels them and persists the
// cancellations as recoverable retries.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	e.shuttingDown = true
	e.mu.Unlock()

	for {
		e.processAvailableResults(ctx)
		if e.InFlight() == 0 {
			return nil
		}
		select {
		case r := <-e.results:
			e.handleResult(ctx, r)
		case <-ctx.Done():
			return e.cancelInFlight(ctx)
		}
	}
}

// cancelInFlight cancels all in-flight HTTP requests and persists their
// cancellations as interrupted/retryable attempts. It uses a fresh context for
// the drain because the shutdown context that triggered it has already expired.
func (e *Engine) cancelInFlight(ctx context.Context) error {
	e.cancelDeliver()
	// Replace the cancelled deliver context so future (post-recovery) dispatches
	// are not immediately cancelled.
	e.mu.Lock()
	e.deliverCtx, e.cancelDeliver = context.WithCancel(e.baseCtx)
	e.mu.Unlock()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), e.cfg.ShutdownDrainTimeout)
	defer drainCancel()
	for {
		e.processAvailableResults(drainCtx)
		if e.InFlight() == 0 {
			return nil
		}
		select {
		case r := <-e.results:
			e.handleResult(drainCtx, r)
		case <-drainCtx.Done():
			return errors.New("shutdown: in-flight requests did not drain within deadline")
		}
	}
}

// Halt simulates an abrupt process crash: in-flight requests are cancelled and
// their outcomes are discarded (not persisted), leaving events in "delivering"
// so that recovery on the next startup is exercised. It is intended for tests.
func (e *Engine) Halt() {
	e.mu.Lock()
	e.halted = true
	e.mu.Unlock()
	e.cancelBase()
	e.cancelDeliver()
}
