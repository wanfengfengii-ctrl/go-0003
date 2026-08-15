// Package app is the composition root: it wires the store, engine and API into
// a single runnable service and is also the entrypoint used by tests to build an
// in-process instance with deterministic dependencies.
package app

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"courierbox/internal/api"
	"courierbox/internal/clock"
	"courierbox/internal/config"
	"courierbox/internal/engine"
	"courierbox/internal/store"
)

// Options configures an App. It is built from config.Config but can be
// constructed directly by tests.
type Options struct {
	Config        config.Config
	ManualClock   *clock.Manual // required when Config.TestMode is true
	OverrideClock clock.Clock   // optional; overrides the derived clock
}

// App is a running Courierbox service.
type App struct {
	Store   store.Store
	Engine  *engine.Engine
	Clock   clock.Clock
	API     *api.Server
	handler http.Handler

	server *http.Server

	shutdownOnce sync.Once
	runCtx       context.Context
	runCancel    context.CancelFunc
}

// New constructs an App, opens the store, recovers in-flight deliveries and
// builds the HTTP handler. It does not start the autonomous engine loop or the
// HTTP server; call Start for that.
func New(opts Options) (*App, error) {
	if opts.Config.TestMode && opts.ManualClock == nil && opts.OverrideClock == nil {
		return nil, errors.New("test mode requires a manual clock")
	}

	var clk clock.Clock = opts.OverrideClock
	if clk == nil {
		if opts.Config.TestMode {
			clk = opts.ManualClock
		} else {
			clk = clock.Real{}
		}
	}

	st, err := store.Open(opts.Config.DBPath, clk)
	if err != nil {
		return nil, err
	}

	engCfg := engine.Config{
		GlobalConcurrency:        opts.Config.GlobalConcurrency,
		DefaultTargetConcurrency: opts.Config.DefaultTargetConcurrency,
		BaseDelay:                config.ParseDuration(opts.Config.BaseDelay, time.Second),
		MaxDelay:                 config.ParseDuration(opts.Config.MaxDelay, 5*time.Minute),
		MaxAttempts:              opts.Config.MaxAttempts,
		RetryAfterMax:            config.ParseDuration(opts.Config.RetryAfterMax, time.Minute),
		HTTPTimeout:              config.ParseDuration(opts.Config.HTTPTimeout, 30*time.Second),
		MaxResponseBody:          opts.Config.MaxResponseBody,
		PollInterval:             config.ParseDuration(opts.Config.PollInterval, 200*time.Millisecond),
		ShutdownDrainTimeout:     config.ParseDuration(opts.Config.ShutdownTimeout, 30*time.Second),
		TestMode:                 opts.Config.TestMode,
	}
	eng := engine.New(st, clk, engCfg)

	// Recover any deliveries left in flight from a prior process.
	if _, err := eng.Recover(context.Background()); err != nil {
		st.Close()
		return nil, err
	}

	apiSrv := api.New(st, eng, clk, api.Config{
		MaxPayloadSize: opts.Config.MaxPayloadSize,
		TestMode:       opts.Config.TestMode,
		ManualClock:    opts.ManualClock,
	})

	runCtx, runCancel := context.WithCancel(context.Background())
	a := &App{
		Store:     st,
		Engine:    eng,
		Clock:     clk,
		API:       apiSrv,
		handler:   apiSrv.Handler(),
		runCtx:    runCtx,
		runCancel: runCancel,
	}
	return a, nil
}

// Handler returns the HTTP handler. Useful for httptest.NewServer in tests.
func (a *App) Handler() http.Handler { return a.handler }

// StartEngine begins the autonomous claim loop (production only).
func (a *App) StartEngine() { a.Engine.Start(a.runCtx) }

// ListenAndServe starts the HTTP server. Blocks until Shutdown or an error.
func (a *App) ListenAndServe(addr string) error {
	if addr == "" {
		addr = ":8080"
	}
	a.server = &http.Server{
		Addr:              addr,
		Handler:           a.handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return a.server.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server and the engine. In-flight HTTP
// deliveries are allowed to finish within ctx, then cancelled and persisted as
// recoverable retries.
func (a *App) Shutdown(ctx context.Context) error {
	var firstErr error
	a.shutdownOnce.Do(func() {
		a.runCancel()
		if a.server != nil {
			if err := a.server.Shutdown(ctx); err != nil {
				firstErr = err
			}
		}
		if err := a.Engine.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}

// Close releases resources. It should be called after Shutdown.
func (a *App) Close() error {
	return a.Store.Close()
}

// Halt simulates an abrupt process crash for tests: in-flight work is abandoned
// without being persisted, so events remain in "delivering" for recovery.
func (a *App) Halt() {
	a.runCancel()
	a.Engine.Halt()
}
