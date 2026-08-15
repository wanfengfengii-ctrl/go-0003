// Command courierbox runs the Courierbox webhook delivery service.
//
// It also exposes a -healthcheck mode that performs a single HTTP GET against
// the readiness probe and exits non-zero on failure. This is used as the
// container HEALTHCHECK because the distroless runtime image has no shell or
// network utilities.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"courierbox/internal/app"
	"courierbox/internal/config"
)

// exitIfReady performs an HTTP GET against the readiness endpoint and exits
// non-zero if it does not return 2xx within a short timeout. Used as the
// container HEALTHCHECK. Redirects are not followed: a 3xx is treated as not
// ready so a misrouted port (e.g. a proxy returning a login redirect) cannot
// produce a false-positive healthy result.
func exitIfReady(url string) {
	client := &http.Client{
		Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "healthcheck: status %d\n", resp.StatusCode)
		os.Exit(1)
	}
	os.Exit(0)
}

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	healthcheck := flag.Bool("healthcheck", false, "probe the readiness endpoint and exit (used for container HEALTHCHECK)")
	healthURL := flag.String("health-url", "http://127.0.0.1:8080/readyz", "URL probed by -healthcheck")
	flag.Parse()

	if *healthcheck {
		exitIfReady(*healthURL)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	a, err := app.New(app.Options{Config: cfg})
	if err != nil {
		log.Fatalf("init: %v", err)
	}
	defer a.Close()

	a.StartEngine()

	// Signal handling: stop claiming new work on SIGINT/SIGTERM and drain.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		addr := cfg.Addr
		log.Printf("courierbox listening on %s (test_mode=%v)", addr, cfg.TestMode)
		if err := a.ListenAndServe(addr); err != nil && err.Error() != "http: Server closed" {
			// http.ErrServerClosed is the expected exit after Shutdown.
			log.Fatalf("server: %v", err)
		}
	}()

	sig := <-sigCh
	log.Printf("received %s, shutting down", sig)

	timeout := config.ParseDuration(cfg.ShutdownTimeout, 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		log.Printf("shutdown completed with error: %v", err)
	} else {
		log.Printf("shutdown complete")
	}
}
