// Package config loads Courierbox runtime configuration from a JSON file and
// environment variables. Environment variables take precedence over the file.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the runtime configuration.
type Config struct {
	// DBPath is the SQLite database path. Use ":memory:" for an ephemeral DB.
	DBPath string `json:"db_path"`
	// Addr is the HTTP listen address.
	Addr string `json:"addr"`
	// TestMode enables the deterministic control plane and requires a manual clock.
	TestMode bool `json:"test_mode"`
	// GlobalConcurrency caps in-flight deliveries across all targets.
	GlobalConcurrency int `json:"global_concurrency"`
	// DefaultTargetConcurrency caps in-flight deliveries per target without an override.
	DefaultTargetConcurrency int `json:"default_target_concurrency"`
	// BaseDelay is the first retry backoff.
	BaseDelay string `json:"base_delay"`
	// MaxDelay caps the exponential backoff.
	MaxDelay string `json:"max_delay"`
	// MaxAttempts is the attempt ceiling before an event is dead.
	MaxAttempts int `json:"max_attempts"`
	// RetryAfterMax caps a 429 Retry-After value.
	RetryAfterMax string `json:"retry_after_max"`
	// HTTPTimeout is the per-request timeout.
	HTTPTimeout string `json:"http_timeout"`
	// MaxResponseBody bounds the stored response body.
	MaxResponseBody int `json:"max_response_body"`
	// MaxPayloadSize bounds the event submission body.
	MaxPayloadSize int `json:"max_payload_size"`
	// PollInterval is the production claim loop interval.
	PollInterval string `json:"poll_interval"`
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout string `json:"shutdown_timeout"`
}

// Default returns a Config with sensible defaults.
func Default() Config {
	return Config{
		DBPath:                   "courierbox.db",
		Addr:                     ":8080",
		GlobalConcurrency:        16,
		DefaultTargetConcurrency: 4,
		BaseDelay:                "1s",
		MaxDelay:                 "5m",
		MaxAttempts:              25,
		RetryAfterMax:            "1m",
		HTTPTimeout:              "30s",
		MaxResponseBody:          8192,
		MaxPayloadSize:           1 << 20,
		PollInterval:             "200ms",
		ShutdownTimeout:          "30s",
	}
}

// Load reads a JSON config file (if path is non-empty) and applies environment
// variable overrides.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	applyEnv(&cfg)
	return cfg, nil
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func applyEnv(c *Config) {
	c.DBPath = envString("COURIERBOX_DB_PATH", c.DBPath)
	c.Addr = envString("COURIERBOX_ADDR", c.Addr)
	c.TestMode = envBool("COURIERBOX_TEST_MODE", c.TestMode)
	c.GlobalConcurrency = envInt("COURIERBOX_GLOBAL_CONCURRENCY", c.GlobalConcurrency)
	c.DefaultTargetConcurrency = envInt("COURIERBOX_DEFAULT_TARGET_CONCURRENCY", c.DefaultTargetConcurrency)
	c.MaxAttempts = envInt("COURIERBOX_MAX_ATTEMPTS", c.MaxAttempts)
	c.MaxResponseBody = envInt("COURIERBOX_MAX_RESPONSE_BODY", c.MaxResponseBody)
	c.MaxPayloadSize = envInt("COURIERBOX_MAX_PAYLOAD_SIZE", c.MaxPayloadSize)
	c.BaseDelay = envString("COURIERBOX_BASE_DELAY", c.BaseDelay)
	c.MaxDelay = envString("COURIERBOX_MAX_DELAY", c.MaxDelay)
	c.RetryAfterMax = envString("COURIERBOX_RETRY_AFTER_MAX", c.RetryAfterMax)
	c.HTTPTimeout = envString("COURIERBOX_HTTP_TIMEOUT", c.HTTPTimeout)
	c.PollInterval = envString("COURIERBOX_POLL_INTERVAL", c.PollInterval)
	c.ShutdownTimeout = envString("COURIERBOX_SHUTDOWN_TIMEOUT", c.ShutdownTimeout)
}

// ParseDuration is a convenience that parses a config duration string.
func ParseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}
