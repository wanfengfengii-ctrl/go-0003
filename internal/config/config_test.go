package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	c := Default()
	if c.Addr != ":8080" {
		t.Errorf("addr = %s", c.Addr)
	}
	if c.GlobalConcurrency != 16 {
		t.Errorf("global concurrency = %d", c.GlobalConcurrency)
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"addr":":9999","global_concurrency":7,"base_delay":"250ms","max_attempts":9}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Addr != ":9999" {
		t.Errorf("addr = %s, want :9999", c.Addr)
	}
	if c.GlobalConcurrency != 7 {
		t.Errorf("global concurrency = %d, want 7", c.GlobalConcurrency)
	}
	if c.MaxAttempts != 9 {
		t.Errorf("max attempts = %d, want 9", c.MaxAttempts)
	}
	if c.BaseDelay != "250ms" {
		t.Errorf("base delay = %s", c.BaseDelay)
	}
}

func TestEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"addr":":1111","global_concurrency":3}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COURIERBOX_ADDR", ":2222")
	t.Setenv("COURIERBOX_GLOBAL_CONCURRENCY", "42")
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":2222" {
		t.Errorf("addr = %s, want :2222 (env override)", c.Addr)
	}
	if c.GlobalConcurrency != 42 {
		t.Errorf("global concurrency = %d, want 42 (env override)", c.GlobalConcurrency)
	}
}

func TestLoadMissingFileReturnsError(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestParseDuration(t *testing.T) {
	if d := ParseDuration("1s", time.Second); d != time.Second {
		t.Errorf("got %v", d)
	}
	if d := ParseDuration("", time.Second); d != time.Second {
		t.Errorf("empty -> default: got %v", d)
	}
	if d := ParseDuration("not-a-duration", time.Second); d != time.Second {
		t.Errorf("invalid -> default: got %v", d)
	}
	if d := ParseDuration("500ms", 0); d != 500*time.Millisecond {
		t.Errorf("got %v", d)
	}
}
