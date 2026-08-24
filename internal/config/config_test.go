package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	for _, name := range []string{
		"GRIDBANK_ADDRESS", "GRIDBANK_DATABASE_PATH",
		"GRIDBANK_SHUTDOWN_TIMEOUT", "GRIDBANK_SESSION_TTL",
		"GRIDBANK_LEASE_TTL", "GRIDBANK_WORKER_POLL_INTERVAL",
		"GRIDBANK_WORKER_RETRY_BASE", "GRIDBANK_WEBHOOK_TIMEOUT",
		"GRIDBANK_WORKER_MAX_ATTEMPTS", "GRIDBANK_DATABASE_MAX_OPEN_CONNS",
	} {
		t.Setenv(name, "")
	}
	// Empty values are explicit values for string fields, so restore their true absence.
	t.Setenv("GRIDBANK_ADDRESS", ":8080")
	t.Setenv("GRIDBANK_DATABASE_PATH", "gridbank.db")
	t.Setenv("GRIDBANK_SHUTDOWN_TIMEOUT", "10s")
	t.Setenv("GRIDBANK_SESSION_TTL", "12h")
	t.Setenv("GRIDBANK_LEASE_TTL", "90s")
	t.Setenv("GRIDBANK_WORKER_POLL_INTERVAL", "500ms")
	t.Setenv("GRIDBANK_WORKER_RETRY_BASE", "250ms")
	t.Setenv("GRIDBANK_WEBHOOK_TIMEOUT", "8s")
	t.Setenv("GRIDBANK_WORKER_MAX_ATTEMPTS", "5")
	t.Setenv("GRIDBANK_DATABASE_MAX_OPEN_CONNS", "8")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Address != ":8080" || cfg.DatabasePath != "gridbank.db" {
		t.Fatalf("unexpected string defaults: %+v", cfg)
	}
	if cfg.SessionTTL != 12*time.Hour || cfg.LeaseTTL != 90*time.Second {
		t.Fatalf("unexpected lifecycle defaults: %+v", cfg)
	}
	if cfg.WorkerMaxAttempts != 5 || cfg.DatabaseMaxOpenConns != 8 {
		t.Fatalf("unexpected numeric defaults: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("GRIDBANK_ADDRESS", "127.0.0.1:9090")
	t.Setenv("GRIDBANK_DATABASE_PATH", "/tmp/gridbank-test.db")
	t.Setenv("GRIDBANK_SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("GRIDBANK_SESSION_TTL", "45m")
	t.Setenv("GRIDBANK_LEASE_TTL", "17s")
	t.Setenv("GRIDBANK_WORKER_POLL_INTERVAL", "40ms")
	t.Setenv("GRIDBANK_WORKER_RETRY_BASE", "75ms")
	t.Setenv("GRIDBANK_WEBHOOK_TIMEOUT", "4s")
	t.Setenv("GRIDBANK_WORKER_MAX_ATTEMPTS", "9")
	t.Setenv("GRIDBANK_DATABASE_MAX_OPEN_CONNS", "3")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Address != "127.0.0.1:9090" || cfg.DatabasePath != "/tmp/gridbank-test.db" {
		t.Fatalf("overrides not loaded: %+v", cfg)
	}
	if cfg.ShutdownTimeout != 3*time.Second || cfg.SessionTTL != 45*time.Minute || cfg.LeaseTTL != 17*time.Second {
		t.Fatalf("duration overrides not loaded: %+v", cfg)
	}
	if cfg.WorkerPollInterval != 40*time.Millisecond || cfg.WorkerRetryBase != 75*time.Millisecond || cfg.WebhookTimeout != 4*time.Second {
		t.Fatalf("worker overrides not loaded: %+v", cfg)
	}
	if cfg.WorkerMaxAttempts != 9 || cfg.DatabaseMaxOpenConns != 3 {
		t.Fatalf("integer overrides not loaded: %+v", cfg)
	}
}

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "address", key: "GRIDBANK_ADDRESS", value: "missing-port", want: "invalid address"},
		{name: "shutdown", key: "GRIDBANK_SHUTDOWN_TIMEOUT", value: "tomorrow", want: "parse GRIDBANK_SHUTDOWN_TIMEOUT"},
		{name: "session", key: "GRIDBANK_SESSION_TTL", value: "-1s", want: "must be positive"},
		{name: "lease", key: "GRIDBANK_LEASE_TTL", value: "0s", want: "must be positive"},
		{name: "poll", key: "GRIDBANK_WORKER_POLL_INTERVAL", value: "0s", want: "must be positive"},
		{name: "retry", key: "GRIDBANK_WORKER_RETRY_BASE", value: "invalid", want: "parse GRIDBANK_WORKER_RETRY_BASE"},
		{name: "webhook", key: "GRIDBANK_WEBHOOK_TIMEOUT", value: "-2s", want: "must be positive"},
		{name: "attempts syntax", key: "GRIDBANK_WORKER_MAX_ATTEMPTS", value: "many", want: "parse GRIDBANK_WORKER_MAX_ATTEMPTS"},
		{name: "attempts lower bound", key: "GRIDBANK_WORKER_MAX_ATTEMPTS", value: "0", want: "at least one"},
		{name: "connections syntax", key: "GRIDBANK_DATABASE_MAX_OPEN_CONNS", value: "none", want: "parse GRIDBANK_DATABASE_MAX_OPEN_CONNS"},
		{name: "connections lower bound", key: "GRIDBANK_DATABASE_MAX_OPEN_CONNS", value: "0", want: "at least one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want fragment %q", err, test.want)
			}
		})
	}
}

func TestValidateRejectsDirectInvalidValues(t *testing.T) {
	valid := Config{Address: ":8080", DatabasePath: "db.sqlite", ShutdownTimeout: time.Second, SessionTTL: time.Hour, LeaseTTL: time.Minute, WorkerPollInterval: time.Second, WorkerRetryBase: time.Second, WorkerMaxAttempts: 1, WebhookTimeout: time.Second, DatabaseMaxOpenConns: 1}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "empty address", mutate: func(c *Config) { c.Address = "" }},
		{name: "empty database", mutate: func(c *Config) { c.DatabasePath = "" }},
		{name: "zero shutdown", mutate: func(c *Config) { c.ShutdownTimeout = 0 }},
		{name: "zero session", mutate: func(c *Config) { c.SessionTTL = 0 }},
		{name: "zero lease", mutate: func(c *Config) { c.LeaseTTL = 0 }},
		{name: "zero poll", mutate: func(c *Config) { c.WorkerPollInterval = 0 }},
		{name: "zero retry", mutate: func(c *Config) { c.WorkerRetryBase = 0 }},
		{name: "zero webhook", mutate: func(c *Config) { c.WebhookTimeout = 0 }},
		{name: "zero attempts", mutate: func(c *Config) { c.WorkerMaxAttempts = 0 }},
		{name: "zero connections", mutate: func(c *Config) { c.DatabaseMaxOpenConns = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if err := cfg.Settle(); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}
