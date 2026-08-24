package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Address              string
	DatabasePath         string
	ShutdownTimeout      time.Duration
	SessionTTL           time.Duration
	LeaseTTL             time.Duration
	WorkerPollInterval   time.Duration
	WorkerRetryBase      time.Duration
	WorkerMaxAttempts    int
	WebhookTimeout       time.Duration
	DatabaseMaxOpenConns int
}

func Load() (Config, error) {
	cfg := Config{
		Address:              env("GRIDBANK_ADDRESS", ":8080"),
		DatabasePath:         env("GRIDBANK_DATABASE_PATH", "gridbank.db"),
		ShutdownTimeout:      10 * time.Second,
		SessionTTL:           12 * time.Hour,
		LeaseTTL:             90 * time.Second,
		WorkerPollInterval:   500 * time.Millisecond,
		WorkerRetryBase:      250 * time.Millisecond,
		WorkerMaxAttempts:    5,
		WebhookTimeout:       8 * time.Second,
		DatabaseMaxOpenConns: 8,
	}

	var err error
	if cfg.ShutdownTimeout, err = duration("GRIDBANK_SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.SessionTTL, err = duration("GRIDBANK_SESSION_TTL", cfg.SessionTTL); err != nil {
		return Config{}, err
	}
	if cfg.LeaseTTL, err = duration("GRIDBANK_LEASE_TTL", cfg.LeaseTTL); err != nil {
		return Config{}, err
	}
	if cfg.WorkerPollInterval, err = duration("GRIDBANK_WORKER_POLL_INTERVAL", cfg.WorkerPollInterval); err != nil {
		return Config{}, err
	}
	if cfg.WorkerRetryBase, err = duration("GRIDBANK_WORKER_RETRY_BASE", cfg.WorkerRetryBase); err != nil {
		return Config{}, err
	}
	if cfg.WebhookTimeout, err = duration("GRIDBANK_WEBHOOK_TIMEOUT", cfg.WebhookTimeout); err != nil {
		return Config{}, err
	}
	if cfg.WorkerMaxAttempts, err = integer("GRIDBANK_WORKER_MAX_ATTEMPTS", cfg.WorkerMaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.DatabaseMaxOpenConns, err = integer("GRIDBANK_DATABASE_MAX_OPEN_CONNS", cfg.DatabaseMaxOpenConns); err != nil {
		return Config{}, err
	}
	if err := cfg.Settle(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Settle() error {
	if strings.TrimSpace(c.Address) == "" {
		return errors.New("address is required")
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return fmt.Errorf("invalid address %q: %w", c.Address, err)
	}
	if strings.TrimSpace(c.DatabasePath) == "" {
		return errors.New("database path is required")
	}
	if c.ShutdownTimeout <= 0 || c.SessionTTL <= 0 || c.LeaseTTL <= 0 {
		return errors.New("shutdown, session, and lease durations must be positive")
	}
	if c.WorkerPollInterval <= 0 || c.WorkerRetryBase <= 0 || c.WebhookTimeout <= 0 {
		return errors.New("worker and webhook durations must be positive")
	}
	if c.WorkerMaxAttempts < 1 {
		return errors.New("worker max attempts must be at least one")
	}
	if c.DatabaseMaxOpenConns < 1 {
		return errors.New("database max open connections must be at least one")
	}
	return nil
}

func env(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func duration(name string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func integer(name string, fallback int) (int, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}
