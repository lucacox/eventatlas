package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddress                       = ":8080"
	defaultNATSURL                           = "nats://127.0.0.1:4222"
	defaultSourceID                          = "provider:nats:default"
	defaultBrokerName                        = "NATS"
	defaultEnvironment                       = "development"
	defaultDiscoveryScope                    = "account:default"
	defaultOTLPSourceID                      = "observation:otel:default"
	defaultOTLPMaxRequestBytes         int64 = 64 << 20
	defaultOTLPMaxFutureSkew                 = 5 * time.Minute
	defaultOTLPMaxAttributeValueLength       = 1024
	defaultDiscoveryWait                     = 10 * time.Second
	defaultDatabaseWait                      = 10 * time.Second
	defaultRefreshInterval                   = time.Minute
	defaultShutdownTimeout                   = 10 * time.Second
)

type config struct {
	httpAddress         string
	otlpHTTPAddress     string
	natsURL             string
	databaseURL         string
	sourceID            string
	otlpSourceID        string
	brokerName          string
	environment         string
	discoveryScope      string
	otlpMaxRequestBytes int64
	otlpMaxFutureSkew   time.Duration
	discoveryTimeout    time.Duration
	databaseTimeout     time.Duration
	refreshInterval     time.Duration
	shutdownTimeout     time.Duration
}

type envLookup func(string) (string, bool)

func loadConfig(lookup envLookup) (config, error) {
	otlpMaxRequestBytes, err := positiveInt64FromEnv(
		lookup,
		"EVENTATLAS_OTLP_MAX_REQUEST_BYTES",
		defaultOTLPMaxRequestBytes,
	)
	if err != nil {
		return config{}, err
	}
	otlpMaxFutureSkew, err := durationFromEnv(
		lookup,
		"EVENTATLAS_OTLP_MAX_FUTURE_SKEW",
		defaultOTLPMaxFutureSkew,
	)
	if err != nil {
		return config{}, err
	}
	discoveryTimeout, err := durationFromEnv(lookup, "EVENTATLAS_DISCOVERY_TIMEOUT", defaultDiscoveryWait)
	if err != nil {
		return config{}, err
	}
	databaseTimeout, err := durationFromEnv(lookup, "EVENTATLAS_DATABASE_TIMEOUT", defaultDatabaseWait)
	if err != nil {
		return config{}, err
	}
	refreshInterval, err := durationFromEnv(lookup, "EVENTATLAS_REFRESH_INTERVAL", defaultRefreshInterval)
	if err != nil {
		return config{}, err
	}
	shutdownTimeout, err := durationFromEnv(lookup, "EVENTATLAS_SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return config{}, err
	}
	return config{
		httpAddress:         stringFromEnv(lookup, "EVENTATLAS_HTTP_ADDRESS", defaultHTTPAddress),
		natsURL:             stringFromEnv(lookup, "EVENTATLAS_NATS_URL", defaultNATSURL),
		databaseURL:         optionalStringFromEnv(lookup, "EVENTATLAS_DATABASE_URL"),
		sourceID:            stringFromEnv(lookup, "EVENTATLAS_NATS_SOURCE_ID", defaultSourceID),
		brokerName:          stringFromEnv(lookup, "EVENTATLAS_NATS_BROKER_NAME", defaultBrokerName),
		environment:         stringFromEnv(lookup, "EVENTATLAS_ENVIRONMENT", defaultEnvironment),
		discoveryScope:      stringFromEnv(lookup, "EVENTATLAS_NATS_DISCOVERY_SCOPE", defaultDiscoveryScope),
		otlpHTTPAddress:     optionalStringFromEnv(lookup, "EVENTATLAS_OTLP_HTTP_ADDRESS"),
		otlpSourceID:        stringFromEnv(lookup, "EVENTATLAS_OTLP_SOURCE_ID", defaultOTLPSourceID),
		otlpMaxRequestBytes: otlpMaxRequestBytes,
		otlpMaxFutureSkew:   otlpMaxFutureSkew,
		discoveryTimeout:    discoveryTimeout,
		databaseTimeout:     databaseTimeout,
		refreshInterval:     refreshInterval,
		shutdownTimeout:     shutdownTimeout,
	}, nil
}

func optionalStringFromEnv(lookup envLookup, name string) string {
	value, ok := lookup(name)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func stringFromEnv(lookup envLookup, name, fallback string) string {
	if value, ok := lookup(name); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func durationFromEnv(lookup envLookup, name string, fallback time.Duration) (time.Duration, error) {
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return duration, nil
}

func positiveInt64FromEnv(lookup envLookup, name string, fallback int64) (int64, error) {
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return parsed, nil
}
