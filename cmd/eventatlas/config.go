package main

import (
	"fmt"
	"strings"
	"time"
)

const (
	defaultHTTPAddress     = ":8080"
	defaultNATSURL         = "nats://127.0.0.1:4222"
	defaultSourceID        = "provider:nats:default"
	defaultBrokerName      = "NATS"
	defaultEnvironment     = "development"
	defaultDiscoveryScope  = "account:default"
	defaultDiscoveryWait   = 10 * time.Second
	defaultRefreshInterval = time.Minute
	defaultShutdownTimeout = 10 * time.Second
)

type config struct {
	httpAddress      string
	natsURL          string
	sourceID         string
	brokerName       string
	environment      string
	discoveryScope   string
	discoveryTimeout time.Duration
	refreshInterval  time.Duration
	shutdownTimeout  time.Duration
}

type envLookup func(string) (string, bool)

func loadConfig(lookup envLookup) (config, error) {
	discoveryTimeout, err := durationFromEnv(lookup, "EVENTATLAS_DISCOVERY_TIMEOUT", defaultDiscoveryWait)
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
		httpAddress:      stringFromEnv(lookup, "EVENTATLAS_HTTP_ADDRESS", defaultHTTPAddress),
		natsURL:          stringFromEnv(lookup, "EVENTATLAS_NATS_URL", defaultNATSURL),
		sourceID:         stringFromEnv(lookup, "EVENTATLAS_NATS_SOURCE_ID", defaultSourceID),
		brokerName:       stringFromEnv(lookup, "EVENTATLAS_NATS_BROKER_NAME", defaultBrokerName),
		environment:      stringFromEnv(lookup, "EVENTATLAS_ENVIRONMENT", defaultEnvironment),
		discoveryScope:   stringFromEnv(lookup, "EVENTATLAS_NATS_DISCOVERY_SCOPE", defaultDiscoveryScope),
		discoveryTimeout: discoveryTimeout,
		refreshInterval:  refreshInterval,
		shutdownTimeout:  shutdownTimeout,
	}, nil
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
