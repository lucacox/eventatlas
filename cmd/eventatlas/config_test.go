package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigUsesDefaults(t *testing.T) {
	t.Parallel()

	config, err := loadConfig(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if config.httpAddress != defaultHTTPAddress || config.natsURL != defaultNATSURL || config.sourceID != defaultSourceID {
		t.Errorf("default endpoints/identity = (%q, %q, %q)", config.httpAddress, config.natsURL, config.sourceID)
	}
	if config.databaseURL != "" {
		t.Errorf("default database URL = %q, want disabled", config.databaseURL)
	}
	if config.brokerName != defaultBrokerName || config.environment != defaultEnvironment || config.discoveryScope != defaultDiscoveryScope {
		t.Errorf("default provider config = (%q, %q, %q)", config.brokerName, config.environment, config.discoveryScope)
	}
	if config.discoveryTimeout != defaultDiscoveryWait || config.databaseTimeout != defaultDatabaseWait || config.refreshInterval != defaultRefreshInterval || config.shutdownTimeout != defaultShutdownTimeout {
		t.Errorf("default timing = (%s, %s, %s, %s)", config.discoveryTimeout, config.databaseTimeout, config.refreshInterval, config.shutdownTimeout)
	}
}

func TestLoadConfigReadsAndTrimsEnvironment(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"EVENTATLAS_HTTP_ADDRESS":         " 127.0.0.1:9090 ",
		"EVENTATLAS_NATS_URL":             " nats://nats:4222 ",
		"EVENTATLAS_DATABASE_URL":         " postgres://eventatlas:secret@postgres/eventatlas ",
		"EVENTATLAS_NATS_SOURCE_ID":       " provider:nats:local ",
		"EVENTATLAS_NATS_BROKER_NAME":     " Local NATS ",
		"EVENTATLAS_ENVIRONMENT":          " test ",
		"EVENTATLAS_NATS_DISCOVERY_SCOPE": " account:local ",
		"EVENTATLAS_DISCOVERY_TIMEOUT":    " 3s ",
		"EVENTATLAS_DATABASE_TIMEOUT":     " 2s ",
		"EVENTATLAS_REFRESH_INTERVAL":     " 30s ",
		"EVENTATLAS_SHUTDOWN_TIMEOUT":     " 4s ",
	}
	config, err := loadConfig(func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	})
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if config.httpAddress != "127.0.0.1:9090" || config.natsURL != "nats://nats:4222" {
		t.Errorf("configured endpoints = (%q, %q)", config.httpAddress, config.natsURL)
	}
	if config.databaseURL != "postgres://eventatlas:secret@postgres/eventatlas" {
		t.Errorf("configured database URL = %q", config.databaseURL)
	}
	if config.sourceID != "provider:nats:local" || config.brokerName != "Local NATS" || config.environment != "test" || config.discoveryScope != "account:local" {
		t.Errorf("configured provider = (%q, %q, %q, %q)", config.sourceID, config.brokerName, config.environment, config.discoveryScope)
	}
	if config.discoveryTimeout != 3*time.Second || config.databaseTimeout != 2*time.Second || config.refreshInterval != 30*time.Second || config.shutdownTimeout != 4*time.Second {
		t.Errorf("configured timing = (%s, %s, %s, %s)", config.discoveryTimeout, config.databaseTimeout, config.refreshInterval, config.shutdownTimeout)
	}
}

func TestLoadConfigRejectsInvalidDurations(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		variable string
		value    string
	}{
		{name: "malformed discovery timeout", variable: "EVENTATLAS_DISCOVERY_TIMEOUT", value: "soon"},
		{name: "zero discovery timeout", variable: "EVENTATLAS_DISCOVERY_TIMEOUT", value: "0s"},
		{name: "negative discovery timeout", variable: "EVENTATLAS_DISCOVERY_TIMEOUT", value: "-1s"},
		{name: "malformed database timeout", variable: "EVENTATLAS_DATABASE_TIMEOUT", value: "eventually"},
		{name: "zero database timeout", variable: "EVENTATLAS_DATABASE_TIMEOUT", value: "0s"},
		{name: "malformed refresh interval", variable: "EVENTATLAS_REFRESH_INTERVAL", value: "later"},
		{name: "zero refresh interval", variable: "EVENTATLAS_REFRESH_INTERVAL", value: "0s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadConfig(func(name string) (string, bool) {
				if name == test.variable {
					return test.value, true
				}
				return "", false
			})
			if err == nil || !strings.Contains(err.Error(), test.variable) {
				t.Errorf("loadConfig() error = %v, want %s error", err, test.variable)
			}
		})
	}
}
