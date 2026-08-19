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
	if config.brokerName != defaultBrokerName || config.environment != defaultEnvironment || config.discoveryScope != defaultDiscoveryScope {
		t.Errorf("default provider config = (%q, %q, %q)", config.brokerName, config.environment, config.discoveryScope)
	}
	if config.discoveryTimeout != defaultDiscoveryWait || config.shutdownTimeout != defaultShutdownTimeout {
		t.Errorf("default timeouts = (%s, %s)", config.discoveryTimeout, config.shutdownTimeout)
	}
}

func TestLoadConfigReadsAndTrimsEnvironment(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"EVENTATLAS_HTTP_ADDRESS":         " 127.0.0.1:9090 ",
		"EVENTATLAS_NATS_URL":             " nats://nats:4222 ",
		"EVENTATLAS_NATS_SOURCE_ID":       " provider:nats:local ",
		"EVENTATLAS_NATS_BROKER_NAME":     " Local NATS ",
		"EVENTATLAS_ENVIRONMENT":          " test ",
		"EVENTATLAS_NATS_DISCOVERY_SCOPE": " account:local ",
		"EVENTATLAS_DISCOVERY_TIMEOUT":    " 3s ",
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
	if config.sourceID != "provider:nats:local" || config.brokerName != "Local NATS" || config.environment != "test" || config.discoveryScope != "account:local" {
		t.Errorf("configured provider = (%q, %q, %q, %q)", config.sourceID, config.brokerName, config.environment, config.discoveryScope)
	}
	if config.discoveryTimeout != 3*time.Second || config.shutdownTimeout != 4*time.Second {
		t.Errorf("configured timeouts = (%s, %s)", config.discoveryTimeout, config.shutdownTimeout)
	}
}

func TestLoadConfigRejectsInvalidDurations(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "malformed", value: "soon"},
		{name: "zero", value: "0s"},
		{name: "negative", value: "-1s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadConfig(func(name string) (string, bool) {
				if name == "EVENTATLAS_DISCOVERY_TIMEOUT" {
					return test.value, true
				}
				return "", false
			})
			if err == nil || !strings.Contains(err.Error(), "EVENTATLAS_DISCOVERY_TIMEOUT") {
				t.Errorf("loadConfig() error = %v, want discovery timeout error", err)
			}
		})
	}
}
