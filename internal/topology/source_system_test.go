package topology

import (
	"errors"
	"testing"
)

func TestNewSourceSystem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "NATS", value: "nats", valid: true},
		{name: "Kafka", value: "kafka", valid: true},
		{name: "RabbitMQ", value: "rabbitmq", valid: true},
		{name: "OpenTelemetry", value: "opentelemetry", valid: true},
		{name: "AWS SQS", value: "aws.sqs", valid: true},
		{name: "Google PubSub", value: "google.pubsub", valid: true},
		{name: "Azure Service Bus", value: "azure.service_bus", valid: true},
		{name: "numeric suffix", value: "provider2.system3", valid: true},
		{name: "zero value", value: "", valid: false},
		{name: "leading dot", value: ".nats", valid: false},
		{name: "trailing dot", value: "nats.", valid: false},
		{name: "empty segment", value: "aws..sqs", valid: false},
		{name: "uppercase", value: "OpenTelemetry", valid: false},
		{name: "leading number", value: "2provider", valid: false},
		{name: "hyphen", value: "google.pub-sub", valid: false},
		{name: "whitespace", value: " nats ", valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			system, err := NewSourceSystem(test.value)
			if !test.valid {
				if !errors.Is(err, ErrSourceSystemInvalid) {
					t.Fatalf("NewSourceSystem(%q) error = %v, want %v", test.value, err, ErrSourceSystemInvalid)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSourceSystem(%q) error = %v", test.value, err)
			}

			if system.String() != test.value {
				t.Errorf("SourceSystem.String() = %q, want %q", system, test.value)
			}
			if !system.IsValid() {
				t.Errorf("SourceSystem(%q).IsValid() = false, want true", system)
			}
		})
	}
}

func TestSourceSystemIsValidRejectsUnvalidatedValue(t *testing.T) {
	t.Parallel()

	if SourceSystem("invalid system").IsValid() {
		t.Error("SourceSystem.IsValid() = true for an invalid value")
	}
}
