package topology

import (
	"errors"
	"testing"
)

func TestNewSourceIDPreservesOpaqueValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "NATS provider", value: "provider:nats:production-main"},
		{name: "Kafka provider", value: "provider:kafka:analytics"},
		{name: "OpenTelemetry observation source", value: "observation:otel:production"},
		{name: "non-blank surrounding whitespace", value: " source-defined-value "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			id, err := NewSourceID(test.value)
			if err != nil {
				t.Fatalf("NewSourceID() error = %v", err)
			}

			if id.String() != test.value {
				t.Errorf("SourceID.String() = %q, want %q", id.String(), test.value)
			}
		})
	}
}

func TestNewSourceIDRejectsBlankValue(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", " ", "\t\n"} {
		if _, err := NewSourceID(value); !errors.Is(err, ErrEmptySourceID) {
			t.Errorf("NewSourceID(%q) error = %v, want %v", value, err, ErrEmptySourceID)
		}
	}
}
