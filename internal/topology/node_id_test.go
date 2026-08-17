package topology

import "testing"

func TestNewNodeIDPreservesOpaqueValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "service", value: "service:production:order-service"},
		{name: "NATS resource", value: "nats:cluster-a:stream:ORDERS"},
		{name: "Kafka consumer", value: "kafka:cluster-b:consumer:billing"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			id, err := NewNodeID(test.value)
			if err != nil {
				t.Fatalf("NewNodeID() error = %v", err)
			}

			if id.String() != test.value {
				t.Errorf("NodeID.String() = %q, want %q", id.String(), test.value)
			}
		})
	}
}

func TestNewNodeIDRejectsBlankValue(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", " ", "\t\n"} {
		if _, err := NewNodeID(value); err == nil {
			t.Errorf("NewNodeID(%q) error = nil, want error", value)
		}
	}
}
