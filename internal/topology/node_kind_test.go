package topology

import "testing"

func TestNodeKindIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  NodeKind
		value string
		valid bool
	}{
		{name: "service", kind: NodeKindService, value: "service", valid: true},
		{name: "broker", kind: NodeKindBroker, value: "broker", valid: true},
		{name: "destination", kind: NodeKindDestination, value: "destination", valid: true},
		{name: "resource", kind: NodeKindResource, value: "resource", valid: true},
		{name: "consumer", kind: NodeKindConsumer, value: "consumer", valid: true},
		{name: "zero value", kind: NodeKind(""), valid: false},
		{name: "unknown", kind: NodeKind("stream"), valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.kind.IsValid(); got != test.valid {
				t.Errorf("NodeKind(%q).IsValid() = %t, want %t", test.kind, got, test.valid)
			}

			if test.valid && string(test.kind) != test.value {
				t.Errorf("NodeKind value = %q, want %q", test.kind, test.value)
			}
		})
	}
}
