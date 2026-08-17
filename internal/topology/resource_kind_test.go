package topology

import "testing"

func TestResourceKindIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  ResourceKind
		valid bool
	}{
		{name: "NATS stream", kind: ResourceKind("nats.jetstream.stream"), valid: true},
		{name: "numeric suffix", kind: ResourceKind("provider.resource2"), valid: true},
		{name: "underscore", kind: ResourceKind("azure.service_bus"), valid: true},
		{name: "zero value", kind: ResourceKind(""), valid: false},
		{name: "missing namespace", kind: ResourceKind("stream"), valid: false},
		{name: "empty namespace", kind: ResourceKind(".stream"), valid: false},
		{name: "empty kind", kind: ResourceKind("nats."), valid: false},
		{name: "empty middle segment", kind: ResourceKind("nats..stream"), valid: false},
		{name: "uppercase", kind: ResourceKind("NATS.jetstream.stream"), valid: false},
		{name: "leading number", kind: ResourceKind("nats.2stream"), valid: false},
		{name: "hyphen", kind: ResourceKind("nats.jet-stream"), valid: false},
		{name: "whitespace", kind: ResourceKind("nats.jetstream.stream "), valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.kind.IsValid(); got != test.valid {
				t.Errorf("ResourceKind(%q).IsValid() = %t, wants %t", test.kind, got, test.valid)
			}
		})
	}
}
