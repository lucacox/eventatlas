package topology

import (
	"testing"
)

func TestResourceKindIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  ResourceKind
		valid bool
	}{
		{name: "mats.jetstream.stream", kind: ResourceKind("nats.jetstream.stream"), valid: true},
		{name: "zero value", kind: ResourceKind(""), valid: false},
		{name: "invalid kind", kind: ResourceKind("invalid_resource_kind"), valid: false},
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
