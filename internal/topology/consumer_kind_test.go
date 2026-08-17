package topology

import (
	"testing"
)

func TestConsumerKindIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  ConsumerKind
		valid bool
	}{
		{name: "nats.jetstream.consumer", kind: ConsumerKind("nats.jetstream.consumer"), valid: true},
		{name: "zero value", kind: ConsumerKind(""), valid: false},
		{name: "invalid kind", kind: ConsumerKind("invalid_Consumer_kind"), valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.kind.IsValid(); got != test.valid {
				t.Errorf("ConsumerKind(%q).IsValid() = %t, wants %t", test.kind, got, test.valid)
			}
		})
	}
}
