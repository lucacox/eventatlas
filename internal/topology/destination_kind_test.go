package topology

import "testing"

func TestDestinationKindIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  DestinationKind
		value string
		valid bool
	}{
		{name: "subject", kind: DestinationKindSubject, value: "subject", valid: true},
		{name: "topic", kind: DestinationKindTopic, value: "topic", valid: true},
		{name: "queue", kind: DestinationKindQueue, value: "queue", valid: true},
		{name: "exchange", kind: DestinationKindExchange, value: "exchange", valid: true},
		{name: "zero value", kind: DestinationKind(""), value: "", valid: false},
		{name: "unknonw", kind: DestinationKind("Subscriber"), value: "", valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.kind.IsValid(); got != test.valid {
				t.Errorf("DestinationKind(%q).IsValid = %t, want %t", test.kind, got, test.valid)
			}

			if test.valid && string(test.kind) != test.value {
				t.Errorf("DestinationKind value = %q, want %q", test.kind, test.value)
			}
		})
	}
}
