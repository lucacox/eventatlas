package topology

import "testing"

func TestEdgeKindIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  EdgeKind
		value string
		valid bool
	}{
		{name: "publishes", kind: EdgeKindPublishes, value: "publishes", valid: true},
		{name: "consumes", kind: EdgeKindConsumes, value: "consumes", valid: true},
		{name: "captured by", kind: EdgeKindCapturedBy, value: "captured_by", valid: true},
		{name: "has consumer", kind: EdgeKindHasConsumer, value: "has_consumer", valid: true},
		{name: "filters", kind: EdgeKindFilters, value: "filters", valid: true},
		{name: "executed by", kind: EdgeKindExecutedBy, value: "executed_by", valid: true},
		{name: "routes to", kind: EdgeKindRoutesTo, value: "routes_to", valid: true},
		{name: "belongs to", kind: EdgeKindBelongsTo, value: "belongs_to", valid: true},
		{name: "zero value", kind: EdgeKind(""), valid: false},
		{name: "unknown", kind: EdgeKind("captured-by"), valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.kind.IsValid(); got != test.valid {
				t.Errorf("EdgeKind(%q).IsValid() = %t, want %t", test.kind, got, test.valid)
			}

			if test.valid && string(test.kind) != test.value {
				t.Errorf("EdgeKind value = %q, want %q", test.kind, test.value)
			}
		})
	}
}
