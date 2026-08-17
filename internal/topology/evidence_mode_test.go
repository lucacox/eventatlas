package topology

import "testing"

func TestEvidenceModeIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		mode  EvidenceMode
		value string
		valid bool
	}{
		{name: "declared", mode: EvidenceModeDeclared, value: "declared", valid: true},
		{name: "observed", mode: EvidenceModeObserved, value: "observed", valid: true},
		{name: "zero value", mode: EvidenceMode(""), valid: false},
		{name: "source system", mode: EvidenceMode("opentelemetry"), valid: false},
		{name: "unknown", mode: EvidenceMode("inferred"), valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.mode.IsValid(); got != test.valid {
				t.Errorf("EvidenceMode(%q).IsValid() = %t, want %t", test.mode, got, test.valid)
			}

			if test.valid && string(test.mode) != test.value {
				t.Errorf("EvidenceMode value = %q, want %q", test.mode, test.value)
			}
		})
	}
}
