package topology

// EvidenceMode identifies whether a topology fact is declared or observed.
type EvidenceMode string

const (
	EvidenceModeDeclared EvidenceMode = "declared"
	EvidenceModeObserved EvidenceMode = "observed"
)

// IsValid reports whether the mode belongs to the core evidence model.
func (mode EvidenceMode) IsValid() bool {
	switch mode {
	case EvidenceModeDeclared, EvidenceModeObserved:
		return true
	default:
		return false
	}
}
