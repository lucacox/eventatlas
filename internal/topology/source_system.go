package topology

import "errors"

var ErrSourceSystemInvalid = errors.New("source system is invalid")

// SourceSystem identifies the technology or integration that supplied evidence.
type SourceSystem string

// NewSourceSystem validates an open source-system identifier.
func NewSourceSystem(value string) (SourceSystem, error) {
	system := SourceSystem(value)
	if !system.IsValid() {
		return "", ErrSourceSystemInvalid
	}

	return system, nil
}

// IsValid reports whether the source system uses the canonical dotted format.
func (system SourceSystem) IsValid() bool {
	return isValidDottedIdentifier(string(system), 1)
}

// String returns the source-system identifier.
func (system SourceSystem) String() string {
	return string(system)
}
