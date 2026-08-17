package topology

import (
	"errors"
	"maps"
	"time"
)

var (
	ErrEvidenceSourceIDInvalid     = errors.New("evidence source ID is invalid")
	ErrEvidenceModeInvalid         = errors.New("evidence mode is invalid")
	ErrEvidenceSourceSystemInvalid = errors.New("evidence source system is invalid")
	ErrEvidenceFirstSeenZero       = errors.New("evidence first seen must not be zero")
	ErrEvidenceLastSeenZero        = errors.New("evidence last seen must not be zero")
	ErrEvidenceTimeRangeInvalid    = errors.New("evidence last seen must not precede first seen")
)

// Evidence records support for a topology relationship from one source.
type Evidence struct {
	sourceID     SourceID
	mode         EvidenceMode
	sourceSystem SourceSystem
	firstSeen    time.Time
	lastSeen     time.Time
	metadata     map[string]string
}

// NewEvidence validates and creates immutable topology evidence.
func NewEvidence(
	sourceID SourceID,
	mode EvidenceMode,
	sourceSystem SourceSystem,
	firstSeen time.Time,
	lastSeen time.Time,
	metadata map[string]string,
) (Evidence, error) {
	if sourceID.String() == "" {
		return Evidence{}, ErrEvidenceSourceIDInvalid
	}
	if !mode.IsValid() {
		return Evidence{}, ErrEvidenceModeInvalid
	}
	if !sourceSystem.IsValid() {
		return Evidence{}, ErrEvidenceSourceSystemInvalid
	}
	if firstSeen.IsZero() {
		return Evidence{}, ErrEvidenceFirstSeenZero
	}
	if lastSeen.IsZero() {
		return Evidence{}, ErrEvidenceLastSeenZero
	}
	if lastSeen.Before(firstSeen) {
		return Evidence{}, ErrEvidenceTimeRangeInvalid
	}

	clonedMetadata := maps.Clone(metadata)
	if clonedMetadata == nil {
		clonedMetadata = make(map[string]string)
	}

	return Evidence{
		sourceID:     sourceID,
		mode:         mode,
		sourceSystem: sourceSystem,
		firstSeen:    firstSeen.UTC(),
		lastSeen:     lastSeen.UTC(),
		metadata:     clonedMetadata,
	}, nil
}

// SourceID returns the configured source instance identity.
func (evidence Evidence) SourceID() SourceID {
	return evidence.sourceID
}

// Mode returns whether the evidence is declared or observed.
func (evidence Evidence) Mode() EvidenceMode {
	return evidence.mode
}

// SourceSystem returns the technology or integration that supplied the evidence.
func (evidence Evidence) SourceSystem() SourceSystem {
	return evidence.sourceSystem
}

// FirstSeen returns when EventAtlas first learned this evidence.
func (evidence Evidence) FirstSeen() time.Time {
	return evidence.firstSeen
}

// LastSeen returns when EventAtlas most recently learned this evidence.
func (evidence Evidence) LastSeen() time.Time {
	return evidence.lastSeen
}

// Metadata returns a copy of the source-specific attributes.
func (evidence Evidence) Metadata() map[string]string {
	return maps.Clone(evidence.metadata)
}
