package topology

import (
	"errors"
	"testing"
	"time"
)

func TestNewEvidence(t *testing.T) {
	t.Parallel()

	sourceID, err := NewSourceID("provider:nats:production-main")
	if err != nil {
		t.Fatalf("NewSourceID() error = %v", err)
	}
	sourceSystem, err := NewSourceSystem("nats")
	if err != nil {
		t.Fatalf("NewSourceSystem() error = %v", err)
	}

	firstSeen := time.Date(2026, time.August, 17, 10, 0, 0, 0, time.FixedZone("CEST", 2*60*60))
	lastSeen := firstSeen.Add(5 * time.Minute)

	tests := []struct {
		name         string
		sourceID     SourceID
		mode         EvidenceMode
		sourceSystem SourceSystem
		firstSeen    time.Time
		lastSeen     time.Time
		wantErr      error
	}{
		{
			name:         "declared evidence",
			sourceID:     sourceID,
			mode:         EvidenceModeDeclared,
			sourceSystem: sourceSystem,
			firstSeen:    firstSeen,
			lastSeen:     lastSeen,
		},
		{
			name:         "observed evidence with equal timestamps",
			sourceID:     sourceID,
			mode:         EvidenceModeObserved,
			sourceSystem: SourceSystem("opentelemetry"),
			firstSeen:    firstSeen,
			lastSeen:     firstSeen,
		},
		{
			name:         "zero source ID",
			mode:         EvidenceModeDeclared,
			sourceSystem: sourceSystem,
			firstSeen:    firstSeen,
			lastSeen:     lastSeen,
			wantErr:      ErrEvidenceSourceIDInvalid,
		},
		{
			name:         "zero mode",
			sourceID:     sourceID,
			sourceSystem: sourceSystem,
			firstSeen:    firstSeen,
			lastSeen:     lastSeen,
			wantErr:      ErrEvidenceModeInvalid,
		},
		{
			name:         "unsupported mode",
			sourceID:     sourceID,
			mode:         EvidenceMode("inferred"),
			sourceSystem: sourceSystem,
			firstSeen:    firstSeen,
			lastSeen:     lastSeen,
			wantErr:      ErrEvidenceModeInvalid,
		},
		{
			name:      "zero source system",
			sourceID:  sourceID,
			mode:      EvidenceModeDeclared,
			firstSeen: firstSeen,
			lastSeen:  lastSeen,
			wantErr:   ErrEvidenceSourceSystemInvalid,
		},
		{
			name:         "invalid unvalidated source system",
			sourceID:     sourceID,
			mode:         EvidenceModeDeclared,
			sourceSystem: SourceSystem("invalid system"),
			firstSeen:    firstSeen,
			lastSeen:     lastSeen,
			wantErr:      ErrEvidenceSourceSystemInvalid,
		},
		{
			name:         "zero first seen",
			sourceID:     sourceID,
			mode:         EvidenceModeDeclared,
			sourceSystem: sourceSystem,
			lastSeen:     lastSeen,
			wantErr:      ErrEvidenceFirstSeenZero,
		},
		{
			name:         "zero last seen",
			sourceID:     sourceID,
			mode:         EvidenceModeDeclared,
			sourceSystem: sourceSystem,
			firstSeen:    firstSeen,
			wantErr:      ErrEvidenceLastSeenZero,
		},
		{
			name:         "last seen before first seen",
			sourceID:     sourceID,
			mode:         EvidenceModeDeclared,
			sourceSystem: sourceSystem,
			firstSeen:    firstSeen,
			lastSeen:     firstSeen.Add(-time.Nanosecond),
			wantErr:      ErrEvidenceTimeRangeInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			evidence, err := NewEvidence(
				test.sourceID,
				test.mode,
				test.sourceSystem,
				test.firstSeen,
				test.lastSeen,
				nil,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("NewEvidence() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewEvidence() error = %v", err)
			}

			if evidence.SourceID() != test.sourceID {
				t.Errorf("Evidence source ID = %q, want %q", evidence.SourceID(), test.sourceID)
			}
			if evidence.Mode() != test.mode {
				t.Errorf("Evidence mode = %q, want %q", evidence.Mode(), test.mode)
			}
			if evidence.SourceSystem() != test.sourceSystem {
				t.Errorf("Evidence source system = %q, want %q", evidence.SourceSystem(), test.sourceSystem)
			}
			if !evidence.FirstSeen().Equal(test.firstSeen) {
				t.Errorf("Evidence first seen = %v, want %v", evidence.FirstSeen(), test.firstSeen)
			}
			if !evidence.LastSeen().Equal(test.lastSeen) {
				t.Errorf("Evidence last seen = %v, want %v", evidence.LastSeen(), test.lastSeen)
			}
			if evidence.FirstSeen().Location() != time.UTC {
				t.Errorf("Evidence first seen location = %v, want UTC", evidence.FirstSeen().Location())
			}
			if evidence.LastSeen().Location() != time.UTC {
				t.Errorf("Evidence last seen location = %v, want UTC", evidence.LastSeen().Location())
			}
			if evidence.Metadata() == nil {
				t.Error("Evidence metadata = nil, want empty map")
			}
		})
	}
}

func TestNewEvidenceClonesMetadata(t *testing.T) {
	t.Parallel()

	sourceID, err := NewSourceID("observation:otel:production")
	if err != nil {
		t.Fatalf("NewSourceID() error = %v", err)
	}
	sourceSystem, err := NewSourceSystem("opentelemetry")
	if err != nil {
		t.Fatalf("NewSourceSystem() error = %v", err)
	}
	observedAt := time.Date(2026, time.August, 17, 10, 0, 0, 0, time.UTC)
	metadata := map[string]string{"otel.scope": "eventatlas.messaging"}

	evidence, err := NewEvidence(
		sourceID,
		EvidenceModeObserved,
		sourceSystem,
		observedAt,
		observedAt,
		metadata,
	)
	if err != nil {
		t.Fatalf("NewEvidence() error = %v", err)
	}

	metadata["otel.scope"] = "changed"
	if scope := evidence.Metadata()["otel.scope"]; scope != "eventatlas.messaging" {
		t.Errorf("Evidence metadata scope = %q, want %q", scope, "eventatlas.messaging")
	}

	returnedMetadata := evidence.Metadata()
	returnedMetadata["otel.scope"] = "changed"
	if scope := evidence.Metadata()["otel.scope"]; scope != "eventatlas.messaging" {
		t.Errorf("Evidence metadata after returned map mutation = %q, want %q", scope, "eventatlas.messaging")
	}
}
