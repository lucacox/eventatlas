package application

import (
	"context"
	"errors"
	"time"

	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrObservationStoreNil        = errors.New("observation store cannot be nil")
	ErrObservationScopeInvalid    = errors.New("observation query scope is invalid")
	ErrObservationActiveSinceZero = errors.New("observation active-since time must not be zero")
)

// ObservationStore persists runtime facts independently from authoritative
// provider snapshots.
type ObservationStore interface {
	UpsertBatch(ctx context.Context, facts []observation.Fact) error
	ListActive(ctx context.Context, scope topology.DiscoveryScope, activeSince time.Time) ([]observation.Aggregate, error)
}
