package application

import (
	"context"
	"fmt"

	"github.com/lucacox/eventatlas/internal/observation"
)

// ObservationService coordinates persistence of normalized runtime facts.
// Transport decoding and span eligibility deliberately remain outside this
// application boundary.
type ObservationService struct {
	store ObservationStore
}

func NewObservationService(store ObservationStore) (*ObservationService, error) {
	if isNilInterface(store) {
		return nil, ErrObservationStoreNil
	}
	return &ObservationService{store: store}, nil
}

// Ingest atomically records one already-normalized batch. An empty batch is a
// successful no-op because it contains no durable work.
func (service *ObservationService) Ingest(ctx context.Context, facts []observation.Fact) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(facts) == 0 {
		return nil
	}

	batch := append([]observation.Fact(nil), facts...)
	if err := service.store.UpsertBatch(ctx, batch); err != nil {
		return fmt.Errorf("persist observation batch: %w", err)
	}
	return nil
}
