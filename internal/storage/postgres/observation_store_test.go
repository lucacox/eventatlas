package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestNewObservationStoreValidatesConfiguration(t *testing.T) {
	t.Parallel()

	if _, err := NewObservationStore(nil); !errors.Is(err, ErrPoolNil) {
		t.Errorf("NewObservationStore(nil) error = %v, want %v", err, ErrPoolNil)
	}
}

func TestObservationStoreValidatesBeforeUsingPool(t *testing.T) {
	t.Parallel()

	store, err := NewObservationStore(new(pgxpool.Pool))
	if err != nil {
		t.Fatalf("NewObservationStore() error = %v", err)
	}
	if err := store.UpsertBatch(context.Background(), []observation.Fact{{}}); !errors.Is(err, observation.ErrFactInvalid) {
		t.Errorf("UpsertBatch(invalid) error = %v, want %v", err, observation.ErrFactInvalid)
	}
	if err := store.UpsertBatch(context.Background(), nil); err != nil {
		t.Errorf("UpsertBatch(empty) error = %v, want nil", err)
	}
	scope, _ := topology.NewDiscoveryScope("account:test")
	if _, err := store.ListActive(context.Background(), topology.DiscoveryScope{}, time.Now()); !errors.Is(err, application.ErrObservationScopeInvalid) {
		t.Errorf("ListActive(invalid scope) error = %v, want %v", err, application.ErrObservationScopeInvalid)
	}
	if _, err := store.ListActive(context.Background(), scope, time.Time{}); !errors.Is(err, application.ErrObservationActiveSinceZero) {
		t.Errorf("ListActive(zero cutoff) error = %v, want %v", err, application.ErrObservationActiveSinceZero)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.UpsertBatch(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("UpsertBatch(canceled) error = %v, want context canceled", err)
	}
	if _, err := store.ListActive(ctx, scope, time.Now()); !errors.Is(err, context.Canceled) {
		t.Errorf("ListActive(canceled) error = %v, want context canceled", err)
	}
}
