package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestObservationServiceIngestsOneAtomicBatch(t *testing.T) {
	t.Parallel()

	store := &observationServiceStoreStub{}
	service, err := NewObservationService(store)
	if err != nil {
		t.Fatalf("NewObservationService() error = %v", err)
	}
	facts := []observation.Fact{
		observationServiceTestFact(t, "checkout", "orders.*"),
		observationServiceTestFact(t, "billing", "billing.*"),
	}

	if err := service.Ingest(context.Background(), facts); err != nil {
		t.Fatalf("Ingest() error = %v", err)
	}
	if store.upsertCalls != 1 {
		t.Fatalf("UpsertBatch() calls = %d, want 1", store.upsertCalls)
	}
	if len(store.facts) != 2 || store.facts[0].Key() != facts[0].Key() || store.facts[1].Key() != facts[1].Key() {
		t.Errorf("UpsertBatch() facts = %#v, want input batch", store.facts)
	}

	facts[0] = observation.Fact{}
	if store.facts[0].Key().Service().Name() != "checkout" {
		t.Error("Ingest() exposed its batch slice to mutation by the caller")
	}
}

func TestObservationServiceTreatsEmptyBatchAsSuccessfulNoOp(t *testing.T) {
	t.Parallel()

	store := &observationServiceStoreStub{}
	service, _ := NewObservationService(store)
	if err := service.Ingest(context.Background(), nil); err != nil {
		t.Fatalf("Ingest(nil) error = %v, want nil", err)
	}
	if store.upsertCalls != 0 {
		t.Errorf("UpsertBatch() calls = %d, want 0", store.upsertCalls)
	}
}

func TestObservationServicePropagatesCancellationAndStoreFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("database unavailable")
	store := &observationServiceStoreStub{err: wantErr}
	service, _ := NewObservationService(store)
	facts := []observation.Fact{observationServiceTestFact(t, "checkout", "orders.*")}
	if err := service.Ingest(context.Background(), facts); !errors.Is(err, wantErr) {
		t.Errorf("Ingest() error = %v, want wrapped %v", err, wantErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store.err = nil
	if err := service.Ingest(ctx, facts); !errors.Is(err, context.Canceled) {
		t.Errorf("Ingest(canceled) error = %v, want context canceled", err)
	}
	if store.upsertCalls != 1 {
		t.Errorf("UpsertBatch() calls after cancellation = %d, want original call only", store.upsertCalls)
	}
}

func TestNewObservationServiceRejectsNilStores(t *testing.T) {
	t.Parallel()

	var typedNil *observationServiceStoreStub
	for _, test := range []struct {
		name  string
		store ObservationStore
	}{
		{name: "nil"},
		{name: "typed nil", store: typedNil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewObservationService(test.store); !errors.Is(err, ErrObservationStoreNil) {
				t.Errorf("NewObservationService() error = %v, want %v", err, ErrObservationStoreNil)
			}
		})
	}
}

type observationServiceStoreStub struct {
	facts       []observation.Fact
	err         error
	upsertCalls int
}

func (store *observationServiceStoreStub) UpsertBatch(_ context.Context, facts []observation.Fact) error {
	store.upsertCalls++
	store.facts = facts
	return store.err
}

func (store *observationServiceStoreStub) ListActive(
	context.Context,
	topology.DiscoveryScope,
	time.Time,
) ([]observation.Aggregate, error) {
	return nil, nil
}

func observationServiceTestFact(t *testing.T, serviceName, destinationName string) observation.Fact {
	t.Helper()
	service, _ := observation.NewServiceIdentity("development", "commerce", serviceName)
	destination, _ := observation.NewDestinationHint("nats", destinationName, "")
	sourceID, _ := topology.NewSourceID("observation:otel:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	fact, err := observation.NewFact(observation.FactParams{
		SourceID:         sourceID,
		Scope:            scope,
		ObservedAt:       time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC),
		RelationshipKind: topology.EdgeKindPublishes,
		Service:          service,
		Destination:      destination,
	})
	if err != nil {
		t.Fatalf("NewFact() error = %v", err)
	}
	return fact
}
