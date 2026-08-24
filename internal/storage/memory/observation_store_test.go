package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestObservationStoreAggregatesAndListsActiveFacts(t *testing.T) {
	t.Parallel()

	store := NewObservationStore()
	scope, _ := topology.NewDiscoveryScope("account:test")
	otherScope, _ := topology.NewDiscoveryScope("account:other")
	base := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	facts := []observation.Fact{
		observationMemoryTestFact(t, scope, "checkout", "orders.*", base, map[string]string{"delivery": "middle"}),
		observationMemoryTestFact(t, scope, "checkout", "orders.*", base.Add(-2*time.Hour), map[string]string{"delivery": "late"}),
		observationMemoryTestFact(t, scope, "checkout", "orders.*", base.Add(time.Hour), map[string]string{"delivery": "latest"}),
		observationMemoryTestFact(t, scope, "billing", "billing.*", base.Add(-3*time.Hour), nil),
		observationMemoryTestFact(t, otherScope, "ignored", "ignored.*", base.Add(time.Hour), nil),
	}
	for _, fact := range facts {
		if err := store.Upsert(context.Background(), fact); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
	}

	active, err := store.ListActive(context.Background(), scope, base)
	if err != nil {
		t.Fatalf("ListActive() error = %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("ListActive() returned %d aggregates, want 1", len(active))
	}
	aggregate := active[0]
	if got := aggregate.Key().Service().Name(); got != "checkout" {
		t.Errorf("active service = %q, want checkout", got)
	}
	if got, want := aggregate.FirstSeen(), base.Add(-2*time.Hour); !got.Equal(want) {
		t.Errorf("FirstSeen() = %v, want %v", got, want)
	}
	if got, want := aggregate.LastSeen(), base.Add(time.Hour); !got.Equal(want) {
		t.Errorf("LastSeen() = %v, want %v", got, want)
	}
	if got := aggregate.ObservationCount(); got != 3 {
		t.Errorf("ObservationCount() = %d, want 3", got)
	}
	if got := aggregate.Metadata()["delivery"]; got != "latest" {
		t.Errorf("Metadata() delivery = %q, want latest", got)
	}

	all, err := store.ListActive(context.Background(), scope, base.Add(-3*time.Hour))
	if err != nil {
		t.Fatalf("ListActive(inclusive cutoff) error = %v", err)
	}
	if len(all) != 2 || all[0].Key().Service().Name() != "billing" || all[1].Key().Service().Name() != "checkout" {
		t.Errorf("ListActive() order/services = %#v, want billing then checkout", all)
	}
}

func TestObservationStoreRejectsInvalidQueriesAndFacts(t *testing.T) {
	t.Parallel()

	store := NewObservationStore()
	scope, _ := topology.NewDiscoveryScope("account:test")
	if err := store.Upsert(context.Background(), observation.Fact{}); !errors.Is(err, observation.ErrFactInvalid) {
		t.Errorf("Upsert(zero fact) error = %v, want %v", err, observation.ErrFactInvalid)
	}
	if _, err := store.ListActive(context.Background(), topology.DiscoveryScope{}, time.Now()); !errors.Is(err, application.ErrObservationScopeInvalid) {
		t.Errorf("ListActive(zero scope) error = %v, want %v", err, application.ErrObservationScopeInvalid)
	}
	if _, err := store.ListActive(context.Background(), scope, time.Time{}); !errors.Is(err, application.ErrObservationActiveSinceZero) {
		t.Errorf("ListActive(zero cutoff) error = %v, want %v", err, application.ErrObservationActiveSinceZero)
	}
}

func TestObservationStoreUpsertsBatchesAtomically(t *testing.T) {
	t.Parallel()

	store := NewObservationStore()
	scope, _ := topology.NewDiscoveryScope("account:test")
	observedAt := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	first := observationMemoryTestFact(t, scope, "checkout", "orders.*", observedAt, nil)
	second := observationMemoryTestFact(t, scope, "checkout", "orders.*", observedAt.Add(time.Minute), nil)
	if err := store.UpsertBatch(context.Background(), []observation.Fact{first, second}); err != nil {
		t.Fatalf("UpsertBatch() error = %v", err)
	}

	validNew := observationMemoryTestFact(t, scope, "billing", "billing.*", observedAt, nil)
	if err := store.UpsertBatch(context.Background(), []observation.Fact{validNew, {}}); !errors.Is(err, observation.ErrFactInvalid) {
		t.Fatalf("UpsertBatch(invalid) error = %v, want %v", err, observation.ErrFactInvalid)
	}
	active, err := store.ListActive(context.Background(), scope, observedAt.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListActive() error = %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("ListActive() length = %d, want only the previously committed aggregate", len(active))
	}
	if got := active[0].ObservationCount(); got != 2 {
		t.Errorf("ObservationCount() = %d, want 2 facts from the successful batch", got)
	}
}

func TestObservationStoreHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := NewObservationStore()
	scope, _ := topology.NewDiscoveryScope("account:test")
	fact := observationMemoryTestFact(t, scope, "checkout", "orders.*", time.Now(), nil)

	if err := store.Upsert(ctx, fact); !errors.Is(err, context.Canceled) {
		t.Errorf("Upsert() error = %v, want context canceled", err)
	}
	if _, err := store.ListActive(ctx, scope, time.Now()); !errors.Is(err, context.Canceled) {
		t.Errorf("ListActive() error = %v, want context canceled", err)
	}
}

func observationMemoryTestFact(
	t *testing.T,
	scope topology.DiscoveryScope,
	serviceName string,
	destinationName string,
	observedAt time.Time,
	metadata map[string]string,
) observation.Fact {
	t.Helper()
	service, _ := observation.NewServiceIdentity("development", "", serviceName)
	destination, _ := observation.NewDestinationHint("nats", destinationName, "")
	sourceID, _ := topology.NewSourceID("observation:otel:test")
	fact, err := observation.NewFact(observation.FactParams{
		SourceID:         sourceID,
		Scope:            scope,
		ObservedAt:       observedAt,
		RelationshipKind: topology.EdgeKindPublishes,
		Service:          service,
		Destination:      destination,
		Metadata:         metadata,
	})
	if err != nil {
		t.Fatalf("NewFact() error = %v", err)
	}
	return fact
}
