//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestObservationStorePersistsAggregatesAndAtomicallyUpsertsBatches(t *testing.T) {
	databaseURL := os.Getenv("EVENTATLAS_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("EVENTATLAS_DATABASE_URL is required for PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("PostgreSQL ping error = %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}

	token := postgresIntegrationToken(t)
	scope, _ := topology.NewDiscoveryScope("account:observations:" + token)
	otherScope, _ := topology.NewDiscoveryScope("account:observations:other:" + token)
	sourceID, _ := topology.NewSourceID("observation:otel:postgres:" + token)
	otherSourceID, _ := topology.NewSourceID("observation:otel:postgres:other:" + token)
	store, err := NewObservationStore(pool)
	if err != nil {
		t.Fatalf("NewObservationStore() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupContext,
			"DELETE FROM topology_observations WHERE discovery_scope = $1 OR discovery_scope = $2",
			scope.String(), otherScope.String(),
		)
	})

	base := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	facts := []observation.Fact{
		postgresObservationFact(t, sourceID, scope, "checkout", "orders.1", "orders.*", base, map[string]string{"delivery": "first"}),
		postgresObservationFactWithRelationship(t, sourceID, scope, "worker", "orders.1", "orders.*", topology.EdgeKindConsumes, base, nil),
		postgresObservationFact(t, sourceID, scope, "checkout", "orders.1", "orders.*", base.Add(-2*time.Hour), map[string]string{"delivery": "late"}),
		postgresObservationFact(t, sourceID, scope, "checkout", "orders.1", "orders.*", base.Add(2*time.Hour), map[string]string{"delivery": "latest"}),
		postgresObservationFact(t, sourceID, scope, "boundary", "boundary", "", base, nil),
		postgresObservationFact(t, sourceID, scope, "expired", "expired", "", base.Add(-time.Second), nil),
		postgresObservationFact(t, otherSourceID, scope, "other-source", "other", "", base.Add(time.Hour), nil),
		postgresObservationFact(t, sourceID, otherScope, "other-scope", "other", "", base.Add(time.Hour), nil),
	}
	if err := store.UpsertBatch(ctx, facts); err != nil {
		t.Fatalf("UpsertBatch() error = %v", err)
	}

	reopened, err := NewObservationStore(pool)
	if err != nil {
		t.Fatalf("NewObservationStore(reopened) error = %v", err)
	}
	active, err := reopened.ListActive(ctx, scope, base)
	if err != nil {
		t.Fatalf("ListActive() error = %v", err)
	}
	if len(active) != 4 {
		t.Fatalf("ListActive() length = %d, want checkout, worker, boundary, and other-source", len(active))
	}
	byService := make(map[string]observation.Aggregate, len(active))
	for _, aggregate := range active {
		byService[aggregate.Key().Service().Name()] = aggregate
	}
	checkout, exists := byService["checkout"]
	if !exists {
		t.Fatal("ListActive() does not contain checkout aggregate")
	}
	if got, want := checkout.FirstSeen(), base.Add(-2*time.Hour); !got.Equal(want) {
		t.Errorf("checkout FirstSeen() = %v, want %v", got, want)
	}
	if got, want := checkout.LastSeen(), base.Add(2*time.Hour); !got.Equal(want) {
		t.Errorf("checkout LastSeen() = %v, want %v", got, want)
	}
	if got := checkout.ObservationCount(); got != 3 {
		t.Errorf("checkout ObservationCount() = %d, want 3", got)
	}
	if got := checkout.Metadata()["delivery"]; got != "latest" {
		t.Errorf("checkout metadata delivery = %q, want latest", got)
	}
	if _, exists := byService["boundary"]; !exists {
		t.Error("observation exactly at activeSince was excluded")
	}
	if worker, exists := byService["worker"]; !exists || worker.Key().RelationshipKind() != topology.EdgeKindConsumes {
		t.Errorf("worker observation = %#v, want consumes aggregate", worker)
	}
	if _, exists := byService["expired"]; exists {
		t.Error("observation before activeSince was included")
	}
	if got := byService["other-source"].Key().SourceID(); got != otherSourceID {
		t.Errorf("other-source SourceID() = %q, want %q", got, otherSourceID)
	}
	otherScopeActive, err := reopened.ListActive(ctx, otherScope, base)
	if err != nil {
		t.Fatalf("ListActive(other scope) error = %v", err)
	}
	if len(otherScopeActive) != 1 || otherScopeActive[0].Key().Service().Name() != "other-scope" {
		t.Errorf("ListActive(other scope) = %#v, want isolated other-scope aggregate", otherScopeActive)
	}

	installRejectingObservationTrigger(t, ctx, pool)
	valid := postgresObservationFact(t, sourceID, scope, "atomic-valid", "atomic.valid", "", base.Add(3*time.Hour), nil)
	rejected := postgresObservationFact(t, sourceID, scope, "reject-write", "atomic.rejected", "", base.Add(3*time.Hour), nil)
	if err := store.UpsertBatch(ctx, []observation.Fact{valid, rejected}); err == nil {
		t.Fatal("UpsertBatch(rejected) error = nil, want database failure")
	}
	afterFailure, err := store.ListActive(ctx, scope, base)
	if err != nil {
		t.Fatalf("ListActive(after failure) error = %v", err)
	}
	for _, aggregate := range afterFailure {
		name := aggregate.Key().Service().Name()
		if name == "atomic-valid" || name == "reject-write" {
			t.Errorf("failed batch persisted service %q", name)
		}
	}

	validBeforeInvalid := postgresObservationFact(t, sourceID, scope, "prevalidated", "prevalidated", "", base.Add(4*time.Hour), nil)
	if err := store.UpsertBatch(ctx, []observation.Fact{validBeforeInvalid, {}}); !errors.Is(err, observation.ErrFactInvalid) {
		t.Fatalf("UpsertBatch(invalid fact) error = %v, want %v", err, observation.ErrFactInvalid)
	}
	var prevalidatedCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM topology_observations
		WHERE discovery_scope = $1 AND service_name = 'prevalidated'`, scope.String(),
	).Scan(&prevalidatedCount); err != nil {
		t.Fatalf("query prevalidated observation count: %v", err)
	}
	if prevalidatedCount != 0 {
		t.Errorf("invalid batch persisted %d prevalidated observations, want 0", prevalidatedCount)
	}
}

func postgresObservationFact(
	t *testing.T,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
	serviceName string,
	physicalName string,
	logicalName string,
	observedAt time.Time,
	metadata map[string]string,
) observation.Fact {
	return postgresObservationFactWithRelationship(t, sourceID, scope, serviceName, physicalName, logicalName, topology.EdgeKindPublishes, observedAt, metadata)
}

func postgresObservationFactWithRelationship(
	t *testing.T,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
	serviceName string,
	physicalName string,
	logicalName string,
	relationship topology.EdgeKind,
	observedAt time.Time,
	metadata map[string]string,
) observation.Fact {
	t.Helper()
	service, err := observation.NewServiceIdentity("integration", "commerce", serviceName)
	if err != nil {
		t.Fatalf("NewServiceIdentity() error = %v", err)
	}
	destination, err := observation.NewDestinationHint("nats", physicalName, logicalName)
	if err != nil {
		t.Fatalf("NewDestinationHint() error = %v", err)
	}
	fact, err := observation.NewFact(observation.FactParams{
		SourceID:         sourceID,
		Scope:            scope,
		ObservedAt:       observedAt,
		RelationshipKind: relationship,
		Service:          service,
		Destination:      destination,
		Metadata:         metadata,
	})
	if err != nil {
		t.Fatalf("NewFact() error = %v", err)
	}
	return fact
}

func installRejectingObservationTrigger(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION eventatlas_reject_test_observation() RETURNS trigger AS $$
		BEGIN
			IF NEW.service_name = 'reject-write' THEN
				RAISE EXCEPTION 'intentional observation integration test failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		DROP TRIGGER IF EXISTS eventatlas_reject_test_observation_trigger ON topology_observations;
		CREATE TRIGGER eventatlas_reject_test_observation_trigger
		BEFORE INSERT OR UPDATE ON topology_observations
		FOR EACH ROW EXECUTE FUNCTION eventatlas_reject_test_observation();`); err != nil {
		t.Fatalf("install rejecting observation trigger: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupContext, "DROP TRIGGER IF EXISTS eventatlas_reject_test_observation_trigger ON topology_observations")
		_, _ = pool.Exec(cleanupContext, "DROP FUNCTION IF EXISTS eventatlas_reject_test_observation()")
	})
}
