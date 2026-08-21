//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestTopologyStoreRoundTripsAndAtomicallyReplacesSnapshot(t *testing.T) {
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
	sourceID, _ := topology.NewSourceID("provider:nats:postgres:" + token)
	scope, _ := topology.NewDiscoveryScope("account:postgres:" + token)
	store, err := NewTopologyStore(pool, sourceID, scope)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupContext,
			"DELETE FROM topology_snapshots WHERE source_id = $1 AND discovery_scope = $2",
			sourceID.String(), scope.String(),
		)
	})

	if _, err := store.Current(ctx); !errors.Is(err, application.ErrTopologyNotFound) {
		t.Fatalf("Current() before Replace error = %v, want %v", err, application.ErrTopologyNotFound)
	}

	want := postgresIntegrationSnapshot(t, sourceID, scope, "snapshot:postgres:"+token, "NATS")
	if err := store.Replace(ctx, want); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}

	reopened, err := NewTopologyStore(pool, sourceID, scope)
	if err != nil {
		t.Fatalf("NewTopologyStore(reopened) error = %v", err)
	}
	got, err := reopened.Current(ctx)
	if err != nil {
		t.Fatalf("Current() error = %v", err)
	}
	assertPostgresSnapshot(t, got, want)

	otherSource, _ := topology.NewSourceID("provider:nats:other:" + token)
	otherStore, _ := NewTopologyStore(pool, otherSource, scope)
	if _, err := otherStore.Current(ctx); !errors.Is(err, application.ErrTopologyNotFound) {
		t.Errorf("Current(other source) error = %v, want not found", err)
	}
	if err := otherStore.Replace(ctx, want); !errors.Is(err, ErrSnapshotSourceMismatch) {
		t.Errorf("Replace(source mismatch) error = %v, want %v", err, ErrSnapshotSourceMismatch)
	}
	otherScope, _ := topology.NewDiscoveryScope("account:other:" + token)
	otherScopeStore, _ := NewTopologyStore(pool, sourceID, otherScope)
	if err := otherScopeStore.Replace(ctx, want); !errors.Is(err, ErrSnapshotScopeMismatch) {
		t.Errorf("Replace(scope mismatch) error = %v, want %v", err, ErrSnapshotScopeMismatch)
	}

	installRejectingNodeTrigger(t, ctx, pool)
	rejected := postgresIntegrationSnapshot(t, sourceID, scope, "snapshot:rejected:"+token, "reject-write")
	if err := store.Replace(ctx, rejected); err == nil {
		t.Fatal("Replace(rejected snapshot) error = nil, want database failure")
	}
	stillCurrent, err := store.Current(ctx)
	if err != nil {
		t.Fatalf("Current() after failed replacement error = %v", err)
	}
	if stillCurrent.ID() != want.ID() {
		t.Errorf("snapshot after failed replacement = %q, want %q", stillCurrent.ID(), want.ID())
	}
}

func postgresIntegrationSnapshot(
	t *testing.T,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
	idValue string,
	brokerName string,
) *topology.TopologySnapshot {
	t.Helper()
	broker, err := topology.NewBroker("broker:nats:postgres", brokerName, "nats", "integration", map[string]string{"cluster": "test"})
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	service, err := topology.NewService("service:orders", "order-service", "integration", map[string]string{"version": "1.2.3"})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	destination, err := topology.NewDestination(
		"destination:orders.created", "orders.created", topology.DestinationKindSubject,
		broker.ID(), "orders.created", map[string]string{"nats.subject": "orders.created"},
	)
	if err != nil {
		t.Fatalf("NewDestination() error = %v", err)
	}
	resource, err := topology.NewMessagingResource(
		"resource:orders", "ORDERS", topology.ResourceKind("nats.jetstream.stream"),
		broker.ID(), map[string]string{"nats.jetstream.storage": "file"},
	)
	if err != nil {
		t.Fatalf("NewMessagingResource() error = %v", err)
	}
	consumer, err := topology.NewConsumer(
		"consumer:fulfillment", "fulfillment-worker", topology.ConsumerKind("nats.jetstream.consumer"),
		topology.DurabilityDurable, broker.ID(), map[string]string{"nats.jetstream.stream": "ORDERS"},
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	firstSeen := time.Date(2026, time.August, 20, 10, 0, 0, 0, time.UTC)
	lastSeen := firstSeen.Add(5 * time.Minute)
	evidence, err := topology.NewEvidence(
		sourceID, topology.EvidenceModeDeclared, topology.SourceSystem("nats"),
		firstSeen, lastSeen,
		map[string]string{
			"nats.jetstream.filter_mode":     "subjects",
			"nats.jetstream.filter_subjects": `["orders.*"]`,
		},
	)
	if err != nil {
		t.Fatalf("NewEvidence() error = %v", err)
	}
	edges := make([]topology.Edge, 0, 4)
	for _, definition := range []struct {
		source topology.TopologyNode
		target topology.TopologyNode
		kind   topology.EdgeKind
	}{
		{source: service, target: destination, kind: topology.EdgeKindPublishes},
		{source: destination, target: resource, kind: topology.EdgeKindCapturedBy},
		{source: resource, target: consumer, kind: topology.EdgeKindHasConsumer},
		{source: consumer, target: service, kind: topology.EdgeKindExecutedBy},
	} {
		edge, edgeErr := topology.NewEdge(definition.source, definition.target, definition.kind, []topology.Evidence{evidence})
		if edgeErr != nil {
			t.Fatalf("NewEdge(%s) error = %v", definition.kind, edgeErr)
		}
		edges = append(edges, edge)
	}
	id, _ := topology.NewSnapshotID(idValue)
	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:           id,
		SourceID:     sourceID,
		Scope:        scope,
		CapturedAt:   lastSeen,
		Nodes:        []topology.TopologyNode{broker, service, destination, resource, consumer},
		Edges:        edges,
		Completeness: topology.SnapshotCompletenessFull,
		Cursor:       "revision:42",
		Metadata:     map[string]string{"provider": "nats", "stream_count": "1"},
	})
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}
	return snapshot
}

func assertPostgresSnapshot(t *testing.T, got, want *topology.TopologySnapshot) {
	t.Helper()
	if got.ID() != want.ID() || got.SourceID() != want.SourceID() || got.Scope() != want.Scope() {
		t.Errorf("snapshot identity = (%q, %q, %q), want (%q, %q, %q)", got.ID(), got.SourceID(), got.Scope(), want.ID(), want.SourceID(), want.Scope())
	}
	if !got.CapturedAt().Equal(want.CapturedAt()) || got.Completeness() != want.Completeness() || got.Cursor() != want.Cursor() {
		t.Errorf("snapshot fields were not preserved")
	}
	if !slices.Equal(got.PartialErrors(), want.PartialErrors()) || got.Metadata()["provider"] != "nats" {
		t.Errorf("snapshot diagnostics/metadata were not preserved")
	}
	gotNodes := got.Nodes()
	wantNodes := want.Nodes()
	if len(gotNodes) != len(wantNodes) {
		t.Fatalf("node count = %d, want %d", len(gotNodes), len(wantNodes))
	}
	for index := range wantNodes {
		if gotNodes[index].ID() != wantNodes[index].ID() || gotNodes[index].Kind() != wantNodes[index].Kind() || gotNodes[index].Name() != wantNodes[index].Name() {
			t.Errorf("node %d = (%q, %q, %q), want (%q, %q, %q)", index, gotNodes[index].ID(), gotNodes[index].Kind(), gotNodes[index].Name(), wantNodes[index].ID(), wantNodes[index].Kind(), wantNodes[index].Name())
		}
	}
	if gotNodes[0].(*topology.Broker).Provider() != "nats" || gotNodes[1].(*topology.Service).Environment() != "integration" {
		t.Error("broker/service typed fields were not preserved")
	}
	if gotNodes[2].(*topology.Destination).LogicalName() != "orders.created" || gotNodes[3].(*topology.MessagingResource).ResourceKind() != topology.ResourceKind("nats.jetstream.stream") {
		t.Error("destination/resource typed fields were not preserved")
	}
	if gotNodes[4].(*topology.Consumer).Durability() != topology.DurabilityDurable {
		t.Error("consumer typed fields were not preserved")
	}
	gotEdges := got.Edges()
	wantEdges := want.Edges()
	if len(gotEdges) != len(wantEdges) {
		t.Fatalf("edge count = %d, want %d", len(gotEdges), len(wantEdges))
	}
	for index := range wantEdges {
		if gotEdges[index].SourceID() != wantEdges[index].SourceID() || gotEdges[index].Kind() != wantEdges[index].Kind() || gotEdges[index].TargetID() != wantEdges[index].TargetID() {
			t.Errorf("edge %d was not preserved", index)
		}
		gotEvidence := gotEdges[index].Evidence()[0]
		wantEvidence := wantEdges[index].Evidence()[0]
		if !gotEvidence.FirstSeen().Equal(wantEvidence.FirstSeen()) || !gotEvidence.LastSeen().Equal(wantEvidence.LastSeen()) || gotEvidence.Metadata()["nats.jetstream.filter_subjects"] != `["orders.*"]` {
			t.Errorf("edge %d evidence was not preserved", index)
		}
	}
}

func installRejectingNodeTrigger(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION eventatlas_reject_test_node() RETURNS trigger AS $$
		BEGIN
			IF NEW.name = 'reject-write' THEN
				RAISE EXCEPTION 'intentional integration test failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		DROP TRIGGER IF EXISTS eventatlas_reject_test_node_trigger ON topology_nodes;
		CREATE TRIGGER eventatlas_reject_test_node_trigger
		BEFORE INSERT ON topology_nodes
		FOR EACH ROW EXECUTE FUNCTION eventatlas_reject_test_node();`); err != nil {
		t.Fatalf("install rejecting trigger: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupContext, "DROP TRIGGER IF EXISTS eventatlas_reject_test_node_trigger ON topology_nodes")
		_, _ = pool.Exec(cleanupContext, "DROP FUNCTION IF EXISTS eventatlas_reject_test_node()")
	})
}

func postgresIntegrationToken(t *testing.T) string {
	t.Helper()
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate integration token: %v", err)
	}
	return hex.EncodeToString(value[:])
}
