package application

import (
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestReconcileSnapshotsRetainsUnseenFactsFromPartialSnapshot(t *testing.T) {
	t.Parallel()

	fixture := newReconciliationFixture(t)
	current := fixture.snapshot(t, "snapshot:current", topology.SnapshotCompletenessFull, fixture.firstSeen, true)
	incoming := fixture.snapshot(t, "snapshot:partial", topology.SnapshotCompletenessPartial, fixture.lastSeen, false)

	got, err := reconcileSnapshots(current, incoming)
	if err != nil {
		t.Fatalf("reconcileSnapshots() error = %v", err)
	}
	if got.ID() != incoming.ID() || got.Completeness() != topology.SnapshotCompletenessPartial {
		t.Errorf("reconciled identity/completeness = (%q, %q), want incoming partial snapshot", got.ID(), got.Completeness())
	}
	if len(got.PartialErrors()) != 1 || got.PartialErrors()[0] != "consumer listing incomplete" {
		t.Errorf("partial errors = %v", got.PartialErrors())
	}
	if got.Cursor() != "cursor:snapshot:partial" || got.Metadata()["revision"] != "snapshot:partial" {
		t.Errorf("incoming cursor/metadata were not retained")
	}
	if len(got.Nodes()) != 5 || !snapshotHasNode(got, fixture.retainedDestination.ID()) {
		t.Errorf("partial reconciliation nodes = %d, want retained destination", len(got.Nodes()))
	}
	if len(got.Edges()) != 3 || !snapshotHasEdge(got, fixture.retainedDestination.ID(), topology.EdgeKindCapturedBy, fixture.resource.ID()) {
		t.Errorf("partial reconciliation edges = %d, want retained captured_by edge", len(got.Edges()))
	}

	updated := snapshotEdge(got, fixture.updatedDestination.ID(), topology.EdgeKindCapturedBy, fixture.resource.ID())
	if updated == nil {
		t.Fatal("updated captured_by edge is missing")
	}
	evidence := updated.Evidence()[0]
	if !evidence.FirstSeen().Equal(fixture.firstSeen) || !evidence.LastSeen().Equal(fixture.lastSeen) {
		t.Errorf("merged evidence range = %s..%s, want %s..%s", evidence.FirstSeen(), evidence.LastSeen(), fixture.firstSeen, fixture.lastSeen)
	}
	if evidence.Metadata()["revision"] != "new" {
		t.Errorf("merged evidence metadata = %v, want incoming metadata", evidence.Metadata())
	}
}

func TestReconcileSnapshotsRemovesAbsentFactsFromFullSnapshot(t *testing.T) {
	t.Parallel()

	fixture := newReconciliationFixture(t)
	current := fixture.snapshot(t, "snapshot:current", topology.SnapshotCompletenessFull, fixture.firstSeen, true)
	incoming := fixture.snapshot(t, "snapshot:full", topology.SnapshotCompletenessFull, fixture.lastSeen, false)

	got, err := reconcileSnapshots(current, incoming)
	if err != nil {
		t.Fatalf("reconcileSnapshots() error = %v", err)
	}
	if len(got.Nodes()) != 4 || snapshotHasNode(got, fixture.retainedDestination.ID()) {
		t.Errorf("full reconciliation retained absent node; nodes = %d", len(got.Nodes()))
	}
	if len(got.Edges()) != 2 || snapshotHasEdge(got, fixture.retainedDestination.ID(), topology.EdgeKindCapturedBy, fixture.resource.ID()) {
		t.Errorf("full reconciliation retained absent edge; edges = %d", len(got.Edges()))
	}
}

func TestReconcileSnapshotsRejectsDifferentSourceOrScope(t *testing.T) {
	t.Parallel()

	fixture := newReconciliationFixture(t)
	current := fixture.snapshot(t, "snapshot:current", topology.SnapshotCompletenessFull, fixture.firstSeen, true)
	incoming := fixture.snapshot(t, "snapshot:incoming", topology.SnapshotCompletenessFull, fixture.lastSeen, false)

	otherSource, _ := topology.NewSourceID("provider:nats:other")
	withOtherSource := cloneReconciliationSnapshot(t, incoming, otherSource, incoming.Scope())
	if _, err := reconcileSnapshots(current, withOtherSource); !errors.Is(err, ErrReconciliationSourceMismatch) {
		t.Errorf("source mismatch error = %v, want %v", err, ErrReconciliationSourceMismatch)
	}

	otherScope, _ := topology.NewDiscoveryScope("account:other")
	withOtherScope := cloneReconciliationSnapshot(t, incoming, incoming.SourceID(), otherScope)
	if _, err := reconcileSnapshots(current, withOtherScope); !errors.Is(err, ErrReconciliationScopeMismatch) {
		t.Errorf("scope mismatch error = %v, want %v", err, ErrReconciliationScopeMismatch)
	}
}

type reconciliationFixture struct {
	sourceID            topology.SourceID
	scope               topology.DiscoveryScope
	broker              *topology.Broker
	updatedDestination  *topology.Destination
	retainedDestination *topology.Destination
	resource            *topology.MessagingResource
	consumer            *topology.Consumer
	firstSeen           time.Time
	lastSeen            time.Time
}

func newReconciliationFixture(t *testing.T) reconciliationFixture {
	t.Helper()
	sourceID, _ := topology.NewSourceID("provider:nats:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	broker, err := topology.NewBroker("broker:nats:test", "NATS", "nats", "test", nil)
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	updated, err := topology.NewDestination("destination:orders.created", "orders.created", topology.DestinationKindSubject, broker.ID(), "orders.created", nil)
	if err != nil {
		t.Fatalf("NewDestination(updated) error = %v", err)
	}
	retained, err := topology.NewDestination("destination:orders.cancelled", "orders.cancelled", topology.DestinationKindSubject, broker.ID(), "orders.cancelled", nil)
	if err != nil {
		t.Fatalf("NewDestination(retained) error = %v", err)
	}
	resource, err := topology.NewMessagingResource("resource:orders", "ORDERS", topology.ResourceKind("nats.jetstream.stream"), broker.ID(), nil)
	if err != nil {
		t.Fatalf("NewMessagingResource() error = %v", err)
	}
	consumer, err := topology.NewConsumer("consumer:fulfillment", "fulfillment-worker", topology.ConsumerKind("nats.jetstream.consumer"), topology.DurabilityDurable, broker.ID(), nil)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	return reconciliationFixture{
		sourceID:            sourceID,
		scope:               scope,
		broker:              broker,
		updatedDestination:  updated,
		retainedDestination: retained,
		resource:            resource,
		consumer:            consumer,
		firstSeen:           time.Date(2026, time.August, 20, 8, 0, 0, 0, time.UTC),
		lastSeen:            time.Date(2026, time.August, 20, 9, 0, 0, 0, time.UTC),
	}
}

func (fixture reconciliationFixture) snapshot(
	t *testing.T,
	idValue string,
	completeness topology.SnapshotCompleteness,
	seenAt time.Time,
	includeRetained bool,
) *topology.TopologySnapshot {
	t.Helper()
	metadataRevision := "new"
	if includeRetained {
		metadataRevision = "old"
	}
	evidence, err := topology.NewEvidence(fixture.sourceID, topology.EvidenceModeDeclared, topology.SourceSystem("nats"), seenAt, seenAt, map[string]string{"revision": metadataRevision})
	if err != nil {
		t.Fatalf("NewEvidence() error = %v", err)
	}
	updatedEdge, err := topology.NewEdge(fixture.updatedDestination, fixture.resource, topology.EdgeKindCapturedBy, []topology.Evidence{evidence})
	if err != nil {
		t.Fatalf("NewEdge(updated) error = %v", err)
	}
	bindingEdge, err := topology.NewEdge(fixture.resource, fixture.consumer, topology.EdgeKindHasConsumer, []topology.Evidence{evidence})
	if err != nil {
		t.Fatalf("NewEdge(binding) error = %v", err)
	}
	nodes := []topology.TopologyNode{fixture.broker, fixture.updatedDestination, fixture.resource, fixture.consumer}
	edges := []topology.Edge{updatedEdge, bindingEdge}
	if includeRetained {
		retainedEdge, edgeErr := topology.NewEdge(fixture.retainedDestination, fixture.resource, topology.EdgeKindCapturedBy, []topology.Evidence{evidence})
		if edgeErr != nil {
			t.Fatalf("NewEdge(retained) error = %v", edgeErr)
		}
		nodes = append(nodes, fixture.retainedDestination)
		edges = append(edges, retainedEdge)
	}
	id, _ := topology.NewSnapshotID(idValue)
	partialErrors := []string(nil)
	if completeness == topology.SnapshotCompletenessPartial {
		partialErrors = []string{"consumer listing incomplete"}
	}
	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:            id,
		SourceID:      fixture.sourceID,
		Scope:         fixture.scope,
		CapturedAt:    seenAt,
		Nodes:         nodes,
		Edges:         edges,
		Completeness:  completeness,
		PartialErrors: partialErrors,
		Cursor:        "cursor:" + idValue,
		Metadata:      map[string]string{"revision": idValue},
	})
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}
	return snapshot
}

func cloneReconciliationSnapshot(t *testing.T, snapshot *topology.TopologySnapshot, sourceID topology.SourceID, scope topology.DiscoveryScope) *topology.TopologySnapshot {
	t.Helper()
	// Source changes require matching edge evidence; the mismatch is tested with
	// an empty graph because reconciliation rejects identity before graph merge.
	cloned, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:           snapshot.ID(),
		SourceID:     sourceID,
		Scope:        scope,
		CapturedAt:   snapshot.CapturedAt(),
		Completeness: topology.SnapshotCompletenessFull,
	})
	if err != nil {
		t.Fatalf("NewTopologySnapshot(clone) error = %v", err)
	}
	return cloned
}

func snapshotHasNode(snapshot *topology.TopologySnapshot, id topology.NodeID) bool {
	for _, node := range snapshot.Nodes() {
		if node.ID() == id {
			return true
		}
	}
	return false
}

func snapshotHasEdge(snapshot *topology.TopologySnapshot, source topology.NodeID, kind topology.EdgeKind, target topology.NodeID) bool {
	return snapshotEdge(snapshot, source, kind, target) != nil
}

func snapshotEdge(snapshot *topology.TopologySnapshot, source topology.NodeID, kind topology.EdgeKind, target topology.NodeID) *topology.Edge {
	for _, edge := range snapshot.Edges() {
		if edge.SourceID() == source && edge.Kind() == kind && edge.TargetID() == target {
			copy := edge
			return &copy
		}
	}
	return nil
}
