package application

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestTopologyViewProjectorMergesResolvedObservations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	snapshot := topologyViewTestSnapshot(t, now.Add(-2*time.Hour))
	scope := snapshot.Scope()
	observationSource, _ := topology.NewSourceID("observation:otel:test")
	service := topologyViewTestServiceIdentity(t, "checkout-api")

	orders := topologyViewTestAggregate(t, topologyViewTestFact(
		t, observationSource, scope, service, "orders.123", "orders.*", now.Add(-3*time.Hour), map[string]string{"delivery": "first"},
	))
	orders, _ = orders.Add(topologyViewTestFact(
		t, observationSource, scope, service, "orders.123", "orders.*", now.Add(-time.Hour), map[string]string{"delivery": "latest"},
	))
	ordersPhysical := topologyViewTestAggregate(t, topologyViewTestFact(
		t, observationSource, scope, service, "orders.*", "", now.Add(-2*time.Hour), map[string]string{"mapping": "physical"},
	))
	billing := topologyViewTestAggregate(t, topologyViewTestFact(
		t, observationSource, scope, service, "billing.created", "", now.Add(-4*time.Hour), nil,
	))
	unresolved := topologyViewTestAggregate(t, topologyViewTestFact(
		t, observationSource, scope, topologyViewTestServiceIdentity(t, "ghost"), "missing", "", now.Add(-30*time.Minute), nil,
	))
	ambiguous := topologyViewTestAggregate(t, topologyViewTestFact(
		t, observationSource, scope, topologyViewTestServiceIdentity(t, "ambiguous"), "unique.one", "shared.*", now.Add(-15*time.Minute), nil,
	))
	store := &topologyViewObservationStoreStub{
		aggregates: []observation.Aggregate{unresolved, billing, ordersPhysical, ambiguous, orders},
	}
	projector, err := NewTopologyViewProjector(store, TopologyViewProjectorConfig{
		Retention: DefaultObservationRetention,
	})
	if err != nil {
		t.Fatalf("NewTopologyViewProjector() error = %v", err)
	}
	projector.clock = func() time.Time { return now }

	view, err := projector.Project(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if store.scope != scope {
		t.Errorf("ListActive() scope = %q, want %q", store.scope.String(), scope.String())
	}
	if want := now.Add(-DefaultObservationRetention); !store.activeSince.Equal(want) {
		t.Errorf("ListActive() activeSince = %v, want %v", store.activeSince, want)
	}

	if got := view.Diagnostics().UnresolvedObservations(); got != 1 {
		t.Errorf("UnresolvedObservations() = %d, want 1", got)
	}
	if got := view.Diagnostics().AmbiguousObservations(); got != 1 {
		t.Errorf("AmbiguousObservations() = %d, want 1", got)
	}
	if got := len(view.Sources()); got != 2 {
		t.Fatalf("Sources() length = %d, want 2", got)
	}
	if source := view.Sources()[1]; source.SourceID() != observationSource || source.Mode() != topology.EvidenceModeObserved || !source.LatestAt().Equal(now.Add(-15*time.Minute)) {
		t.Errorf("observed source = %#v, want latest active observation provenance", source)
	}

	var projectedService *topology.Service
	destinationsByName := make(map[string]*topology.Destination)
	for _, node := range view.Nodes() {
		switch value := node.(type) {
		case *topology.Service:
			if projectedService != nil {
				t.Fatalf("view contains multiple projected services: %s and %s", projectedService.Name(), value.Name())
			}
			projectedService = value
		case *topology.Destination:
			destinationsByName[value.Name()] = value
		}
	}
	if projectedService == nil {
		t.Fatal("view does not contain resolved checkout service")
	}
	if projectedService.Name() != "checkout-api" || projectedService.Environment() != "development" {
		t.Errorf("projected service = %s/%s, want development/checkout-api", projectedService.Environment(), projectedService.Name())
	}
	if namespace, ok := projectedService.Attribute(serviceNamespaceAttribute); !ok || namespace != "commerce" {
		t.Errorf("projected service namespace = %q, %v; want commerce, true", namespace, ok)
	}
	if !strings.HasPrefix(projectedService.ID().String(), "service:eventatlas:") {
		t.Errorf("projected service ID = %q, want application-owned stable prefix", projectedService.ID())
	}

	publishes := make(map[string]topology.Edge)
	for _, edge := range view.Edges() {
		if edge.SourceID() == projectedService.ID() && edge.Kind() == topology.EdgeKindPublishes {
			for name, destination := range destinationsByName {
				if edge.TargetID() == destination.ID() {
					publishes[name] = edge
				}
			}
		}
	}
	if len(publishes) != 2 {
		t.Fatalf("projected publishes edges = %d, want orders and billing", len(publishes))
	}
	ordersEvidence := publishes["orders.*"].Evidence()
	if len(ordersEvidence) != 1 {
		t.Fatalf("orders evidence length = %d, want 1 merged source", len(ordersEvidence))
	}
	evidence := ordersEvidence[0]
	if evidence.Mode() != topology.EvidenceModeObserved || evidence.SourceSystem().String() != openTelemetrySourceSystem {
		t.Errorf("orders evidence mode/system = %s/%s, want observed/opentelemetry", evidence.Mode(), evidence.SourceSystem())
	}
	if got, want := evidence.FirstSeen(), now.Add(-3*time.Hour); !got.Equal(want) {
		t.Errorf("orders FirstSeen() = %v, want %v", got, want)
	}
	if got, want := evidence.LastSeen(), now.Add(-time.Hour); !got.Equal(want) {
		t.Errorf("orders LastSeen() = %v, want %v", got, want)
	}
	if got := evidence.Metadata()[observationCountMetadataAttribute]; got != "3" {
		t.Errorf("orders approximate observation count = %q, want 3", got)
	}
	if got := evidence.Metadata()["delivery"]; got != "latest" {
		t.Errorf("orders latest metadata delivery = %q, want latest", got)
	}

	if got := len(snapshot.Nodes()); got != 5 {
		t.Errorf("declared snapshot nodes changed after projection: got %d, want 5", got)
	}
	if got := len(snapshot.Edges()); got != 4 {
		t.Errorf("declared snapshot edges changed after projection: got %d, want 4", got)
	}
}

func TestProjectedServiceIdentityIsStableAndNamespaceSensitive(t *testing.T) {
	t.Parallel()

	first, _ := observation.NewServiceIdentity("development", "commerce", "checkout")
	equivalent, _ := observation.NewServiceIdentity("development", "commerce", "checkout")
	differentNamespace, _ := observation.NewServiceIdentity("development", "payments", "checkout")
	firstID, err := projectedServiceNodeID(first)
	if err != nil {
		t.Fatalf("projectedServiceNodeID(first) error = %v", err)
	}
	equivalentID, _ := projectedServiceNodeID(equivalent)
	differentID, _ := projectedServiceNodeID(differentNamespace)
	if firstID != equivalentID {
		t.Errorf("equivalent service identities produced %q and %q", firstID, equivalentID)
	}
	if firstID == differentID {
		t.Errorf("different namespaces produced the same service ID %q", firstID)
	}
}

func TestTopologyViewProjectorReportsConfigurationAndStoreFailures(t *testing.T) {
	t.Parallel()

	var nilStore *topologyViewObservationStoreStub
	if _, err := NewTopologyViewProjector(nilStore, TopologyViewProjectorConfig{Retention: time.Hour}); !errors.Is(err, ErrObservationStoreNil) {
		t.Errorf("NewTopologyViewProjector(nil store) error = %v, want %v", err, ErrObservationStoreNil)
	}
	store := &topologyViewObservationStoreStub{}
	if _, err := NewTopologyViewProjector(store, TopologyViewProjectorConfig{}); !errors.Is(err, ErrObservationRetentionInvalid) {
		t.Errorf("NewTopologyViewProjector(zero retention) error = %v, want %v", err, ErrObservationRetentionInvalid)
	}

	projector, _ := NewTopologyViewProjector(store, TopologyViewProjectorConfig{Retention: time.Hour})
	if _, err := projector.Project(context.Background(), nil); !errors.Is(err, ErrSnapshotNil) {
		t.Errorf("Project(nil) error = %v, want %v", err, ErrSnapshotNil)
	}
	store.err = errors.New("store unavailable")
	if _, err := projector.Project(context.Background(), topologyViewTestSnapshot(t, time.Now())); !errors.Is(err, store.err) {
		t.Errorf("Project(store failure) error = %v, want wrapped %v", err, store.err)
	}
}

type topologyViewObservationStoreStub struct {
	aggregates  []observation.Aggregate
	err         error
	scope       topology.DiscoveryScope
	activeSince time.Time
}

func (store *topologyViewObservationStoreStub) UpsertBatch(context.Context, []observation.Fact) error {
	return store.err
}

func (store *topologyViewObservationStoreStub) ListActive(
	_ context.Context,
	scope topology.DiscoveryScope,
	activeSince time.Time,
) ([]observation.Aggregate, error) {
	store.scope = scope
	store.activeSince = activeSince
	return append([]observation.Aggregate(nil), store.aggregates...), store.err
}

func topologyViewTestSnapshot(t *testing.T, capturedAt time.Time) *topology.TopologySnapshot {
	t.Helper()
	sourceID, _ := topology.NewSourceID("provider:nats:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	snapshotID, _ := topology.NewSnapshotID("snapshot:nats:test")
	broker, _ := topology.NewBroker("broker:nats:test", "NATS", "nats", "development", nil)
	destinations := []*topology.Destination{
		topologyViewTestDestination(t, "destination:orders", "orders.*", "", broker.ID()),
		topologyViewTestDestination(t, "destination:billing", "billing.created", "", broker.ID()),
		topologyViewTestDestination(t, "destination:ambiguous-one", "unique.one", "shared.*", broker.ID()),
		topologyViewTestDestination(t, "destination:ambiguous-two", "unique.two", "shared.*", broker.ID()),
	}
	nodes := []topology.TopologyNode{broker}
	evidenceSystem, _ := topology.NewSourceSystem("nats")
	evidence, _ := topology.NewEvidence(sourceID, topology.EvidenceModeDeclared, evidenceSystem, capturedAt, capturedAt, nil)
	edges := make([]topology.Edge, 0, len(destinations))
	for _, destination := range destinations {
		nodes = append(nodes, destination)
		edge, _ := topology.NewEdge(destination, broker, topology.EdgeKindBelongsTo, []topology.Evidence{evidence})
		edges = append(edges, edge)
	}
	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:           snapshotID,
		SourceID:     sourceID,
		Scope:        scope,
		CapturedAt:   capturedAt,
		Nodes:        nodes,
		Edges:        edges,
		Completeness: topology.SnapshotCompletenessFull,
	})
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}
	return snapshot
}

func topologyViewTestDestination(
	t *testing.T,
	id string,
	name string,
	logicalName string,
	brokerID topology.NodeID,
) *topology.Destination {
	t.Helper()
	destination, err := topology.NewDestination(id, name, topology.DestinationKindSubject, brokerID, logicalName, nil)
	if err != nil {
		t.Fatalf("NewDestination() error = %v", err)
	}
	return destination
}

func topologyViewTestServiceIdentity(t *testing.T, name string) observation.ServiceIdentity {
	t.Helper()
	identity, err := observation.NewServiceIdentity("development", "commerce", name)
	if err != nil {
		t.Fatalf("NewServiceIdentity() error = %v", err)
	}
	return identity
}

func topologyViewTestFact(
	t *testing.T,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
	service observation.ServiceIdentity,
	physicalName string,
	logicalName string,
	observedAt time.Time,
	metadata map[string]string,
) observation.Fact {
	t.Helper()
	destination, _ := observation.NewDestinationHint("nats", physicalName, logicalName)
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

func topologyViewTestAggregate(t *testing.T, fact observation.Fact) observation.Aggregate {
	t.Helper()
	aggregate, err := observation.NewAggregate(fact)
	if err != nil {
		t.Fatalf("NewAggregate() error = %v", err)
	}
	return aggregate
}
