package topology

import (
	"errors"
	"testing"
	"time"
)

func TestSnapshotValueTypes(t *testing.T) {
	t.Parallel()

	snapshotID, err := NewSnapshotID(" snapshot:nats:production:42 ")
	if err != nil {
		t.Fatalf("NewSnapshotID() error = %v", err)
	}
	if got := snapshotID.String(); got != " snapshot:nats:production:42 " {
		t.Errorf("SnapshotID.String() = %q, want opaque value preserved", got)
	}

	scope, err := NewDiscoveryScope(" nats:production:account:ORDERS ")
	if err != nil {
		t.Fatalf("NewDiscoveryScope() error = %v", err)
	}
	if got := scope.String(); got != " nats:production:account:ORDERS " {
		t.Errorf("DiscoveryScope.String() = %q, want opaque value preserved", got)
	}

	for _, value := range []string{"", " ", "\t\n"} {
		if _, err := NewSnapshotID(value); !errors.Is(err, ErrEmptySnapshotID) {
			t.Errorf("NewSnapshotID(%q) error = %v, want %v", value, err, ErrEmptySnapshotID)
		}
		if _, err := NewDiscoveryScope(value); !errors.Is(err, ErrEmptyDiscoveryScope) {
			t.Errorf("NewDiscoveryScope(%q) error = %v, want %v", value, err, ErrEmptyDiscoveryScope)
		}
	}

	for _, completeness := range []SnapshotCompleteness{SnapshotCompletenessFull, SnapshotCompletenessPartial} {
		if !completeness.IsValid() {
			t.Errorf("SnapshotCompleteness(%q).IsValid() = false, want true", completeness)
		}
	}
	if SnapshotCompleteness("complete").IsValid() {
		t.Error("unsupported SnapshotCompleteness.IsValid() = true, want false")
	}
}

func TestNewTopologySnapshot(t *testing.T) {
	t.Parallel()

	params, _, _, _ := newTopologySnapshotFixture(t)
	snapshot, err := NewTopologySnapshot(params)
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}

	if snapshot.ID() != params.ID {
		t.Errorf("Snapshot ID = %q, want %q", snapshot.ID(), params.ID)
	}
	if snapshot.SourceID() != params.SourceID {
		t.Errorf("Snapshot source ID = %q, want %q", snapshot.SourceID(), params.SourceID)
	}
	if snapshot.Scope() != params.Scope {
		t.Errorf("Snapshot scope = %q, want %q", snapshot.Scope(), params.Scope)
	}
	if !snapshot.CapturedAt().Equal(params.CapturedAt) {
		t.Errorf("Snapshot captured at = %v, want %v", snapshot.CapturedAt(), params.CapturedAt)
	}
	if snapshot.CapturedAt().Location() != time.UTC {
		t.Errorf("Snapshot captured at location = %v, want UTC", snapshot.CapturedAt().Location())
	}
	if got := len(snapshot.Nodes()); got != 3 {
		t.Errorf("Snapshot node count = %d, want 3", got)
	}
	if got := len(snapshot.Edges()); got != 1 {
		t.Errorf("Snapshot edge count = %d, want 1", got)
	}
	if snapshot.Completeness() != SnapshotCompletenessFull {
		t.Errorf("Snapshot completeness = %q, want %q", snapshot.Completeness(), SnapshotCompletenessFull)
	}
	if snapshot.PartialErrors() == nil {
		t.Error("Snapshot partial errors = nil, want empty slice")
	}
	if snapshot.Metadata() == nil {
		t.Error("Snapshot metadata = nil, want empty map")
	}
	if snapshot.Cursor() != "revision:42" {
		t.Errorf("Snapshot cursor = %q, want %q", snapshot.Cursor(), "revision:42")
	}
}

func TestNewTopologySnapshotValidatesCoreFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*TopologySnapshotParams)
		wantErr error
	}{
		{name: "zero snapshot ID", mutate: func(params *TopologySnapshotParams) { params.ID = SnapshotID{} }, wantErr: ErrEmptySnapshotID},
		{name: "zero source ID", mutate: func(params *TopologySnapshotParams) { params.SourceID = SourceID{} }, wantErr: ErrSnapshotSourceIDInvalid},
		{name: "zero scope", mutate: func(params *TopologySnapshotParams) { params.Scope = DiscoveryScope{} }, wantErr: ErrEmptyDiscoveryScope},
		{name: "zero captured at", mutate: func(params *TopologySnapshotParams) { params.CapturedAt = time.Time{} }, wantErr: ErrSnapshotCapturedAtZero},
		{name: "invalid completeness", mutate: func(params *TopologySnapshotParams) { params.Completeness = SnapshotCompleteness("complete") }, wantErr: ErrSnapshotCompletenessInvalid},
		{name: "full with partial errors", mutate: func(params *TopologySnapshotParams) { params.PartialErrors = []string{"stream unavailable"} }, wantErr: ErrSnapshotFullHasPartialErrors},
		{name: "partial without errors", mutate: func(params *TopologySnapshotParams) { params.Completeness = SnapshotCompletenessPartial }, wantErr: ErrSnapshotPartialErrorsEmpty},
		{name: "blank partial error", mutate: func(params *TopologySnapshotParams) {
			params.Completeness = SnapshotCompletenessPartial
			params.PartialErrors = []string{"  "}
		}, wantErr: ErrSnapshotPartialErrorBlank},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			params, _, _, _ := newTopologySnapshotFixture(t)
			test.mutate(&params)
			if _, err := NewTopologySnapshot(params); !errors.Is(err, test.wantErr) {
				t.Errorf("NewTopologySnapshot() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestNewTopologySnapshotValidatesNodes(t *testing.T) {
	t.Parallel()

	t.Run("typed nil node", func(t *testing.T) {
		t.Parallel()
		params, _, _, _ := newTopologySnapshotFixture(t)
		var service *Service
		params.Nodes = append(params.Nodes, service)
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotNodeInvalid) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotNodeInvalid)
		}
	})

	t.Run("generic node is not a core entity", func(t *testing.T) {
		t.Parallel()
		params, _, _, _ := newTopologySnapshotFixture(t)
		node, err := NewNode("generic:service", NodeKindService, "generic", nil)
		if err != nil {
			t.Fatalf("NewNode() error = %v", err)
		}
		params.Nodes = append(params.Nodes, &node)
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotNodeInvalid) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotNodeInvalid)
		}
	})

	t.Run("duplicate node identity", func(t *testing.T) {
		t.Parallel()
		params, broker, _, _ := newTopologySnapshotFixture(t)
		params.Nodes = append(params.Nodes, broker)
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotNodeDuplicate) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotNodeDuplicate)
		}
	})

	t.Run("missing referenced broker", func(t *testing.T) {
		t.Parallel()
		params, _, destination, resource := newTopologySnapshotFixture(t)
		params.Nodes = []TopologyNode{destination, resource}
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotBrokerReferenceInvalid) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotBrokerReferenceInvalid)
		}
	})
}

func TestNewTopologySnapshotValidatesEdges(t *testing.T) {
	t.Parallel()

	t.Run("invalid edge", func(t *testing.T) {
		t.Parallel()
		params, _, _, _ := newTopologySnapshotFixture(t)
		params.Edges = []Edge{{}}
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotEdgeInvalid) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotEdgeInvalid)
		}
	})

	t.Run("missing edge node", func(t *testing.T) {
		t.Parallel()
		params, broker, _, resource := newTopologySnapshotFixture(t)
		params.Nodes = []TopologyNode{broker, resource}
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotEdgeNodeMissing) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotEdgeNodeMissing)
		}
	})

	t.Run("endpoint kind mismatch", func(t *testing.T) {
		t.Parallel()
		params, broker, _, _ := newTopologySnapshotFixture(t)
		params.Edges[0].sourceID = broker.ID()
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotEdgeEndpointsInvalid) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotEdgeEndpointsInvalid)
		}
	})

	t.Run("duplicate normalized edge", func(t *testing.T) {
		t.Parallel()
		params, _, _, _ := newTopologySnapshotFixture(t)
		params.Edges = append(params.Edges, params.Edges[0])
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotEdgeDuplicate) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotEdgeDuplicate)
		}
	})

	t.Run("observed evidence", func(t *testing.T) {
		t.Parallel()
		params, _, destination, resource := newTopologySnapshotFixture(t)
		evidence := newSnapshotEvidence(t, params.SourceID, EvidenceModeObserved)
		edge, err := NewEdge(destination, resource, EdgeKindCapturedBy, []Evidence{evidence})
		if err != nil {
			t.Fatalf("NewEdge() error = %v", err)
		}
		params.Edges = []Edge{edge}
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotEvidenceModeInvalid) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotEvidenceModeInvalid)
		}
	})

	t.Run("evidence from another source", func(t *testing.T) {
		t.Parallel()
		params, _, destination, resource := newTopologySnapshotFixture(t)
		otherSource, err := NewSourceID("provider:nats:another-cluster")
		if err != nil {
			t.Fatalf("NewSourceID() error = %v", err)
		}
		evidence := newSnapshotEvidence(t, otherSource, EvidenceModeDeclared)
		edge, err := NewEdge(destination, resource, EdgeKindCapturedBy, []Evidence{evidence})
		if err != nil {
			t.Fatalf("NewEdge() error = %v", err)
		}
		params.Edges = []Edge{edge}
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotEvidenceSourceMismatch) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotEvidenceSourceMismatch)
		}
	})

	t.Run("belongs-to conflicts with broker ID", func(t *testing.T) {
		t.Parallel()
		params, broker, destination, _ := newTopologySnapshotFixture(t)
		otherBroker, err := NewBroker("broker:production:nats-secondary", "NATS Secondary", "nats", "production", nil)
		if err != nil {
			t.Fatalf("NewBroker() error = %v", err)
		}
		edge, err := NewEdge(destination, otherBroker, EdgeKindBelongsTo, []Evidence{newSnapshotEvidence(t, params.SourceID, EvidenceModeDeclared)})
		if err != nil {
			t.Fatalf("NewEdge() error = %v", err)
		}
		params.Nodes = []TopologyNode{broker, otherBroker, destination}
		params.Edges = []Edge{edge}
		if _, err := NewTopologySnapshot(params); !errors.Is(err, ErrSnapshotBelongsToBrokerMismatch) {
			t.Errorf("NewTopologySnapshot() error = %v, want %v", err, ErrSnapshotBelongsToBrokerMismatch)
		}
	})
}

func TestTopologySnapshotDefensivelyCopiesData(t *testing.T) {
	t.Parallel()

	params, _, destination, _ := newTopologySnapshotFixture(t)
	if err := destination.SetAttribute("nats.account", "ORDERS"); err != nil {
		t.Fatalf("Destination.SetAttribute() error = %v", err)
	}
	params.Edges[0].evidence[0].metadata["nats.filter"] = "orders.*"
	params.Completeness = SnapshotCompletenessPartial
	params.PartialErrors = []string{" stream discovery unavailable "}
	params.Metadata = map[string]string{"nats.server": "nats-1"}

	snapshot, err := NewTopologySnapshot(params)
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}

	_ = destination.SetAttribute("nats.account", "changed")
	params.Edges[0].evidence[0].metadata["nats.filter"] = "changed"
	params.PartialErrors[0] = "changed"
	params.Metadata["nats.server"] = "changed"

	nodes := snapshot.Nodes()
	if got, _ := nodes[1].Attribute("nats.account"); got != "ORDERS" {
		t.Errorf("Snapshot node attribute after input mutation = %q, want %q", got, "ORDERS")
	}
	if got := snapshot.Edges()[0].Evidence()[0].Metadata()["nats.filter"]; got != "orders.*" {
		t.Errorf("Snapshot evidence metadata after input mutation = %q, want %q", got, "orders.*")
	}
	if got := snapshot.PartialErrors()[0]; got != "stream discovery unavailable" {
		t.Errorf("Snapshot partial error = %q, want trimmed original", got)
	}
	if got := snapshot.Metadata()["nats.server"]; got != "nats-1" {
		t.Errorf("Snapshot metadata after input mutation = %q, want %q", got, "nats-1")
	}

	_ = nodes[1].SetAttribute("nats.account", "returned-change")
	edges := snapshot.Edges()
	edges[0].evidence[0].metadata["nats.filter"] = "returned-change"
	partialErrors := snapshot.PartialErrors()
	partialErrors[0] = "returned-change"
	metadata := snapshot.Metadata()
	metadata["nats.server"] = "returned-change"

	if got, _ := snapshot.Nodes()[1].Attribute("nats.account"); got != "ORDERS" {
		t.Errorf("Snapshot node attribute after returned mutation = %q, want %q", got, "ORDERS")
	}
	if got := snapshot.Edges()[0].Evidence()[0].Metadata()["nats.filter"]; got != "orders.*" {
		t.Errorf("Snapshot evidence metadata after returned mutation = %q, want %q", got, "orders.*")
	}
	if got := snapshot.PartialErrors()[0]; got != "stream discovery unavailable" {
		t.Errorf("Snapshot partial error after returned mutation = %q, want original", got)
	}
	if got := snapshot.Metadata()["nats.server"]; got != "nats-1" {
		t.Errorf("Snapshot metadata after returned mutation = %q, want %q", got, "nats-1")
	}
}

func newTopologySnapshotFixture(t *testing.T) (TopologySnapshotParams, *Broker, *Destination, *MessagingResource) {
	t.Helper()

	snapshotID, err := NewSnapshotID("snapshot:nats:production:42")
	if err != nil {
		t.Fatalf("NewSnapshotID() error = %v", err)
	}
	sourceID, err := NewSourceID("provider:nats:production-main")
	if err != nil {
		t.Fatalf("NewSourceID() error = %v", err)
	}
	scope, err := NewDiscoveryScope("nats:production:account:ORDERS")
	if err != nil {
		t.Fatalf("NewDiscoveryScope() error = %v", err)
	}
	broker, err := NewBroker("broker:production:nats-main", "NATS Main", "nats", "production", nil)
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	destination, err := NewDestination(
		"destination:production:nats-main:orders.created",
		"orders.created",
		DestinationKindSubject,
		broker.ID(),
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("NewDestination() error = %v", err)
	}
	resource, err := NewMessagingResource(
		"resource:production:nats-main:ORDERS",
		"ORDERS",
		ResourceKind("nats.jetstream.stream"),
		broker.ID(),
		nil,
	)
	if err != nil {
		t.Fatalf("NewMessagingResource() error = %v", err)
	}
	edge, err := NewEdge(
		destination,
		resource,
		EdgeKindCapturedBy,
		[]Evidence{newSnapshotEvidence(t, sourceID, EvidenceModeDeclared)},
	)
	if err != nil {
		t.Fatalf("NewEdge() error = %v", err)
	}

	return TopologySnapshotParams{
		ID:           snapshotID,
		SourceID:     sourceID,
		Scope:        scope,
		CapturedAt:   time.Date(2026, time.August, 19, 14, 0, 0, 0, time.FixedZone("CEST", 2*60*60)),
		Nodes:        []TopologyNode{broker, destination, resource},
		Edges:        []Edge{edge},
		Completeness: SnapshotCompletenessFull,
		Cursor:       "revision:42",
	}, broker, destination, resource
}

func newSnapshotEvidence(t *testing.T, sourceID SourceID, mode EvidenceMode) Evidence {
	t.Helper()

	system, err := NewSourceSystem("nats")
	if err != nil {
		t.Fatalf("NewSourceSystem() error = %v", err)
	}
	seenAt := time.Date(2026, time.August, 19, 14, 0, 0, 0, time.UTC)
	evidence, err := NewEvidence(sourceID, mode, system, seenAt, seenAt, nil)
	if err != nil {
		t.Fatalf("NewEvidence() error = %v", err)
	}
	return evidence
}
