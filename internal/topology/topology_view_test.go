package topology

import (
	"errors"
	"testing"
	"time"
)

func TestTopologyViewAllowsMixedEvidenceAndDefensivelyCopiesData(t *testing.T) {
	t.Parallel()

	params, broker, service, destination, publishes := newTopologyViewFixture(t)
	view, err := NewTopologyView(params)
	if err != nil {
		t.Fatalf("NewTopologyView() error = %v", err)
	}

	_ = service.SetAttribute("mutated", "input")
	params.Nodes[0] = nil
	params.Sources[0] = TopologyViewSource{}
	if got := view.Nodes()[2].Attributes()["mutated"]; got != "" {
		t.Errorf("view retained a mutable input node: mutated = %q", got)
	}
	if view.Sources()[0].SourceID().String() == "" {
		t.Error("view retained mutable source slice")
	}

	returnedNodes := view.Nodes()
	for _, node := range returnedNodes {
		if node.ID() == broker.ID() {
			_ = node.SetAttribute("mutated", "output")
		}
	}
	for _, node := range view.Nodes() {
		if node.ID() == broker.ID() {
			if got := node.Attributes()["mutated"]; got != "" {
				t.Errorf("Nodes() exposed internal node state: mutated = %q", got)
			}
		}
	}

	returnedEvidence := view.Edges()[1].Evidence()
	metadata := returnedEvidence[0].Metadata()
	metadata["delivery"] = "mutated"
	if got := view.Edges()[1].Evidence()[0].Metadata()["delivery"]; got != "accepted" {
		t.Errorf("Edges() exposed evidence metadata: delivery = %q", got)
	}

	if got := view.GeneratedAt(); got.Location() != time.UTC {
		t.Errorf("GeneratedAt() location = %v, want UTC", got.Location())
	}
	if got := view.Diagnostics().UnresolvedObservations(); got != 2 {
		t.Errorf("UnresolvedObservations() = %d, want 2", got)
	}
	if got := view.Diagnostics().AmbiguousObservations(); got != 1 {
		t.Errorf("AmbiguousObservations() = %d, want 1", got)
	}
	if publishes.Kind() != EdgeKindPublishes || destination.Name() != "orders.*" {
		t.Fatal("invalid topology view fixture")
	}
}

func TestTopologyViewValidatesSourcesAndEdges(t *testing.T) {
	t.Parallel()

	valid, _, _, _, publishes := newTopologyViewFixture(t)

	tests := []struct {
		name   string
		change func(*TopologyViewParams)
		want   error
	}{
		{
			name: "no sources",
			change: func(params *TopologyViewParams) {
				params.Sources = nil
			},
			want: ErrTopologyViewSourcesEmpty,
		},
		{
			name: "duplicate source",
			change: func(params *TopologyViewParams) {
				params.Sources = append(params.Sources, params.Sources[0])
			},
			want: ErrTopologyViewSourceDuplicate,
		},
		{
			name: "evidence source missing",
			change: func(params *TopologyViewParams) {
				params.Sources = params.Sources[:1]
			},
			want: ErrTopologyViewEvidenceSourceMissing,
		},
		{
			name: "duplicate edge",
			change: func(params *TopologyViewParams) {
				params.Edges = append(params.Edges, publishes)
			},
			want: ErrTopologyViewEdgeDuplicate,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := valid
			params.Sources = append([]TopologyViewSource(nil), valid.Sources...)
			params.Edges = append([]Edge(nil), valid.Edges...)
			test.change(&params)
			if _, err := NewTopologyView(params); !errors.Is(err, test.want) {
				t.Errorf("NewTopologyView() error = %v, want %v", err, test.want)
			}
		})
	}
}

func newTopologyViewFixture(
	t *testing.T,
) (TopologyViewParams, *Broker, *Service, *Destination, Edge) {
	t.Helper()
	generatedAt := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	declaredSourceID, _ := NewSourceID("provider:nats:test")
	observedSourceID, _ := NewSourceID("observation:otel:test")
	scope, _ := NewDiscoveryScope("account:test")
	broker, _ := NewBroker("broker:test", "NATS", "nats", "development", nil)
	destination, _ := NewDestination("destination:orders", "orders.*", DestinationKindSubject, broker.ID(), "", nil)
	service, _ := NewService("service:checkout", "checkout", "development", nil)
	declaredSystem, _ := NewSourceSystem("nats")
	observedSystem, _ := NewSourceSystem("opentelemetry")
	declaredEvidence, _ := NewEvidence(
		declaredSourceID,
		EvidenceModeDeclared,
		declaredSystem,
		generatedAt.Add(-time.Hour),
		generatedAt.Add(-time.Hour),
		nil,
	)
	observedEvidence, _ := NewEvidence(
		observedSourceID,
		EvidenceModeObserved,
		observedSystem,
		generatedAt.Add(-30*time.Minute),
		generatedAt,
		map[string]string{"delivery": "accepted"},
	)
	belongsTo, _ := NewEdge(destination, broker, EdgeKindBelongsTo, []Evidence{declaredEvidence})
	publishes, _ := NewEdge(service, destination, EdgeKindPublishes, []Evidence{observedEvidence})
	declaredSource, _ := NewTopologyViewSource(declaredSourceID, EvidenceModeDeclared, generatedAt.Add(-time.Hour))
	observedSource, _ := NewTopologyViewSource(observedSourceID, EvidenceModeObserved, generatedAt)

	return TopologyViewParams{
		GeneratedAt: generatedAt,
		Scope:       scope,
		Sources:     []TopologyViewSource{declaredSource, observedSource},
		Nodes:       []TopologyNode{broker, destination, service},
		Edges:       []Edge{belongsTo, publishes},
		Diagnostics: NewTopologyViewDiagnostics(2, 1),
	}, broker, service, destination, publishes
}
