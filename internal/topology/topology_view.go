package topology

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrTopologyViewGeneratedAtZero       = errors.New("topology view generated at must not be zero")
	ErrTopologyViewSourcesEmpty          = errors.New("topology view must have at least one contributing source")
	ErrTopologyViewSourceInvalid         = errors.New("topology view contributing source is invalid")
	ErrTopologyViewSourceDuplicate       = errors.New("topology view contains a duplicate contributing source")
	ErrTopologyViewEdgeInvalid           = errors.New("topology view edge is invalid")
	ErrTopologyViewEdgeNodeMissing       = errors.New("topology view edge references a missing node")
	ErrTopologyViewEdgeEndpointsInvalid  = errors.New("topology view edge endpoints are invalid for its kind")
	ErrTopologyViewEdgeDuplicate         = errors.New("topology view contains a duplicate edge")
	ErrTopologyViewEvidenceSourceMissing = errors.New("topology view evidence references a missing contributing source")
	ErrTopologyViewEvidenceDuplicate     = errors.New("topology view edge contains duplicate evidence")
)

// TopologyViewSource describes one declared or observed source contributing to
// a merged view.
type TopologyViewSource struct {
	sourceID SourceID
	mode     EvidenceMode
	latestAt time.Time
}

type topologyViewSourceID struct {
	id   SourceID
	mode EvidenceMode
}

// NewTopologyViewSource creates source provenance for a merged view.
func NewTopologyViewSource(sourceID SourceID, mode EvidenceMode, latestAt time.Time) (TopologyViewSource, error) {
	if sourceID.String() == "" || !mode.IsValid() || latestAt.IsZero() {
		return TopologyViewSource{}, ErrTopologyViewSourceInvalid
	}
	return TopologyViewSource{
		sourceID: sourceID,
		mode:     mode,
		latestAt: latestAt.UTC(),
	}, nil
}

func (source TopologyViewSource) SourceID() SourceID  { return source.sourceID }
func (source TopologyViewSource) Mode() EvidenceMode  { return source.mode }
func (source TopologyViewSource) LatestAt() time.Time { return source.latestAt }

// TopologyViewDiagnostics reports observations that could not safely become
// navigable topology relationships.
type TopologyViewDiagnostics struct {
	unresolvedObservations uint64
	ambiguousObservations  uint64
}

func NewTopologyViewDiagnostics(unresolved, ambiguous uint64) TopologyViewDiagnostics {
	return TopologyViewDiagnostics{
		unresolvedObservations: unresolved,
		ambiguousObservations:  ambiguous,
	}
}

func (diagnostics TopologyViewDiagnostics) UnresolvedObservations() uint64 {
	return diagnostics.unresolvedObservations
}

func (diagnostics TopologyViewDiagnostics) AmbiguousObservations() uint64 {
	return diagnostics.ambiguousObservations
}

// TopologyViewParams contains a merged, multi-source topology projection.
type TopologyViewParams struct {
	GeneratedAt time.Time
	Scope       DiscoveryScope
	Sources     []TopologyViewSource
	Nodes       []TopologyNode
	Edges       []Edge
	Diagnostics TopologyViewDiagnostics
}

// TopologyView is an immutable read model combining declared and observed
// evidence without changing the semantics of a provider snapshot.
type TopologyView struct {
	generatedAt time.Time
	scope       DiscoveryScope
	sources     []TopologyViewSource
	nodes       []TopologyNode
	edges       []Edge
	diagnostics TopologyViewDiagnostics
}

func NewTopologyView(params TopologyViewParams) (*TopologyView, error) {
	if params.GeneratedAt.IsZero() {
		return nil, ErrTopologyViewGeneratedAtZero
	}
	if params.Scope.String() == "" {
		return nil, ErrEmptyDiscoveryScope
	}
	if len(params.Sources) == 0 {
		return nil, ErrTopologyViewSourcesEmpty
	}

	sources := make([]TopologyViewSource, len(params.Sources))
	sourceIDs := make(map[topologyViewSourceID]struct{}, len(params.Sources))
	for index, source := range params.Sources {
		if source.SourceID().String() == "" || !source.Mode().IsValid() || source.LatestAt().IsZero() {
			return nil, fmt.Errorf("%w at index %d", ErrTopologyViewSourceInvalid, index)
		}
		sourceKey := topologyViewSourceID{id: source.SourceID(), mode: source.Mode()}
		if _, exists := sourceIDs[sourceKey]; exists {
			return nil, fmt.Errorf("%w at index %d", ErrTopologyViewSourceDuplicate, index)
		}
		sourceIDs[sourceKey] = struct{}{}
		sources[index] = source
	}

	nodes, nodesByID, err := validateAndCloneSnapshotNodes(params.Nodes)
	if err != nil {
		return nil, err
	}
	if err := validateSnapshotBrokerReferences(nodes, nodesByID); err != nil {
		return nil, err
	}
	edges, err := validateAndCloneTopologyViewEdges(params.Edges, nodesByID, sourceIDs)
	if err != nil {
		return nil, err
	}

	return &TopologyView{
		generatedAt: params.GeneratedAt.UTC(),
		scope:       params.Scope,
		sources:     sources,
		nodes:       nodes,
		edges:       edges,
		diagnostics: params.Diagnostics,
	}, nil
}

func (view *TopologyView) GeneratedAt() time.Time { return view.generatedAt }
func (view *TopologyView) Scope() DiscoveryScope  { return view.scope }

func (view *TopologyView) Sources() []TopologyViewSource {
	return append([]TopologyViewSource(nil), view.sources...)
}

func (view *TopologyView) Nodes() []TopologyNode {
	nodes := make([]TopologyNode, len(view.nodes))
	for index, node := range view.nodes {
		nodes[index] = cloneSnapshotNode(node)
	}
	return nodes
}

func (view *TopologyView) Edges() []Edge {
	edges := make([]Edge, len(view.edges))
	for index, edge := range view.edges {
		edges[index] = edge.clone()
	}
	return edges
}

func (view *TopologyView) Diagnostics() TopologyViewDiagnostics { return view.diagnostics }

type topologyViewEvidenceKey struct {
	sourceID     SourceID
	mode         EvidenceMode
	sourceSystem SourceSystem
}

func validateAndCloneTopologyViewEdges(
	edges []Edge,
	nodesByID map[NodeID]TopologyNode,
	sourceIDs map[topologyViewSourceID]struct{},
) ([]Edge, error) {
	cloned := make([]Edge, len(edges))
	seenEdges := make(map[snapshotEdgeKey]struct{}, len(edges))
	for index, edge := range edges {
		if !edge.isValid() {
			return nil, fmt.Errorf("%w at index %d", ErrTopologyViewEdgeInvalid, index)
		}

		source, sourceExists := nodesByID[edge.SourceID()]
		target, targetExists := nodesByID[edge.TargetID()]
		if !sourceExists || !targetExists {
			return nil, fmt.Errorf("%w at index %d", ErrTopologyViewEdgeNodeMissing, index)
		}
		if !isAllowedEdgeEndpointPair(source.Kind(), target.Kind(), edge.Kind()) {
			return nil, fmt.Errorf("%w at index %d", ErrTopologyViewEdgeEndpointsInvalid, index)
		}

		seenEvidence := make(map[topologyViewEvidenceKey]struct{}, len(edge.evidence))
		for _, evidence := range edge.evidence {
			sourceKey := topologyViewSourceID{id: evidence.SourceID(), mode: evidence.Mode()}
			if _, exists := sourceIDs[sourceKey]; !exists {
				return nil, fmt.Errorf("%w at edge index %d", ErrTopologyViewEvidenceSourceMissing, index)
			}
			evidenceKey := topologyViewEvidenceKey{
				sourceID:     evidence.SourceID(),
				mode:         evidence.Mode(),
				sourceSystem: evidence.SourceSystem(),
			}
			if _, exists := seenEvidence[evidenceKey]; exists {
				return nil, fmt.Errorf("%w at edge index %d", ErrTopologyViewEvidenceDuplicate, index)
			}
			seenEvidence[evidenceKey] = struct{}{}
		}

		if edge.Kind() == EdgeKindBelongsTo {
			brokerID, _ := snapshotNodeBrokerID(source)
			if brokerID != target.ID() {
				return nil, fmt.Errorf("%w at edge index %d", ErrSnapshotBelongsToBrokerMismatch, index)
			}
		}

		edgeKey := snapshotEdgeKey{source: edge.SourceID(), kind: edge.Kind(), target: edge.TargetID()}
		if _, exists := seenEdges[edgeKey]; exists {
			return nil, fmt.Errorf("%w at index %d", ErrTopologyViewEdgeDuplicate, index)
		}
		seenEdges[edgeKey] = struct{}{}

		clonedEdge, err := NewEdge(source, target, edge.Kind(), edge.Evidence())
		if err != nil {
			return nil, fmt.Errorf("%w at index %d: %v", ErrTopologyViewEdgeInvalid, index, err)
		}
		cloned[index] = clonedEdge
	}
	return cloned, nil
}
