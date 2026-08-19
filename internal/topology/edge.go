package topology

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrEdgeSourceInvalid    = errors.New("edge source is invalid")
	ErrEdgeTargetInvalid    = errors.New("edge target is invalid")
	ErrEdgeKindInvalid      = errors.New("edge kind is invalid")
	ErrEdgeEndpointsInvalid = errors.New("edge endpoints are invalid for its kind")
	ErrEdgeEvidenceEmpty    = errors.New("edge must have at least one evidence record")
	ErrEdgeEvidenceInvalid  = errors.New("edge evidence is invalid")
)

// Edge is a directed relationship between two topology nodes.
//
// An edge represents one normalized fact and can retain evidence from multiple
// independent sources. Its identity is defined by source, kind, and target;
// provenance is not part of that identity.
type Edge struct {
	sourceID NodeID
	targetID NodeID
	kind     EdgeKind
	evidence []Evidence
}

// NewEdge validates the relationship endpoints and requires evidence for the
// topology fact.
func NewEdge(source, target TopologyNode, kind EdgeKind, evidence []Evidence) (Edge, error) {
	if !isValidEdgeNode(source) {
		return Edge{}, ErrEdgeSourceInvalid
	}
	if !isValidEdgeNode(target) {
		return Edge{}, ErrEdgeTargetInvalid
	}
	if !kind.IsValid() {
		return Edge{}, ErrEdgeKindInvalid
	}
	if !isAllowedEdgeEndpointPair(source.Kind(), target.Kind(), kind) {
		return Edge{}, ErrEdgeEndpointsInvalid
	}
	if len(evidence) == 0 {
		return Edge{}, ErrEdgeEvidenceEmpty
	}

	clonedEvidence := make([]Evidence, len(evidence))
	for index, item := range evidence {
		if !item.isValid() {
			return Edge{}, fmt.Errorf("%w at index %d", ErrEdgeEvidenceInvalid, index)
		}
		clonedEvidence[index] = item.clone()
	}

	return Edge{
		sourceID: source.ID(),
		targetID: target.ID(),
		kind:     kind,
		evidence: clonedEvidence,
	}, nil
}

// SourceID returns the source node identity.
func (edge Edge) SourceID() NodeID {
	return edge.sourceID
}

// TargetID returns the target node identity.
func (edge Edge) TargetID() NodeID {
	return edge.targetID
}

// Kind returns the relationship semantics.
func (edge Edge) Kind() EdgeKind {
	return edge.kind
}

// Evidence returns an independent copy of all evidence supporting the edge.
func (edge Edge) Evidence() []Evidence {
	clonedEvidence := make([]Evidence, len(edge.evidence))
	for index, item := range edge.evidence {
		clonedEvidence[index] = item.clone()
	}
	return clonedEvidence
}

func (edge Edge) isValid() bool {
	if edge.sourceID.String() == "" || edge.targetID.String() == "" || !edge.kind.IsValid() || len(edge.evidence) == 0 {
		return false
	}

	for _, item := range edge.evidence {
		if !item.isValid() {
			return false
		}
	}
	return true
}

func (edge Edge) clone() Edge {
	edge.evidence = edge.Evidence()
	return edge
}

func isValidEdgeNode(node TopologyNode) bool {
	if node == nil {
		return false
	}

	value := reflect.ValueOf(node)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return false
		}
	}

	return node.ID().String() != "" && node.Kind().IsValid()
}

func isAllowedEdgeEndpointPair(source, target NodeKind, edge EdgeKind) bool {
	switch edge {
	case EdgeKindPublishes, EdgeKindConsumes:
		return source == NodeKindService && target == NodeKindDestination
	case EdgeKindCapturedBy:
		return source == NodeKindDestination && target == NodeKindResource
	case EdgeKindHasConsumer:
		return (source == NodeKindResource || source == NodeKindDestination) && target == NodeKindConsumer
	case EdgeKindExecutedBy:
		return source == NodeKindConsumer && target == NodeKindService
	case EdgeKindRoutesTo:
		return (source == NodeKindDestination || source == NodeKindResource) &&
			(target == NodeKindDestination || target == NodeKindResource)
	case EdgeKindBelongsTo:
		return (source == NodeKindDestination || source == NodeKindResource || source == NodeKindConsumer) &&
			target == NodeKindBroker
	default:
		return false
	}
}
