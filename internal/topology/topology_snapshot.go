package topology

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

var (
	ErrEmptySnapshotID                 = errors.New("snapshot ID must not be empty")
	ErrEmptyDiscoveryScope             = errors.New("discovery scope must not be empty")
	ErrSnapshotSourceIDInvalid         = errors.New("snapshot source ID is invalid")
	ErrSnapshotCapturedAtZero          = errors.New("snapshot captured at must not be zero")
	ErrSnapshotCompletenessInvalid     = errors.New("snapshot completeness is invalid")
	ErrSnapshotFullHasPartialErrors    = errors.New("full snapshot cannot contain partial errors")
	ErrSnapshotPartialErrorsEmpty      = errors.New("partial snapshot must contain at least one partial error")
	ErrSnapshotPartialErrorBlank       = errors.New("snapshot partial error cannot be blank")
	ErrSnapshotNodeInvalid             = errors.New("snapshot node is invalid")
	ErrSnapshotNodeDuplicate           = errors.New("snapshot contains a duplicate node")
	ErrSnapshotBrokerReferenceInvalid  = errors.New("snapshot broker reference is invalid")
	ErrSnapshotEdgeInvalid             = errors.New("snapshot edge is invalid")
	ErrSnapshotEdgeNodeMissing         = errors.New("snapshot edge references a missing node")
	ErrSnapshotEdgeEndpointsInvalid    = errors.New("snapshot edge endpoints are invalid for its kind")
	ErrSnapshotEdgeDuplicate           = errors.New("snapshot contains a duplicate edge")
	ErrSnapshotEvidenceModeInvalid     = errors.New("snapshot edge evidence must be declared")
	ErrSnapshotEvidenceSourceMismatch  = errors.New("snapshot edge evidence source does not match the snapshot source")
	ErrSnapshotBelongsToBrokerMismatch = errors.New("snapshot belongs-to edge conflicts with the node broker reference")
)

// SnapshotID is the opaque identity of one discovery result.
type SnapshotID struct {
	value string
}

// NewSnapshotID rejects blank values while preserving provider-defined identity.
func NewSnapshotID(value string) (SnapshotID, error) {
	if strings.TrimSpace(value) == "" {
		return SnapshotID{}, ErrEmptySnapshotID
	}
	return SnapshotID{value: value}, nil
}

// String returns the opaque snapshot identity without normalization.
func (id SnapshotID) String() string {
	return id.value
}

// DiscoveryScope identifies the exact boundary inspected by a provider.
type DiscoveryScope struct {
	value string
}

// NewDiscoveryScope rejects blank values while preserving provider-defined scope.
func NewDiscoveryScope(value string) (DiscoveryScope, error) {
	if strings.TrimSpace(value) == "" {
		return DiscoveryScope{}, ErrEmptyDiscoveryScope
	}
	return DiscoveryScope{value: value}, nil
}

// String returns the opaque discovery scope without normalization.
func (scope DiscoveryScope) String() string {
	return scope.value
}

// SnapshotCompleteness describes whether absence can be used for reconciliation.
type SnapshotCompleteness string

const (
	SnapshotCompletenessFull    SnapshotCompleteness = "full"
	SnapshotCompletenessPartial SnapshotCompleteness = "partial"
)

// IsValid reports whether completeness belongs to the core snapshot model.
func (completeness SnapshotCompleteness) IsValid() bool {
	switch completeness {
	case SnapshotCompletenessFull, SnapshotCompletenessPartial:
		return true
	default:
		return false
	}
}

// TopologySnapshotParams contains the source-scoped result of one discovery run.
type TopologySnapshotParams struct {
	ID            SnapshotID
	SourceID      SourceID
	Scope         DiscoveryScope
	CapturedAt    time.Time
	Nodes         []TopologyNode
	Edges         []Edge
	Completeness  SnapshotCompleteness
	PartialErrors []string
	Cursor        string
	Metadata      map[string]string
}

// TopologySnapshot is an immutable, point-in-time declared topology result.
type TopologySnapshot struct {
	id            SnapshotID
	sourceID      SourceID
	scope         DiscoveryScope
	capturedAt    time.Time
	nodes         []TopologyNode
	edges         []Edge
	completeness  SnapshotCompleteness
	partialErrors []string
	cursor        string
	metadata      map[string]string
}

// NewTopologySnapshot validates and defensively copies a discovery result.
func NewTopologySnapshot(params TopologySnapshotParams) (*TopologySnapshot, error) {
	if params.ID.String() == "" {
		return nil, ErrEmptySnapshotID
	}
	if params.SourceID.String() == "" {
		return nil, ErrSnapshotSourceIDInvalid
	}
	if params.Scope.String() == "" {
		return nil, ErrEmptyDiscoveryScope
	}
	if params.CapturedAt.IsZero() {
		return nil, ErrSnapshotCapturedAtZero
	}
	if !params.Completeness.IsValid() {
		return nil, ErrSnapshotCompletenessInvalid
	}

	partialErrors, err := validateAndClonePartialErrors(params.Completeness, params.PartialErrors)
	if err != nil {
		return nil, err
	}

	nodes, nodesByID, err := validateAndCloneSnapshotNodes(params.Nodes)
	if err != nil {
		return nil, err
	}
	if err := validateSnapshotBrokerReferences(nodes, nodesByID); err != nil {
		return nil, err
	}

	edges, err := validateAndCloneSnapshotEdges(params.Edges, nodesByID, params.SourceID)
	if err != nil {
		return nil, err
	}

	metadata := maps.Clone(params.Metadata)
	if metadata == nil {
		metadata = make(map[string]string)
	}

	return &TopologySnapshot{
		id:            params.ID,
		sourceID:      params.SourceID,
		scope:         params.Scope,
		capturedAt:    params.CapturedAt.UTC(),
		nodes:         nodes,
		edges:         edges,
		completeness:  params.Completeness,
		partialErrors: partialErrors,
		cursor:        params.Cursor,
		metadata:      metadata,
	}, nil
}

func (snapshot *TopologySnapshot) ID() SnapshotID {
	return snapshot.id
}

func (snapshot *TopologySnapshot) SourceID() SourceID {
	return snapshot.sourceID
}

func (snapshot *TopologySnapshot) Scope() DiscoveryScope {
	return snapshot.scope
}

func (snapshot *TopologySnapshot) CapturedAt() time.Time {
	return snapshot.capturedAt
}

func (snapshot *TopologySnapshot) Nodes() []TopologyNode {
	nodes := make([]TopologyNode, len(snapshot.nodes))
	for index, node := range snapshot.nodes {
		nodes[index] = cloneSnapshotNode(node)
	}
	return nodes
}

func (snapshot *TopologySnapshot) Edges() []Edge {
	edges := make([]Edge, len(snapshot.edges))
	for index, edge := range snapshot.edges {
		edges[index] = edge.clone()
	}
	return edges
}

func (snapshot *TopologySnapshot) Completeness() SnapshotCompleteness {
	return snapshot.completeness
}

func (snapshot *TopologySnapshot) PartialErrors() []string {
	partialErrors := make([]string, len(snapshot.partialErrors))
	copy(partialErrors, snapshot.partialErrors)
	return partialErrors
}

func (snapshot *TopologySnapshot) Cursor() string {
	return snapshot.cursor
}

func (snapshot *TopologySnapshot) Metadata() map[string]string {
	return maps.Clone(snapshot.metadata)
}

func validateAndClonePartialErrors(completeness SnapshotCompleteness, partialErrors []string) ([]string, error) {
	if completeness == SnapshotCompletenessFull && len(partialErrors) != 0 {
		return nil, ErrSnapshotFullHasPartialErrors
	}
	if completeness == SnapshotCompletenessPartial && len(partialErrors) == 0 {
		return nil, ErrSnapshotPartialErrorsEmpty
	}

	cloned := make([]string, len(partialErrors))
	for index, partialError := range partialErrors {
		partialError = strings.TrimSpace(partialError)
		if partialError == "" {
			return nil, fmt.Errorf("%w at index %d", ErrSnapshotPartialErrorBlank, index)
		}
		cloned[index] = partialError
	}
	return cloned, nil
}

func validateAndCloneSnapshotNodes(nodes []TopologyNode) ([]TopologyNode, map[NodeID]TopologyNode, error) {
	cloned := make([]TopologyNode, len(nodes))
	byID := make(map[NodeID]TopologyNode, len(nodes))
	for index, node := range nodes {
		clonedNode, err := validateAndCloneSnapshotNode(node)
		if err != nil {
			return nil, nil, fmt.Errorf("%w at index %d: %v", ErrSnapshotNodeInvalid, index, err)
		}
		if _, exists := byID[clonedNode.ID()]; exists {
			return nil, nil, fmt.Errorf("%w: %s", ErrSnapshotNodeDuplicate, clonedNode.ID())
		}
		cloned[index] = clonedNode
		byID[clonedNode.ID()] = clonedNode
	}
	return cloned, byID, nil
}

func validateAndCloneSnapshotNode(node TopologyNode) (TopologyNode, error) {
	if !isValidEdgeNode(node) {
		return nil, ErrSnapshotNodeInvalid
	}

	switch value := node.(type) {
	case *Service:
		if value.Kind() != NodeKindService {
			return nil, ErrSnapshotNodeInvalid
		}
		return NewService(value.ID().String(), value.Name(), value.Environment(), value.Attributes())
	case *Broker:
		if value.Kind() != NodeKindBroker {
			return nil, ErrSnapshotNodeInvalid
		}
		return NewBroker(value.ID().String(), value.Name(), value.Provider(), value.Environment(), value.Attributes())
	case *Destination:
		if value.Kind() != NodeKindDestination {
			return nil, ErrSnapshotNodeInvalid
		}
		return NewDestination(value.ID().String(), value.Name(), value.DestinationKind(), value.BrokerID(), value.LogicalName(), value.Attributes())
	case *MessagingResource:
		if value.Kind() != NodeKindResource {
			return nil, ErrSnapshotNodeInvalid
		}
		return NewMessagingResource(value.ID().String(), value.Name(), value.ResourceKind(), value.BrokerID(), value.Attributes())
	case *Consumer:
		if value.Kind() != NodeKindConsumer {
			return nil, ErrSnapshotNodeInvalid
		}
		return NewConsumer(value.ID().String(), value.Name(), value.ConsumerKind(), value.Durability(), value.BrokerID(), value.Attributes())
	default:
		return nil, errors.New("node must be a core topology entity")
	}
}

func cloneSnapshotNode(node TopologyNode) TopologyNode {
	cloned, err := validateAndCloneSnapshotNode(node)
	if err != nil {
		panic("topology snapshot contains an invalid internal node: " + err.Error())
	}
	return cloned
}

func validateSnapshotBrokerReferences(nodes []TopologyNode, nodesByID map[NodeID]TopologyNode) error {
	for _, node := range nodes {
		brokerID, providerOwned := snapshotNodeBrokerID(node)
		if !providerOwned {
			continue
		}
		broker, exists := nodesByID[brokerID]
		if !exists || broker.Kind() != NodeKindBroker {
			return fmt.Errorf("%w: node %s references %s", ErrSnapshotBrokerReferenceInvalid, node.ID(), brokerID)
		}
	}
	return nil
}

func snapshotNodeBrokerID(node TopologyNode) (NodeID, bool) {
	switch value := node.(type) {
	case *Destination:
		return value.BrokerID(), true
	case *MessagingResource:
		return value.BrokerID(), true
	case *Consumer:
		return value.BrokerID(), true
	default:
		return NodeID{}, false
	}
}

type snapshotEdgeKey struct {
	source NodeID
	kind   EdgeKind
	target NodeID
}

func validateAndCloneSnapshotEdges(edges []Edge, nodesByID map[NodeID]TopologyNode, sourceID SourceID) ([]Edge, error) {
	cloned := make([]Edge, len(edges))
	seen := make(map[snapshotEdgeKey]struct{}, len(edges))
	for index, edge := range edges {
		if !edge.isValid() {
			return nil, fmt.Errorf("%w at index %d", ErrSnapshotEdgeInvalid, index)
		}

		source, sourceExists := nodesByID[edge.SourceID()]
		target, targetExists := nodesByID[edge.TargetID()]
		if !sourceExists || !targetExists {
			return nil, fmt.Errorf("%w at index %d", ErrSnapshotEdgeNodeMissing, index)
		}
		if !isAllowedEdgeEndpointPair(source.Kind(), target.Kind(), edge.Kind()) {
			return nil, fmt.Errorf("%w at index %d", ErrSnapshotEdgeEndpointsInvalid, index)
		}

		for _, evidence := range edge.evidence {
			if evidence.Mode() != EvidenceModeDeclared {
				return nil, fmt.Errorf("%w at edge index %d", ErrSnapshotEvidenceModeInvalid, index)
			}
			if evidence.SourceID() != sourceID {
				return nil, fmt.Errorf("%w at edge index %d", ErrSnapshotEvidenceSourceMismatch, index)
			}
		}

		if edge.Kind() == EdgeKindBelongsTo {
			brokerID, _ := snapshotNodeBrokerID(source)
			if brokerID != target.ID() {
				return nil, fmt.Errorf("%w at edge index %d", ErrSnapshotBelongsToBrokerMismatch, index)
			}
		}

		key := snapshotEdgeKey{source: edge.SourceID(), kind: edge.Kind(), target: edge.TargetID()}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("%w at index %d", ErrSnapshotEdgeDuplicate, index)
		}
		seen[key] = struct{}{}
		cloned[index] = edge.clone()
	}
	return cloned, nil
}
