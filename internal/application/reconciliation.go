package application

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrReconciliationSourceMismatch = errors.New("cannot reconcile snapshots from different sources")
	ErrReconciliationScopeMismatch  = errors.New("cannot reconcile snapshots from different scopes")
)

type reconciliationEdgeKey struct {
	source topology.NodeID
	kind   topology.EdgeKind
	target topology.NodeID
}

type reconciliationEvidenceKey struct {
	sourceID     string
	mode         topology.EvidenceMode
	sourceSystem string
}

// reconcileSnapshots applies source-scoped snapshot semantics before storage.
// Full snapshots define the complete graph, while partial snapshots retain
// facts that the incomplete discovery did not observe.
func reconcileSnapshots(current, incoming *topology.TopologySnapshot) (*topology.TopologySnapshot, error) {
	if current == nil {
		return incoming, nil
	}
	if current.SourceID() != incoming.SourceID() {
		return nil, ErrReconciliationSourceMismatch
	}
	if current.Scope() != incoming.Scope() {
		return nil, ErrReconciliationScopeMismatch
	}

	nodes := reconcileNodes(current, incoming)
	nodesByID := make(map[topology.NodeID]topology.TopologyNode, len(nodes))
	for _, node := range nodes {
		nodesByID[node.ID()] = node
	}
	edges, err := reconcileEdges(current, incoming, nodesByID)
	if err != nil {
		return nil, err
	}

	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:            incoming.ID(),
		SourceID:      incoming.SourceID(),
		Scope:         incoming.Scope(),
		CapturedAt:    incoming.CapturedAt(),
		Nodes:         nodes,
		Edges:         edges,
		Completeness:  incoming.Completeness(),
		PartialErrors: incoming.PartialErrors(),
		Cursor:        incoming.Cursor(),
		Metadata:      incoming.Metadata(),
	})
	if err != nil {
		return nil, fmt.Errorf("build reconciled topology snapshot: %w", err)
	}
	return snapshot, nil
}

func reconcileNodes(current, incoming *topology.TopologySnapshot) []topology.TopologyNode {
	byID := make(map[topology.NodeID]topology.TopologyNode)
	if incoming.Completeness() == topology.SnapshotCompletenessPartial {
		for _, node := range current.Nodes() {
			byID[node.ID()] = node
		}
	}
	for _, node := range incoming.Nodes() {
		byID[node.ID()] = node
	}

	nodes := make([]topology.TopologyNode, 0, len(byID))
	for _, node := range byID {
		nodes = append(nodes, node)
	}
	slices.SortFunc(nodes, func(left, right topology.TopologyNode) int {
		return strings.Compare(left.ID().String(), right.ID().String())
	})
	return nodes
}

func reconcileEdges(
	current, incoming *topology.TopologySnapshot,
	nodesByID map[topology.NodeID]topology.TopologyNode,
) ([]topology.Edge, error) {
	currentByKey := make(map[reconciliationEdgeKey]topology.Edge, len(current.Edges()))
	for _, edge := range current.Edges() {
		currentByKey[edgeReconciliationKey(edge)] = edge
	}

	byKey := make(map[reconciliationEdgeKey]topology.Edge)
	if incoming.Completeness() == topology.SnapshotCompletenessPartial {
		for key, edge := range currentByKey {
			byKey[key] = edge
		}
	}
	for _, edge := range incoming.Edges() {
		key := edgeReconciliationKey(edge)
		evidence, err := reconcileEvidence(currentByKey[key].Evidence(), edge.Evidence())
		if err != nil {
			return nil, fmt.Errorf("reconcile %s edge evidence: %w", edge.Kind(), err)
		}
		reconciled, err := topology.NewEdge(nodesByID[edge.SourceID()], nodesByID[edge.TargetID()], edge.Kind(), evidence)
		if err != nil {
			return nil, fmt.Errorf("rebuild %s edge: %w", edge.Kind(), err)
		}
		byKey[key] = reconciled
	}

	edges := make([]topology.Edge, 0, len(byKey))
	for _, edge := range byKey {
		edges = append(edges, edge)
	}
	slices.SortFunc(edges, func(left, right topology.Edge) int {
		if compared := strings.Compare(left.SourceID().String(), right.SourceID().String()); compared != 0 {
			return compared
		}
		if left.Kind() < right.Kind() {
			return -1
		}
		if left.Kind() > right.Kind() {
			return 1
		}
		return strings.Compare(left.TargetID().String(), right.TargetID().String())
	})
	return edges, nil
}

func reconcileEvidence(current, incoming []topology.Evidence) ([]topology.Evidence, error) {
	currentByKey := make(map[reconciliationEvidenceKey]topology.Evidence, len(current))
	for _, evidence := range current {
		currentByKey[evidenceReconciliationKey(evidence)] = evidence
	}

	reconciled := make([]topology.Evidence, 0, len(incoming))
	for _, evidence := range incoming {
		firstSeen := evidence.FirstSeen()
		lastSeen := evidence.LastSeen()
		if previous, exists := currentByKey[evidenceReconciliationKey(evidence)]; exists {
			if previous.FirstSeen().Before(firstSeen) {
				firstSeen = previous.FirstSeen()
			}
			if previous.LastSeen().After(lastSeen) {
				lastSeen = previous.LastSeen()
			}
		}
		merged, err := topology.NewEvidence(
			evidence.SourceID(),
			evidence.Mode(),
			evidence.SourceSystem(),
			firstSeen,
			lastSeen,
			evidence.Metadata(),
		)
		if err != nil {
			return nil, err
		}
		reconciled = append(reconciled, merged)
	}
	return reconciled, nil
}

func edgeReconciliationKey(edge topology.Edge) reconciliationEdgeKey {
	return reconciliationEdgeKey{source: edge.SourceID(), kind: edge.Kind(), target: edge.TargetID()}
}

func evidenceReconciliationKey(evidence topology.Evidence) reconciliationEvidenceKey {
	return reconciliationEvidenceKey{
		sourceID:     evidence.SourceID().String(),
		mode:         evidence.Mode(),
		sourceSystem: evidence.SourceSystem().String(),
	}
}
