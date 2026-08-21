package application

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

const (
	DefaultObservationRetention       = 24 * time.Hour
	serviceNamespaceAttribute         = "service.namespace"
	observationCountMetadataAttribute = "eventatlas.observation_count_approximate"
	openTelemetrySourceSystem         = "opentelemetry"
)

var (
	ErrObservationRetentionInvalid = errors.New("observation retention must be positive")
	ErrProjectedServiceConflict    = errors.New("observed service identity conflicts with an existing topology node")
)

// TopologyViewProjectorConfig controls observation activity without coupling
// the projector to a transport or persistence adapter.
type TopologyViewProjectorConfig struct {
	Retention time.Duration
}

// TopologyViewProjector merges a provider snapshot with active observations.
type TopologyViewProjector struct {
	store     ObservationStore
	retention time.Duration
	clock     func() time.Time
}

func NewTopologyViewProjector(
	store ObservationStore,
	config TopologyViewProjectorConfig,
) (*TopologyViewProjector, error) {
	if isNilInterface(store) {
		return nil, ErrObservationStoreNil
	}
	if config.Retention <= 0 {
		return nil, ErrObservationRetentionInvalid
	}
	return &TopologyViewProjector{
		store:     store,
		retention: config.Retention,
		clock:     time.Now,
	}, nil
}

// Project builds a multi-source read model without mutating the declared
// snapshot. Only observations resolving to exactly one declared destination
// become nodes and edges.
func (projector *TopologyViewProjector) Project(
	ctx context.Context,
	declared *topology.TopologySnapshot,
) (*topology.TopologyView, error) {
	if declared == nil {
		return nil, ErrSnapshotNil
	}

	generatedAt := projector.clock().UTC()
	if generatedAt.IsZero() {
		return nil, topology.ErrTopologyViewGeneratedAtZero
	}
	active, err := projector.store.ListActive(ctx, declared.Scope(), generatedAt.Add(-projector.retention))
	if err != nil {
		return nil, fmt.Errorf("list active topology observations: %w", err)
	}
	slices.SortFunc(active, compareActiveObservations)

	nodes := declared.Nodes()
	nodesByID := make(map[topology.NodeID]topology.TopologyNode, len(nodes))
	for _, node := range nodes {
		nodesByID[node.ID()] = node
	}
	destinations := declaredDestinationCandidates(nodes)

	edgeEvidence := make(map[reconciliationEdgeKey][]topology.Evidence, len(declared.Edges()))
	for _, edge := range declared.Edges() {
		edgeEvidence[edgeReconciliationKey(edge)] = edge.Evidence()
	}
	observedEvidence := make(map[reconciliationEdgeKey]map[reconciliationEvidenceKey]projectedObservationEvidence)
	observedSourceLatest := make(map[topology.SourceID]time.Time)
	unresolved := uint64(0)
	ambiguous := uint64(0)

	for _, aggregate := range active {
		key := aggregate.Key()
		if latest, exists := observedSourceLatest[key.SourceID()]; !exists || aggregate.LastSeen().After(latest) {
			observedSourceLatest[key.SourceID()] = aggregate.LastSeen()
		}

		destination, status := resolveDeclaredDestination(key, destinations)
		switch status {
		case destinationResolutionUnresolved:
			unresolved++
			continue
		case destinationResolutionAmbiguous:
			ambiguous++
			continue
		}

		service, err := projectedService(key.Service(), nodesByID)
		if err != nil {
			return nil, err
		}
		if _, exists := nodesByID[service.ID()]; !exists {
			nodesByID[service.ID()] = service
			nodes = append(nodes, service)
		}

		edgeKey := reconciliationEdgeKey{
			source: service.ID(),
			kind:   key.RelationshipKind(),
			target: destination.ID(),
		}
		evidenceKey := reconciliationEvidenceKey{
			sourceID:     key.SourceID().String(),
			mode:         topology.EvidenceModeObserved,
			sourceSystem: openTelemetrySourceSystem,
		}
		bySource := observedEvidence[edgeKey]
		if bySource == nil {
			bySource = make(map[reconciliationEvidenceKey]projectedObservationEvidence)
			observedEvidence[edgeKey] = bySource
		}
		bySource[evidenceKey] = mergeProjectedObservationEvidence(bySource[evidenceKey], aggregate)
	}
	diagnostics := topology.NewTopologyViewDiagnostics(unresolved, ambiguous)

	for edgeKey, bySource := range observedEvidence {
		evidenceKeys := make([]reconciliationEvidenceKey, 0, len(bySource))
		for evidenceKey := range bySource {
			evidenceKeys = append(evidenceKeys, evidenceKey)
		}
		slices.SortFunc(evidenceKeys, compareReconciliationEvidenceKeys)
		for _, evidenceKey := range evidenceKeys {
			projected := bySource[evidenceKey]
			metadata := maps.Clone(projected.metadata)
			if metadata == nil {
				metadata = make(map[string]string)
			}
			metadata[observationCountMetadataAttribute] = strconv.FormatUint(projected.count, 10)
			sourceID, err := topology.NewSourceID(evidenceKey.sourceID)
			if err != nil {
				return nil, fmt.Errorf("rebuild observed evidence source ID: %w", err)
			}
			sourceSystem, err := topology.NewSourceSystem(evidenceKey.sourceSystem)
			if err != nil {
				return nil, fmt.Errorf("build observed evidence source system: %w", err)
			}
			evidence, err := topology.NewEvidence(
				sourceID,
				evidenceKey.mode,
				sourceSystem,
				projected.firstSeen,
				projected.lastSeen,
				metadata,
			)
			if err != nil {
				return nil, fmt.Errorf("build observed topology evidence: %w", err)
			}
			edgeEvidence[edgeKey] = append(edgeEvidence[edgeKey], evidence)
		}
	}

	edges, err := projectedEdges(edgeEvidence, nodesByID)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(nodes, compareTopologyNodes)

	declaredSource, err := topology.NewTopologyViewSource(
		declared.SourceID(),
		topology.EvidenceModeDeclared,
		declared.CapturedAt(),
	)
	if err != nil {
		return nil, fmt.Errorf("build declared topology view source: %w", err)
	}
	sources := []topology.TopologyViewSource{declaredSource}
	observedSourceIDs := make([]topology.SourceID, 0, len(observedSourceLatest))
	for sourceID := range observedSourceLatest {
		observedSourceIDs = append(observedSourceIDs, sourceID)
	}
	slices.SortFunc(observedSourceIDs, func(left, right topology.SourceID) int {
		return strings.Compare(left.String(), right.String())
	})
	for _, sourceID := range observedSourceIDs {
		source, err := topology.NewTopologyViewSource(
			sourceID,
			topology.EvidenceModeObserved,
			observedSourceLatest[sourceID],
		)
		if err != nil {
			return nil, fmt.Errorf("build observed topology view source: %w", err)
		}
		sources = append(sources, source)
	}

	view, err := topology.NewTopologyView(topology.TopologyViewParams{
		GeneratedAt: generatedAt,
		Scope:       declared.Scope(),
		Sources:     sources,
		Nodes:       nodes,
		Edges:       edges,
		Diagnostics: diagnostics,
	})
	if err != nil {
		return nil, fmt.Errorf("build topology view: %w", err)
	}
	return view, nil
}

type destinationCandidate struct {
	destination *topology.Destination
	broker      *topology.Broker
}

func declaredDestinationCandidates(nodes []topology.TopologyNode) []destinationCandidate {
	brokers := make(map[topology.NodeID]*topology.Broker)
	for _, node := range nodes {
		if broker, ok := node.(*topology.Broker); ok {
			brokers[broker.ID()] = broker
		}
	}

	candidates := make([]destinationCandidate, 0)
	for _, node := range nodes {
		destination, ok := node.(*topology.Destination)
		if !ok {
			continue
		}
		broker := brokers[destination.BrokerID()]
		if broker != nil {
			candidates = append(candidates, destinationCandidate{destination: destination, broker: broker})
		}
	}
	return candidates
}

type destinationResolution uint8

const (
	destinationResolutionResolved destinationResolution = iota
	destinationResolutionUnresolved
	destinationResolutionAmbiguous
)

func resolveDeclaredDestination(
	key observation.Key,
	candidates []destinationCandidate,
) (*topology.Destination, destinationResolution) {
	service := key.Service()
	hint := key.Destination()
	eligible := make([]*topology.Destination, 0)
	for _, candidate := range candidates {
		if !strings.EqualFold(candidate.broker.Provider(), hint.MessagingSystem()) ||
			candidate.broker.Environment() != service.Environment() {
			continue
		}
		eligible = append(eligible, candidate.destination)
	}

	if hint.LogicalName() != "" {
		logical := matchingDestinations(eligible, func(destination *topology.Destination) bool {
			return destination.LogicalName() == hint.LogicalName() || destination.Name() == hint.LogicalName()
		})
		if len(logical) == 1 {
			return logical[0], destinationResolutionResolved
		}
		if len(logical) > 1 {
			return nil, destinationResolutionAmbiguous
		}
	}

	physical := matchingDestinations(eligible, func(destination *topology.Destination) bool {
		return destination.Name() == hint.PhysicalName()
	})
	if len(physical) == 1 {
		return physical[0], destinationResolutionResolved
	}
	if len(physical) > 1 {
		return nil, destinationResolutionAmbiguous
	}
	return nil, destinationResolutionUnresolved
}

func matchingDestinations(
	destinations []*topology.Destination,
	matches func(*topology.Destination) bool,
) []*topology.Destination {
	result := make([]*topology.Destination, 0)
	for _, destination := range destinations {
		if matches(destination) {
			result = append(result, destination)
		}
	}
	return result
}

func projectedService(
	identity observation.ServiceIdentity,
	nodesByID map[topology.NodeID]topology.TopologyNode,
) (*topology.Service, error) {
	id, err := projectedServiceNodeID(identity)
	if err != nil {
		return nil, err
	}
	if existing, exists := nodesByID[id]; exists {
		service, ok := existing.(*topology.Service)
		if !ok || service.Name() != identity.Name() || service.Environment() != identity.Environment() {
			return nil, fmt.Errorf("%w: %s", ErrProjectedServiceConflict, id)
		}
		namespace, exists := service.Attribute(serviceNamespaceAttribute)
		if (identity.Namespace() == "" && exists) || (identity.Namespace() != "" && namespace != identity.Namespace()) {
			return nil, fmt.Errorf("%w: %s", ErrProjectedServiceConflict, id)
		}
		return service, nil
	}

	attributes := make(map[string]string)
	if identity.Namespace() != "" {
		attributes[serviceNamespaceAttribute] = identity.Namespace()
	}
	service, err := topology.NewService(id.String(), identity.Name(), identity.Environment(), attributes)
	if err != nil {
		return nil, fmt.Errorf("build observed service node: %w", err)
	}
	return service, nil
}

func projectedServiceNodeID(identity observation.ServiceIdentity) (topology.NodeID, error) {
	hash := sha256.New()
	var size [8]byte
	for _, part := range []string{identity.Environment(), identity.Namespace(), identity.Name()} {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(part))
	}
	return topology.NewNodeID("service:eventatlas:" + hex.EncodeToString(hash.Sum(nil)))
}

type projectedObservationEvidence struct {
	firstSeen time.Time
	lastSeen  time.Time
	count     uint64
	metadata  map[string]string
}

func mergeProjectedObservationEvidence(
	current projectedObservationEvidence,
	aggregate observation.Aggregate,
) projectedObservationEvidence {
	if current.firstSeen.IsZero() || aggregate.FirstSeen().Before(current.firstSeen) {
		current.firstSeen = aggregate.FirstSeen()
	}
	if current.lastSeen.IsZero() || !aggregate.LastSeen().Before(current.lastSeen) {
		current.lastSeen = aggregate.LastSeen()
		current.metadata = aggregate.Metadata()
	}
	if ^uint64(0)-current.count < aggregate.ObservationCount() {
		current.count = ^uint64(0)
	} else {
		current.count += aggregate.ObservationCount()
	}
	return current
}

func projectedEdges(
	edgeEvidence map[reconciliationEdgeKey][]topology.Evidence,
	nodesByID map[topology.NodeID]topology.TopologyNode,
) ([]topology.Edge, error) {
	edgeKeys := make([]reconciliationEdgeKey, 0, len(edgeEvidence))
	for key := range edgeEvidence {
		edgeKeys = append(edgeKeys, key)
	}
	slices.SortFunc(edgeKeys, compareReconciliationEdgeKeys)

	edges := make([]topology.Edge, 0, len(edgeKeys))
	for _, key := range edgeKeys {
		evidence := edgeEvidence[key]
		slices.SortFunc(evidence, func(left, right topology.Evidence) int {
			return compareReconciliationEvidenceKeys(evidenceReconciliationKey(left), evidenceReconciliationKey(right))
		})
		edge, err := topology.NewEdge(nodesByID[key.source], nodesByID[key.target], key.kind, evidence)
		if err != nil {
			return nil, fmt.Errorf("build projected %s edge: %w", key.kind, err)
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func compareActiveObservations(left, right observation.Aggregate) int {
	leftKey := left.Key()
	rightKey := right.Key()
	values := [][2]string{
		{leftKey.SourceID().String(), rightKey.SourceID().String()},
		{leftKey.Scope().String(), rightKey.Scope().String()},
		{string(leftKey.RelationshipKind()), string(rightKey.RelationshipKind())},
		{leftKey.Service().Environment(), rightKey.Service().Environment()},
		{leftKey.Service().Namespace(), rightKey.Service().Namespace()},
		{leftKey.Service().Name(), rightKey.Service().Name()},
		{leftKey.Destination().MessagingSystem(), rightKey.Destination().MessagingSystem()},
		{leftKey.Destination().LogicalName(), rightKey.Destination().LogicalName()},
		{leftKey.Destination().PhysicalName(), rightKey.Destination().PhysicalName()},
	}
	for _, values := range values {
		if compared := strings.Compare(values[0], values[1]); compared != 0 {
			return compared
		}
	}
	return 0
}

func compareReconciliationEdgeKeys(left, right reconciliationEdgeKey) int {
	if compared := strings.Compare(left.source.String(), right.source.String()); compared != 0 {
		return compared
	}
	if left.kind < right.kind {
		return -1
	}
	if left.kind > right.kind {
		return 1
	}
	return strings.Compare(left.target.String(), right.target.String())
}

func compareReconciliationEvidenceKeys(left, right reconciliationEvidenceKey) int {
	if compared := strings.Compare(left.sourceID, right.sourceID); compared != 0 {
		return compared
	}
	if left.mode < right.mode {
		return -1
	}
	if left.mode > right.mode {
		return 1
	}
	return strings.Compare(left.sourceSystem, right.sourceSystem)
}

func compareTopologyNodes(left, right topology.TopologyNode) int {
	return strings.Compare(left.ID().String(), right.ID().String())
}
