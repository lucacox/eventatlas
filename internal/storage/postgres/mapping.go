package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lucacox/eventatlas/internal/topology"
)

type storedNodeFields struct {
	brokerID        string
	provider        string
	environment     string
	destinationKind string
	logicalName     string
	resourceKind    string
	consumerKind    string
	durability      string
}

type storedEdge struct {
	sourceID string
	kind     string
	targetID string
}

type storedEdgeKey struct {
	sourceID string
	kind     string
	targetID string
}

func insertNodes(
	ctx context.Context,
	tx pgx.Tx,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
	nodes []topology.TopologyNode,
) error {
	for position, node := range nodes {
		fields, err := fieldsForNode(node)
		if err != nil {
			return fmt.Errorf("map topology node %s: %w", node.ID(), err)
		}
		attributes, err := json.Marshal(node.Attributes())
		if err != nil {
			return fmt.Errorf("encode attributes for node %s: %w", node.ID(), err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO topology_nodes (
				source_id, discovery_scope, node_id, node_position, kind, name,
				broker_id, provider, environment, destination_kind, logical_name,
				resource_kind, consumer_kind, durability, attributes
			) VALUES (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9, $10, $11, $12, $13, $14, $15
			)`,
			sourceID.String(), scope.String(), node.ID().String(), position,
			string(node.Kind()), node.Name(),
			nullableString(fields.brokerID),
			nullableString(fields.provider),
			nullableString(fields.environment),
			nullableString(fields.destinationKind),
			nullableString(fields.logicalName),
			nullableString(fields.resourceKind),
			nullableString(fields.consumerKind),
			nullableString(fields.durability),
			attributes,
		); err != nil {
			return fmt.Errorf("insert topology node %s: %w", node.ID(), err)
		}
	}
	return nil
}

func insertEdges(
	ctx context.Context,
	tx pgx.Tx,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
	edges []topology.Edge,
) error {
	for position, edge := range edges {
		if _, err := tx.Exec(ctx, `
			INSERT INTO topology_edges (
				source_id, discovery_scope, source_node_id, edge_kind,
				target_node_id, edge_position
			) VALUES ($1, $2, $3, $4, $5, $6)`,
			sourceID.String(), scope.String(), edge.SourceID().String(),
			string(edge.Kind()), edge.TargetID().String(), position,
		); err != nil {
			return fmt.Errorf("insert %s topology edge: %w", edge.Kind(), err)
		}
		for evidencePosition, evidence := range edge.Evidence() {
			metadata, err := json.Marshal(evidence.Metadata())
			if err != nil {
				return fmt.Errorf("encode %s edge evidence metadata: %w", edge.Kind(), err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO topology_edge_evidence (
					source_id, discovery_scope, source_node_id, edge_kind,
					target_node_id, evidence_position, evidence_source_id,
					mode, source_system, first_seen, last_seen, metadata
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
				sourceID.String(), scope.String(), edge.SourceID().String(),
				string(edge.Kind()), edge.TargetID().String(), evidencePosition,
				evidence.SourceID().String(), string(evidence.Mode()),
				evidence.SourceSystem().String(), evidence.FirstSeen(),
				evidence.LastSeen(), metadata,
			); err != nil {
				return fmt.Errorf("insert %s topology edge evidence: %w", edge.Kind(), err)
			}
		}
	}
	return nil
}

func fieldsForNode(node topology.TopologyNode) (storedNodeFields, error) {
	switch value := node.(type) {
	case *topology.Broker:
		return storedNodeFields{provider: value.Provider(), environment: value.Environment()}, nil
	case *topology.Service:
		return storedNodeFields{environment: value.Environment()}, nil
	case *topology.Destination:
		return storedNodeFields{
			brokerID:        value.BrokerID().String(),
			destinationKind: string(value.DestinationKind()),
			logicalName:     value.LogicalName(),
		}, nil
	case *topology.MessagingResource:
		return storedNodeFields{
			brokerID:     value.BrokerID().String(),
			resourceKind: string(value.ResourceKind()),
		}, nil
	case *topology.Consumer:
		return storedNodeFields{
			brokerID:     value.BrokerID().String(),
			consumerKind: string(value.ConsumerKind()),
			durability:   string(value.Durability()),
		}, nil
	default:
		return storedNodeFields{}, fmt.Errorf("unsupported topology node type %T", node)
	}
}

func loadSnapshot(ctx context.Context, tx pgx.Tx, sourceID topology.SourceID, scope topology.DiscoveryScope) (*topology.TopologySnapshot, error) {
	var (
		snapshotIDValue    string
		capturedAt         time.Time
		completenessValue  string
		partialErrorsValue string
		cursor             string
		metadataValue      string
	)
	if err := tx.QueryRow(ctx, `
		SELECT snapshot_id, captured_at, completeness, partial_errors::text,
		       cursor, metadata::text
		FROM topology_snapshots
		WHERE source_id = $1 AND discovery_scope = $2`,
		sourceID.String(), scope.String(),
	).Scan(
		&snapshotIDValue,
		&capturedAt,
		&completenessValue,
		&partialErrorsValue,
		&cursor,
		&metadataValue,
	); err != nil {
		return nil, fmt.Errorf("load topology snapshot: %w", err)
	}

	nodes, nodesByID, err := loadNodes(ctx, tx, sourceID, scope)
	if err != nil {
		return nil, err
	}
	edges, err := loadEdges(ctx, tx, sourceID, scope, nodesByID)
	if err != nil {
		return nil, err
	}
	partialErrors, err := decodeStringSlice(partialErrorsValue)
	if err != nil {
		return nil, fmt.Errorf("decode topology snapshot partial errors: %w", err)
	}
	metadata, err := decodeStringMap(metadataValue)
	if err != nil {
		return nil, fmt.Errorf("decode topology snapshot metadata: %w", err)
	}
	snapshotID, err := topology.NewSnapshotID(snapshotIDValue)
	if err != nil {
		return nil, fmt.Errorf("restore topology snapshot ID: %w", err)
	}
	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:            snapshotID,
		SourceID:      sourceID,
		Scope:         scope,
		CapturedAt:    capturedAt,
		Nodes:         nodes,
		Edges:         edges,
		Completeness:  topology.SnapshotCompleteness(completenessValue),
		PartialErrors: partialErrors,
		Cursor:        cursor,
		Metadata:      metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("restore topology snapshot: %w", err)
	}
	return snapshot, nil
}

func loadNodes(
	ctx context.Context,
	tx pgx.Tx,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
) ([]topology.TopologyNode, map[string]topology.TopologyNode, error) {
	rows, err := tx.Query(ctx, `
		SELECT node_id, kind, name, broker_id, provider, environment,
		       destination_kind, logical_name, resource_kind, consumer_kind,
		       durability, attributes::text
		FROM topology_nodes
		WHERE source_id = $1 AND discovery_scope = $2
		ORDER BY node_position`, sourceID.String(), scope.String())
	if err != nil {
		return nil, nil, fmt.Errorf("query topology nodes: %w", err)
	}
	defer rows.Close()

	nodes := make([]topology.TopologyNode, 0)
	byID := make(map[string]topology.TopologyNode)
	for rows.Next() {
		var (
			idValue         string
			kindValue       string
			name            string
			brokerID        sql.NullString
			provider        sql.NullString
			environment     sql.NullString
			destinationKind sql.NullString
			logicalName     sql.NullString
			resourceKind    sql.NullString
			consumerKind    sql.NullString
			durability      sql.NullString
			attributesValue string
		)
		if err := rows.Scan(
			&idValue, &kindValue, &name, &brokerID, &provider, &environment,
			&destinationKind, &logicalName, &resourceKind, &consumerKind,
			&durability, &attributesValue,
		); err != nil {
			return nil, nil, fmt.Errorf("scan topology node: %w", err)
		}
		attributes, err := decodeStringMap(attributesValue)
		if err != nil {
			return nil, nil, fmt.Errorf("decode topology node %s attributes: %w", idValue, err)
		}
		node, err := restoreNode(idValue, topology.NodeKind(kindValue), name, brokerID, provider, environment, destinationKind, logicalName, resourceKind, consumerKind, durability, attributes)
		if err != nil {
			return nil, nil, fmt.Errorf("restore topology node %s: %w", idValue, err)
		}
		nodes = append(nodes, node)
		byID[idValue] = node
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate topology nodes: %w", err)
	}
	return nodes, byID, nil
}

func restoreNode(
	idValue string,
	kind topology.NodeKind,
	name string,
	brokerIDValue, provider, environment, destinationKind, logicalName,
	resourceKind, consumerKind, durability sql.NullString,
	attributes map[string]string,
) (topology.TopologyNode, error) {
	var brokerID topology.NodeID
	var err error
	if brokerIDValue.Valid {
		brokerID, err = topology.NewNodeID(brokerIDValue.String)
		if err != nil {
			return nil, err
		}
	}
	switch kind {
	case topology.NodeKindBroker:
		return topology.NewBroker(idValue, name, provider.String, environment.String, attributes)
	case topology.NodeKindService:
		return topology.NewService(idValue, name, environment.String, attributes)
	case topology.NodeKindDestination:
		return topology.NewDestination(idValue, name, topology.DestinationKind(destinationKind.String), brokerID, logicalName.String, attributes)
	case topology.NodeKindResource:
		return topology.NewMessagingResource(idValue, name, topology.ResourceKind(resourceKind.String), brokerID, attributes)
	case topology.NodeKindConsumer:
		return topology.NewConsumer(idValue, name, topology.ConsumerKind(consumerKind.String), topology.Durability(durability.String), brokerID, attributes)
	default:
		return nil, fmt.Errorf("unsupported node kind %q", kind)
	}
}

func loadEdges(
	ctx context.Context,
	tx pgx.Tx,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
	nodesByID map[string]topology.TopologyNode,
) ([]topology.Edge, error) {
	rows, err := tx.Query(ctx, `
		SELECT source_node_id, edge_kind, target_node_id
		FROM topology_edges
		WHERE source_id = $1 AND discovery_scope = $2
		ORDER BY edge_position`, sourceID.String(), scope.String())
	if err != nil {
		return nil, fmt.Errorf("query topology edges: %w", err)
	}
	stored := make([]storedEdge, 0)
	for rows.Next() {
		var edge storedEdge
		if err := rows.Scan(&edge.sourceID, &edge.kind, &edge.targetID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan topology edge: %w", err)
		}
		stored = append(stored, edge)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate topology edges: %w", err)
	}
	rows.Close()

	evidenceByEdge, err := loadEvidence(ctx, tx, sourceID, scope)
	if err != nil {
		return nil, err
	}
	edges := make([]topology.Edge, 0, len(stored))
	for _, value := range stored {
		key := storedEdgeKey{sourceID: value.sourceID, kind: value.kind, targetID: value.targetID}
		kind := topology.EdgeKind(value.kind)
		edge, err := topology.NewEdge(nodesByID[value.sourceID], nodesByID[value.targetID], kind, evidenceByEdge[key])
		if err != nil {
			return nil, fmt.Errorf("restore %s topology edge: %w", kind, err)
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func loadEvidence(
	ctx context.Context,
	tx pgx.Tx,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
) (map[storedEdgeKey][]topology.Evidence, error) {
	rows, err := tx.Query(ctx, `
		SELECT ev.source_node_id, ev.edge_kind, ev.target_node_id,
		       ev.evidence_source_id, ev.mode, ev.source_system,
		       ev.first_seen, ev.last_seen, ev.metadata::text
		FROM topology_edge_evidence ev
		JOIN topology_edges edge
		  ON edge.source_id = ev.source_id
		 AND edge.discovery_scope = ev.discovery_scope
		 AND edge.source_node_id = ev.source_node_id
		 AND edge.edge_kind = ev.edge_kind
		 AND edge.target_node_id = ev.target_node_id
		WHERE ev.source_id = $1 AND ev.discovery_scope = $2
		ORDER BY edge.edge_position, ev.evidence_position`, sourceID.String(), scope.String())
	if err != nil {
		return nil, fmt.Errorf("query topology edge evidence: %w", err)
	}
	defer rows.Close()

	byEdge := make(map[storedEdgeKey][]topology.Evidence)
	for rows.Next() {
		var (
			key              storedEdgeKey
			evidenceSourceID string
			modeValue        string
			sourceSystem     string
			firstSeen        time.Time
			lastSeen         time.Time
			metadataValue    string
		)
		if err := rows.Scan(
			&key.sourceID, &key.kind, &key.targetID, &evidenceSourceID,
			&modeValue, &sourceSystem, &firstSeen, &lastSeen, &metadataValue,
		); err != nil {
			return nil, fmt.Errorf("scan topology edge evidence: %w", err)
		}
		metadata, err := decodeStringMap(metadataValue)
		if err != nil {
			return nil, fmt.Errorf("decode topology edge evidence metadata: %w", err)
		}
		evidenceSource, err := topology.NewSourceID(evidenceSourceID)
		if err != nil {
			return nil, fmt.Errorf("restore topology evidence source ID: %w", err)
		}
		system, err := topology.NewSourceSystem(sourceSystem)
		if err != nil {
			return nil, fmt.Errorf("restore topology evidence source system: %w", err)
		}
		evidence, err := topology.NewEvidence(evidenceSource, topology.EvidenceMode(modeValue), system, firstSeen, lastSeen, metadata)
		if err != nil {
			return nil, fmt.Errorf("restore topology edge evidence: %w", err)
		}
		byEdge[key] = append(byEdge[key], evidence)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate topology edge evidence: %w", err)
	}
	return byEdge, nil
}

func decodeStringMap(value string) (map[string]string, error) {
	decoded := make(map[string]string)
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func decodeStringSlice(value string) ([]string, error) {
	var decoded []string
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		return nil, err
	}
	if decoded == nil {
		decoded = []string{}
	}
	return decoded, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
