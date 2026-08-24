package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

const maxPostgresObservationCount int64 = 1<<63 - 1

// ObservationStore persists runtime observations independently from declared
// topology snapshots.
type ObservationStore struct {
	pool *pgxpool.Pool
}

func NewObservationStore(pool *pgxpool.Pool) (*ObservationStore, error) {
	if pool == nil {
		return nil, ErrPoolNil
	}
	return &ObservationStore{pool: pool}, nil
}

// Upsert is a convenience for callers recording a single normalized fact.
func (store *ObservationStore) Upsert(ctx context.Context, fact observation.Fact) error {
	return store.UpsertBatch(ctx, []observation.Fact{fact})
}

// UpsertBatch validates the complete batch before atomically updating its
// aggregates. Repeated deliveries extend the time range and increment an
// approximate, saturating count.
func (store *ObservationStore) UpsertBatch(ctx context.Context, facts []observation.Fact) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	writes := make([]observationWrite, len(facts))
	for index, fact := range facts {
		if _, err := observation.NewAggregate(fact); err != nil {
			return fmt.Errorf("validate observation fact at index %d: %w", index, err)
		}
		metadata, err := json.Marshal(fact.Metadata())
		if err != nil {
			return fmt.Errorf("encode observation fact metadata at index %d: %w", index, err)
		}
		writes[index] = observationWrite{fact: fact, metadata: metadata}
	}
	if len(writes) == 0 {
		return nil
	}

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin observation batch upsert: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for index, write := range writes {
		if err := upsertObservation(ctx, tx, write); err != nil {
			return fmt.Errorf("upsert observation fact at index %d: %w", index, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit observation batch upsert: %w", err)
	}
	return nil
}

func (store *ObservationStore) ListActive(
	ctx context.Context,
	scope topology.DiscoveryScope,
	activeSince time.Time,
) ([]observation.Aggregate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scope.String() == "" {
		return nil, application.ErrObservationScopeInvalid
	}
	if activeSince.IsZero() {
		return nil, application.ErrObservationActiveSinceZero
	}

	rows, err := store.pool.Query(ctx, `
		SELECT source_id, relationship_kind,
		       service_environment, service_namespace, service_name,
		       messaging_system, destination_physical_name, destination_logical_name,
		       first_seen, last_seen, observation_count, metadata::text
		FROM topology_observations
		WHERE discovery_scope = $1 AND last_seen >= $2
		ORDER BY source_id, relationship_kind,
		         service_environment, service_namespace, service_name,
		         messaging_system, destination_logical_name, destination_physical_name`,
		scope.String(), activeSince.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("query active observations: %w", err)
	}
	defer rows.Close()

	aggregates := make([]observation.Aggregate, 0)
	for rows.Next() {
		aggregate, err := scanObservationAggregate(rows, scope)
		if err != nil {
			return nil, err
		}
		aggregates = append(aggregates, aggregate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active observations: %w", err)
	}
	slices.SortFunc(aggregates, compareStoredObservationAggregates)
	return aggregates, nil
}

type observationWrite struct {
	fact     observation.Fact
	metadata []byte
}

func upsertObservation(ctx context.Context, tx pgx.Tx, write observationWrite) error {
	fact := write.fact
	service := fact.Service()
	destination := fact.Destination()
	_, err := tx.Exec(ctx, `
		INSERT INTO topology_observations (
			source_id, discovery_scope, relationship_kind,
			service_environment, service_namespace, service_name,
			messaging_system, destination_physical_name, destination_logical_name,
			first_seen, last_seen, observation_count, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, 1, $11
		)
		ON CONFLICT (
			source_id, discovery_scope, relationship_kind,
			service_environment, service_namespace, service_name,
			messaging_system, destination_physical_name, destination_logical_name
		) DO UPDATE SET
			first_seen = LEAST(topology_observations.first_seen, EXCLUDED.first_seen),
			last_seen = GREATEST(topology_observations.last_seen, EXCLUDED.last_seen),
			observation_count = CASE
				WHEN topology_observations.observation_count < $12
				THEN topology_observations.observation_count + 1
				ELSE topology_observations.observation_count
			END,
			metadata = CASE
				WHEN EXCLUDED.last_seen >= topology_observations.last_seen
				THEN EXCLUDED.metadata
				ELSE topology_observations.metadata
			END,
			updated_at = now()`,
		fact.SourceID().String(),
		fact.Scope().String(),
		string(fact.RelationshipKind()),
		service.Environment(),
		service.Namespace(),
		service.Name(),
		destination.MessagingSystem(),
		destination.PhysicalName(),
		destination.LogicalName(),
		fact.ObservedAt(),
		write.metadata,
		maxPostgresObservationCount,
	)
	return err
}

type observationRowScanner interface {
	Scan(dest ...any) error
}

func scanObservationAggregate(
	row observationRowScanner,
	scope topology.DiscoveryScope,
) (observation.Aggregate, error) {
	var (
		sourceIDValue         string
		relationshipKindValue string
		serviceEnvironment    string
		serviceNamespace      string
		serviceName           string
		messagingSystem       string
		destinationPhysical   string
		destinationLogical    string
		firstSeen             time.Time
		lastSeen              time.Time
		observationCount      int64
		metadataValue         string
	)
	if err := row.Scan(
		&sourceIDValue,
		&relationshipKindValue,
		&serviceEnvironment,
		&serviceNamespace,
		&serviceName,
		&messagingSystem,
		&destinationPhysical,
		&destinationLogical,
		&firstSeen,
		&lastSeen,
		&observationCount,
		&metadataValue,
	); err != nil {
		return observation.Aggregate{}, fmt.Errorf("scan active observation: %w", err)
	}
	if observationCount <= 0 {
		return observation.Aggregate{}, fmt.Errorf("restore observation aggregate: invalid count %d", observationCount)
	}
	metadata, err := decodeStringMap(metadataValue)
	if err != nil {
		return observation.Aggregate{}, fmt.Errorf("decode observation metadata: %w", err)
	}
	sourceID, err := topology.NewSourceID(sourceIDValue)
	if err != nil {
		return observation.Aggregate{}, fmt.Errorf("restore observation source ID: %w", err)
	}
	service, err := observation.NewServiceIdentity(serviceEnvironment, serviceNamespace, serviceName)
	if err != nil {
		return observation.Aggregate{}, fmt.Errorf("restore observation service identity: %w", err)
	}
	destination, err := observation.NewDestinationHint(messagingSystem, destinationPhysical, destinationLogical)
	if err != nil {
		return observation.Aggregate{}, fmt.Errorf("restore observation destination hint: %w", err)
	}
	fact, err := observation.NewFact(observation.FactParams{
		SourceID:         sourceID,
		Scope:            scope,
		ObservedAt:       lastSeen,
		RelationshipKind: topology.EdgeKind(relationshipKindValue),
		Service:          service,
		Destination:      destination,
		Metadata:         metadata,
	})
	if err != nil {
		return observation.Aggregate{}, fmt.Errorf("restore observation fact: %w", err)
	}
	aggregate, err := observation.NewAggregateFromState(fact, firstSeen, lastSeen, uint64(observationCount))
	if err != nil {
		return observation.Aggregate{}, fmt.Errorf("restore observation aggregate: %w", err)
	}
	return aggregate, nil
}

func compareStoredObservationAggregates(left, right observation.Aggregate) int {
	leftKey := left.Key()
	rightKey := right.Key()
	values := [][2]string{
		{leftKey.SourceID().String(), rightKey.SourceID().String()},
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

var _ application.ObservationStore = (*ObservationStore)(nil)
