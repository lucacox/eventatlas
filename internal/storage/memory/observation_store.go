package memory

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
)

var _ application.ObservationStore = (*ObservationStoreAdapter)(nil)

// ObservationStoreAdapter is a concurrency-safe, process-local observation adapter.
type ObservationStoreAdapter struct {
	mu         sync.RWMutex
	aggregates map[observation.Key]observation.Aggregate
}

func NewObservationStore() *ObservationStoreAdapter {
	return &ObservationStoreAdapter{aggregates: make(map[observation.Key]observation.Aggregate)}
}

func (store *ObservationStoreAdapter) Upsert(ctx context.Context, fact observation.Fact) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	incoming, err := observation.NewAggregate(fact)
	if err != nil {
		return err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.aggregates == nil {
		store.aggregates = make(map[observation.Key]observation.Aggregate)
	}
	if current, exists := store.aggregates[fact.Key()]; exists {
		incoming, err = current.Add(fact)
		if err != nil {
			return err
		}
	}
	store.aggregates[fact.Key()] = incoming
	return nil
}

func (store *ObservationStoreAdapter) ListActive(
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

	store.mu.RLock()
	defer store.mu.RUnlock()
	active := make([]observation.Aggregate, 0)
	for _, aggregate := range store.aggregates {
		if aggregate.Key().Scope() != scope || aggregate.LastSeen().Before(activeSince) {
			continue
		}
		active = append(active, aggregate)
	}
	slices.SortFunc(active, compareObservationAggregates)
	return active, nil
}

func compareObservationAggregates(left, right observation.Aggregate) int {
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
