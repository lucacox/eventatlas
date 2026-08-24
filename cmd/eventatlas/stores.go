package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/storage/memory"
	postgresstore "github.com/lucacox/eventatlas/internal/storage/postgres"
	"github.com/lucacox/eventatlas/internal/topology"
)

func configureStores(
	ctx context.Context,
	config config,
	sourceID topology.SourceID,
	scope topology.DiscoveryScope,
) (application.TopologyStore, application.ObservationStore, func(), error) {
	if config.databaseURL == "" {
		return memory.NewTopologyStore(), memory.NewObservationStore(), func() {}, nil
	}

	databaseContext, cancel := context.WithTimeout(ctx, config.databaseTimeout)
	defer cancel()
	pool, err := pgxpool.New(databaseContext, config.databaseURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("configure PostgreSQL: %w", err)
	}
	closePool := func() { pool.Close() }
	if err := pool.Ping(databaseContext); err != nil {
		closePool()
		return nil, nil, nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	if err := postgresstore.Migrate(databaseContext, pool); err != nil {
		closePool()
		return nil, nil, nil, fmt.Errorf("migrate PostgreSQL: %w", err)
	}

	topologyStore, err := postgresstore.NewTopologyStore(pool, sourceID, scope)
	if err != nil {
		closePool()
		return nil, nil, nil, fmt.Errorf("configure PostgreSQL topology store: %w", err)
	}
	observationStore, err := postgresstore.NewObservationStore(pool)
	if err != nil {
		closePool()
		return nil, nil, nil, fmt.Errorf("configure PostgreSQL observation store: %w", err)
	}
	return topologyStore, observationStore, closePool, nil
}
