package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrPoolNil                = errors.New("PostgreSQL pool cannot be nil")
	ErrSourceIDInvalid        = errors.New("PostgreSQL topology store source ID is invalid")
	ErrScopeInvalid           = errors.New("PostgreSQL topology store scope is invalid")
	ErrSnapshotSourceMismatch = errors.New("snapshot source does not match PostgreSQL topology store")
	ErrSnapshotScopeMismatch  = errors.New("snapshot scope does not match PostgreSQL topology store")
)

// TopologyStore persists one current topology for a configured source and scope.
type TopologyStore struct {
	pool     *pgxpool.Pool
	sourceID topology.SourceID
	scope    topology.DiscoveryScope
}

func NewTopologyStore(pool *pgxpool.Pool, sourceID topology.SourceID, scope topology.DiscoveryScope) (*TopologyStore, error) {
	if pool == nil {
		return nil, ErrPoolNil
	}
	if sourceID.String() == "" {
		return nil, ErrSourceIDInvalid
	}
	if scope.String() == "" {
		return nil, ErrScopeInvalid
	}
	return &TopologyStore{pool: pool, sourceID: sourceID, scope: scope}, nil
}

func (store *TopologyStore) Replace(ctx context.Context, snapshot *topology.TopologySnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot == nil {
		return application.ErrSnapshotNil
	}
	if snapshot.SourceID() != store.sourceID {
		return ErrSnapshotSourceMismatch
	}
	if snapshot.Scope() != store.scope {
		return ErrSnapshotScopeMismatch
	}

	encodedPartialErrors, err := json.Marshal(snapshot.PartialErrors())
	if err != nil {
		return fmt.Errorf("encode snapshot partial errors: %w", err)
	}
	encodedMetadata, err := json.Marshal(snapshot.Metadata())
	if err != nil {
		return fmt.Errorf("encode snapshot metadata: %w", err)
	}

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin topology replacement: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		"DELETE FROM topology_snapshots WHERE source_id = $1 AND discovery_scope = $2",
		store.sourceID.String(), store.scope.String(),
	); err != nil {
		return fmt.Errorf("remove previous topology snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO topology_snapshots (
			source_id, discovery_scope, snapshot_id, captured_at, completeness,
			partial_errors, cursor, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		store.sourceID.String(),
		store.scope.String(),
		snapshot.ID().String(),
		snapshot.CapturedAt(),
		string(snapshot.Completeness()),
		encodedPartialErrors,
		snapshot.Cursor(),
		encodedMetadata,
	); err != nil {
		return fmt.Errorf("insert topology snapshot: %w", err)
	}
	if err := insertNodes(ctx, tx, store.sourceID, store.scope, snapshot.Nodes()); err != nil {
		return err
	}
	if err := insertEdges(ctx, tx, store.sourceID, store.scope, snapshot.Edges()); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit topology replacement: %w", err)
	}
	return nil
}

func (store *TopologyStore) Current(ctx context.Context) (*topology.TopologySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin topology read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	snapshot, err := loadSnapshot(ctx, tx, store.sourceID, store.scope)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, application.ErrTopologyNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit topology read: %w", err)
	}
	return snapshot, nil
}

var _ application.TopologyStore = (*TopologyStore)(nil)
