//go:build integration

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/application"
	postgresstore "github.com/lucacox/eventatlas/internal/storage/postgres"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestInitialTopologyFallsBackToPostgreSQLWhenDiscoveryFails(t *testing.T) {
	databaseURL := os.Getenv("EVENTATLAS_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("EVENTATLAS_DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("PostgreSQL ping error = %v", err)
	}
	if err := postgresstore.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	sourceID, _ := topology.NewSourceID("provider:nats:test")
	scope, _ := topology.NewDiscoveryScope("account:startup:" + startupIntegrationToken(t))
	store, err := postgresstore.NewTopologyStore(pool, sourceID, scope)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	persisted := startupTestSnapshot(t, "snapshot:persisted", scope)
	if err := store.Replace(ctx, persisted); err != nil {
		t.Fatalf("Replace(persisted) error = %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupContext,
			"DELETE FROM topology_snapshots WHERE source_id = $1 AND discovery_scope = $2",
			sourceID.String(), scope.String(),
		)
	})

	wantErr := errors.New("NATS unavailable")
	service, err := application.NewTopologyService(&startupProvider{err: wantErr}, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	got, err := loadInitialTopology(ctx, service, scope, startupTestConfig())
	if err != nil {
		t.Fatalf("loadInitialTopology() error = %v", err)
	}
	if got.ID() != persisted.ID() {
		t.Errorf("fallback snapshot = %q, want persisted %q", got.ID(), persisted.ID())
	}
}

func startupIntegrationToken(t *testing.T) string {
	t.Helper()
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate integration token: %v", err)
	}
	return hex.EncodeToString(value[:])
}
