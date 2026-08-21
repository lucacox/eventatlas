package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/storage/memory"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestLoadInitialTopologyPrefersSuccessfulDiscovery(t *testing.T) {
	t.Parallel()

	scope, _ := topology.NewDiscoveryScope("account:test")
	persisted := startupTestSnapshot(t, "snapshot:persisted", scope)
	discovered := startupTestSnapshot(t, "snapshot:discovered", scope)
	store := memory.NewTopologyStore()
	if err := store.Replace(context.Background(), persisted); err != nil {
		t.Fatalf("Replace(persisted) error = %v", err)
	}
	service, err := application.NewTopologyService(&startupProvider{snapshot: discovered}, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}

	got, err := loadInitialTopology(context.Background(), service, scope, startupTestConfig())
	if err != nil {
		t.Fatalf("loadInitialTopology() error = %v", err)
	}
	if got.ID() != discovered.ID() {
		t.Errorf("initial snapshot = %q, want discovered %q", got.ID(), discovered.ID())
	}
}

func TestLoadInitialTopologyFallsBackToPersistedSnapshot(t *testing.T) {
	t.Parallel()

	scope, _ := topology.NewDiscoveryScope("account:test")
	persisted := startupTestSnapshot(t, "snapshot:persisted", scope)
	store := memory.NewTopologyStore()
	if err := store.Replace(context.Background(), persisted); err != nil {
		t.Fatalf("Replace(persisted) error = %v", err)
	}
	wantErr := errors.New("NATS unavailable")
	service, err := application.NewTopologyService(&startupProvider{err: wantErr}, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}

	got, err := loadInitialTopology(context.Background(), service, scope, startupTestConfig())
	if err != nil {
		t.Fatalf("loadInitialTopology() error = %v", err)
	}
	if got.ID() != persisted.ID() {
		t.Errorf("fallback snapshot = %q, want persisted %q", got.ID(), persisted.ID())
	}
}

func TestLoadInitialTopologyFailsWithoutAnySnapshot(t *testing.T) {
	t.Parallel()

	scope, _ := topology.NewDiscoveryScope("account:test")
	wantErr := errors.New("NATS unavailable")
	service, err := application.NewTopologyService(&startupProvider{err: wantErr}, memory.NewTopologyStore())
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}

	if snapshot, err := loadInitialTopology(context.Background(), service, scope, startupTestConfig()); snapshot != nil || !errors.Is(err, wantErr) {
		t.Errorf("loadInitialTopology() = (%v, %v), want nil and wrapped provider error", snapshot, err)
	}
}

type startupProvider struct {
	snapshot *topology.TopologySnapshot
	err      error
}

func (provider *startupProvider) Discover(context.Context, topology.DiscoveryScope) (*topology.TopologySnapshot, error) {
	return provider.snapshot, provider.err
}

func startupTestSnapshot(t *testing.T, idValue string, scope topology.DiscoveryScope) *topology.TopologySnapshot {
	t.Helper()
	id, _ := topology.NewSnapshotID(idValue)
	sourceID, _ := topology.NewSourceID("provider:nats:test")
	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:           id,
		SourceID:     sourceID,
		Scope:        scope,
		CapturedAt:   time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC),
		Completeness: topology.SnapshotCompletenessFull,
	})
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}
	return snapshot
}

func startupTestConfig() config {
	return config{databaseTimeout: time.Second, discoveryTimeout: time.Second}
}
