package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestTopologyStoreReplacesAndReturnsCurrentSnapshot(t *testing.T) {
	t.Parallel()

	store := NewTopologyStore()
	first := memoryTestSnapshot(t, "snapshot:first")
	second := memoryTestSnapshot(t, "snapshot:second")
	if err := store.Replace(context.Background(), first); err != nil {
		t.Fatalf("Replace(first) error = %v", err)
	}
	if err := store.Replace(context.Background(), second); err != nil {
		t.Fatalf("Replace(second) error = %v", err)
	}
	got, err := store.Current(context.Background())
	if err != nil {
		t.Fatalf("Current() error = %v", err)
	}
	if got != second {
		t.Errorf("Current() = %p, want latest snapshot %p", got, second)
	}
}

func TestTopologyStoreReportsMissingAndInvalidSnapshots(t *testing.T) {
	t.Parallel()

	store := NewTopologyStore()
	if _, err := store.Current(context.Background()); !errors.Is(err, application.ErrTopologyNotFound) {
		t.Errorf("Current() error = %v, want %v", err, application.ErrTopologyNotFound)
	}
	if err := store.Replace(context.Background(), nil); !errors.Is(err, application.ErrSnapshotNil) {
		t.Errorf("Replace(nil) error = %v, want %v", err, application.ErrSnapshotNil)
	}
}

func TestTopologyStoreHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := NewTopologyStore()
	if err := store.Replace(ctx, memoryTestSnapshot(t, "snapshot:first")); !errors.Is(err, context.Canceled) {
		t.Errorf("Replace() error = %v, want context canceled", err)
	}
	if _, err := store.Current(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Current() error = %v, want context canceled", err)
	}
}

func memoryTestSnapshot(t *testing.T, idValue string) *topology.TopologySnapshot {
	t.Helper()
	id, _ := topology.NewSnapshotID(idValue)
	sourceID, _ := topology.NewSourceID("provider:nats:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:           id,
		SourceID:     sourceID,
		Scope:        scope,
		CapturedAt:   time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC),
		Completeness: topology.SnapshotCompletenessFull,
	})
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}
	return snapshot
}
