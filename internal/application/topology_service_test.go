package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestTopologyServiceRefreshStoresDiscoveredSnapshot(t *testing.T) {
	t.Parallel()

	snapshot := applicationTestSnapshot(t, "snapshot:one")
	provider := &applicationFakeProvider{snapshot: snapshot}
	store := &applicationFakeStore{}
	service, err := NewTopologyService(provider, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	scope, err := topology.NewDiscoveryScope("account:test")
	if err != nil {
		t.Fatalf("NewDiscoveryScope() error = %v", err)
	}

	got, err := service.Refresh(context.Background(), scope)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if got != snapshot || store.snapshot != snapshot {
		t.Errorf("Refresh() result/store = (%p, %p), want %p", got, store.snapshot, snapshot)
	}
	if provider.scope != scope {
		t.Errorf("Discover() scope = %q, want %q", provider.scope, scope)
	}
}

func TestTopologyServiceRefreshDoesNotReplaceOnDiscoveryFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("broker unavailable")
	provider := &applicationFakeProvider{err: wantErr}
	store := &applicationFakeStore{snapshot: applicationTestSnapshot(t, "snapshot:existing")}
	service, err := NewTopologyService(provider, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	scope, _ := topology.NewDiscoveryScope("account:test")

	if snapshot, err := service.Refresh(context.Background(), scope); !errors.Is(err, wantErr) || snapshot != nil {
		t.Fatalf("Refresh() = (%v, %v), want nil and wrapped discovery error", snapshot, err)
	}
	if store.replaceCalls != 0 {
		t.Errorf("store Replace() calls = %d, want 0", store.replaceCalls)
	}
}

func TestTopologyServicePropagatesStoreFailures(t *testing.T) {
	t.Parallel()

	replaceErr := errors.New("replace failed")
	currentErr := errors.New("load failed")
	provider := &applicationFakeProvider{snapshot: applicationTestSnapshot(t, "snapshot:one")}
	store := &applicationFakeStore{replaceErr: replaceErr, currentErr: currentErr}
	service, err := NewTopologyService(provider, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	scope, _ := topology.NewDiscoveryScope("account:test")

	if _, err := service.Refresh(context.Background(), scope); !errors.Is(err, replaceErr) {
		t.Errorf("Refresh() error = %v, want wrapped %v", err, replaceErr)
	}
	if _, err := service.Current(context.Background()); !errors.Is(err, currentErr) {
		t.Errorf("Current() error = %v, want wrapped %v", err, currentErr)
	}
}

func TestTopologyServiceRejectsNilDependenciesAndSnapshots(t *testing.T) {
	t.Parallel()

	validProvider := &applicationFakeProvider{snapshot: applicationTestSnapshot(t, "snapshot:one")}
	validStore := &applicationFakeStore{}
	var nilProvider *applicationFakeProvider
	var nilStore *applicationFakeStore

	tests := []struct {
		name     string
		provider *applicationFakeProvider
		store    *applicationFakeStore
		wantErr  error
	}{
		{name: "nil provider", store: validStore, wantErr: ErrDiscoveryProviderNil},
		{name: "typed nil provider", provider: nilProvider, store: validStore, wantErr: ErrDiscoveryProviderNil},
		{name: "nil store", provider: validProvider, wantErr: ErrTopologyStoreNil},
		{name: "typed nil store", provider: validProvider, store: nilStore, wantErr: ErrTopologyStoreNil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewTopologyService(test.provider, test.store); !errors.Is(err, test.wantErr) {
				t.Errorf("NewTopologyService() error = %v, want %v", err, test.wantErr)
			}
		})
	}

	service, err := NewTopologyService(&applicationFakeProvider{}, validStore)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	scope, _ := topology.NewDiscoveryScope("account:test")
	if _, err := service.Refresh(context.Background(), scope); !errors.Is(err, ErrSnapshotNil) {
		t.Errorf("Refresh() nil snapshot error = %v, want %v", err, ErrSnapshotNil)
	}
	if _, err := service.Current(context.Background()); !errors.Is(err, ErrSnapshotNil) {
		t.Errorf("Current() nil snapshot error = %v, want %v", err, ErrSnapshotNil)
	}
}

type applicationFakeProvider struct {
	snapshot *topology.TopologySnapshot
	err      error
	scope    topology.DiscoveryScope
}

func (provider *applicationFakeProvider) Discover(_ context.Context, scope topology.DiscoveryScope) (*topology.TopologySnapshot, error) {
	provider.scope = scope
	return provider.snapshot, provider.err
}

type applicationFakeStore struct {
	snapshot     *topology.TopologySnapshot
	replaceErr   error
	currentErr   error
	replaceCalls int
}

func (store *applicationFakeStore) Replace(_ context.Context, snapshot *topology.TopologySnapshot) error {
	store.replaceCalls++
	if store.replaceErr == nil {
		store.snapshot = snapshot
	}
	return store.replaceErr
}

func (store *applicationFakeStore) Current(context.Context) (*topology.TopologySnapshot, error) {
	return store.snapshot, store.currentErr
}

func applicationTestSnapshot(t *testing.T, idValue string) *topology.TopologySnapshot {
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
