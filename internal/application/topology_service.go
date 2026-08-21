// Package application coordinates EventAtlas use cases around the topology core.
package application

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/lucacox/eventatlas/internal/discovery"
	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrDiscoveryProviderNil = errors.New("discovery provider cannot be nil")
	ErrTopologyStoreNil     = errors.New("topology store cannot be nil")
	ErrTopologyNotFound     = errors.New("topology snapshot not found")
	ErrSnapshotNil          = errors.New("topology snapshot cannot be nil")
)

// TopologyStore owns the current read model used by the topology API.
//
// The first vertical slice stores one provider snapshot. A durable adapter can
// later implement reconciliation behind the same application-owned boundary.
type TopologyStore interface {
	Replace(ctx context.Context, snapshot *topology.TopologySnapshot) error
	Current(ctx context.Context) (*topology.TopologySnapshot, error)
}

// TopologyService coordinates declared-topology discovery and querying.
type TopologyService struct {
	provider discovery.Provider
	store    TopologyStore
}

func NewTopologyService(provider discovery.Provider, store TopologyStore) (*TopologyService, error) {
	if isNilInterface(provider) {
		return nil, ErrDiscoveryProviderNil
	}
	if isNilInterface(store) {
		return nil, ErrTopologyStoreNil
	}
	return &TopologyService{provider: provider, store: store}, nil
}

// Refresh discovers one scope and atomically replaces the current read model.
func (service *TopologyService) Refresh(ctx context.Context, scope topology.DiscoveryScope) (*topology.TopologySnapshot, error) {
	incoming, err := service.provider.Discover(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("discover topology: %w", err)
	}
	if incoming == nil {
		return nil, ErrSnapshotNil
	}

	snapshot := incoming
	current, err := service.store.Current(ctx)
	if err != nil && !errors.Is(err, ErrTopologyNotFound) {
		return nil, fmt.Errorf("load current topology for reconciliation: %w", err)
	}
	if current != nil {
		snapshot, err = reconcileSnapshots(current, incoming)
		if err != nil {
			return nil, fmt.Errorf("reconcile topology: %w", err)
		}
	}
	if err := service.store.Replace(ctx, snapshot); err != nil {
		return nil, fmt.Errorf("replace topology: %w", err)
	}
	return snapshot, nil
}

// Current returns the latest topology made available by a successful refresh.
func (service *TopologyService) Current(ctx context.Context) (*topology.TopologySnapshot, error) {
	snapshot, err := service.store.Current(ctx)
	if err != nil {
		return nil, fmt.Errorf("load current topology: %w", err)
	}
	if snapshot == nil {
		return nil, ErrSnapshotNil
	}
	return snapshot, nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
