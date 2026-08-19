// Package memory contains volatile storage adapters for local and test use.
package memory

import (
	"context"
	"sync"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/topology"
)

// TopologyStore keeps the latest immutable snapshot in memory.
type TopologyStore struct {
	mu       sync.RWMutex
	snapshot *topology.TopologySnapshot
}

func NewTopologyStore() *TopologyStore {
	return &TopologyStore{}
}

func (store *TopologyStore) Replace(ctx context.Context, snapshot *topology.TopologySnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot == nil {
		return application.ErrSnapshotNil
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	store.snapshot = snapshot
	return nil
}

func (store *TopologyStore) Current(ctx context.Context) (*topology.TopologySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.snapshot == nil {
		return nil, application.ErrTopologyNotFound
	}
	return store.snapshot, nil
}

var _ application.TopologyStore = (*TopologyStore)(nil)
