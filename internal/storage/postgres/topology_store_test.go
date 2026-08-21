package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestNewTopologyStoreValidatesConfiguration(t *testing.T) {
	t.Parallel()

	sourceID, _ := topology.NewSourceID("provider:nats:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	if _, err := NewTopologyStore(nil, sourceID, scope); !errors.Is(err, ErrPoolNil) {
		t.Errorf("NewTopologyStore(nil) error = %v, want %v", err, ErrPoolNil)
	}
	pool := new(pgxpool.Pool)
	if _, err := NewTopologyStore(pool, topology.SourceID{}, scope); !errors.Is(err, ErrSourceIDInvalid) {
		t.Errorf("NewTopologyStore(invalid source) error = %v, want %v", err, ErrSourceIDInvalid)
	}
	if _, err := NewTopologyStore(pool, sourceID, topology.DiscoveryScope{}); !errors.Is(err, ErrScopeInvalid) {
		t.Errorf("NewTopologyStore(invalid scope) error = %v, want %v", err, ErrScopeInvalid)
	}
}
