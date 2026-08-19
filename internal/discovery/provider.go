// Package discovery owns application contracts for declared topology discovery.
package discovery

import (
	"context"

	"github.com/lucacox/eventatlas/internal/topology"
)

// Provider discovers broker-declared topology for one explicit scope.
//
// Implementations normalize provider-native entities into the topology core.
// Partial but trustworthy results are represented by a partial snapshot; an
// error means that no snapshot should be reconciled.
type Provider interface {
	Discover(ctx context.Context, scope topology.DiscoveryScope) (*topology.TopologySnapshot, error)
}
