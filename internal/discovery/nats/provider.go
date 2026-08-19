// Package nats discovers and normalizes NATS and JetStream declared topology.
package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lucacox/eventatlas/internal/discovery"
	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrClientNil           = errors.New("NATS discovery client cannot be nil")
	ErrSourceIDInvalid     = errors.New("NATS provider source ID is invalid")
	ErrBrokerNameEmpty     = errors.New("NATS broker name cannot be blank")
	ErrEnvironmentEmpty    = errors.New("NATS environment cannot be blank")
	ErrDiscoveryScopeEmpty = errors.New("NATS discovery scope cannot be empty")
)

type Config struct {
	SourceID    topology.SourceID
	BrokerName  string
	Environment string
}

// Provider discovers declared JetStream topology and produces full snapshots.
type Provider struct {
	client        Client
	sourceID      topology.SourceID
	brokerName    string
	environment   string
	sourceSystem  topology.SourceSystem
	now           func() time.Time
	newSnapshotID func() (topology.SnapshotID, error)
}

func NewProvider(client Client, config Config) (*Provider, error) {
	if isNilInterface(client) {
		return nil, ErrClientNil
	}
	if config.SourceID.String() == "" {
		return nil, ErrSourceIDInvalid
	}
	if strings.TrimSpace(config.BrokerName) == "" {
		return nil, ErrBrokerNameEmpty
	}
	if strings.TrimSpace(config.Environment) == "" {
		return nil, ErrEnvironmentEmpty
	}
	sourceSystem, err := topology.NewSourceSystem("nats")
	if err != nil {
		return nil, fmt.Errorf("create NATS source system: %w", err)
	}
	return &Provider{
		client:        client,
		sourceID:      config.SourceID,
		brokerName:    strings.TrimSpace(config.BrokerName),
		environment:   strings.TrimSpace(config.Environment),
		sourceSystem:  sourceSystem,
		now:           time.Now,
		newSnapshotID: generateSnapshotID,
	}, nil
}

func (provider *Provider) Discover(ctx context.Context, scope topology.DiscoveryScope) (*topology.TopologySnapshot, error) {
	if scope.String() == "" {
		return nil, ErrDiscoveryScopeEmpty
	}
	streams, err := provider.client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover NATS streams: %w", err)
	}
	return provider.normalize(scope, provider.now(), streams)
}

var _ discovery.Provider = (*Provider)(nil)
