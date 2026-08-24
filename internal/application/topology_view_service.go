package application

import (
	"context"
	"errors"
	"sync"

	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrDeclaredTopologyReaderNil = errors.New("declared topology reader cannot be nil")
	ErrTopologyViewProjectorNil  = errors.New("topology view projector cannot be nil")
)

// DeclaredTopologyReader exposes the current authoritative provider snapshot.
// It is intentionally narrower than TopologyService so the merged read path
// does not depend on discovery or refresh operations.
type DeclaredTopologyReader interface {
	Current(ctx context.Context) (*topology.TopologySnapshot, error)
}

type topologyViewProjector interface {
	Project(ctx context.Context, declared *topology.TopologySnapshot) (*topology.TopologyView, error)
}

// TopologyViewService builds the public, multi-source read model from the
// current declared snapshot and active observations. The last successful view
// remains readable if a later store read or projection fails.
type TopologyViewService struct {
	declared  DeclaredTopologyReader
	projector topologyViewProjector

	mu          sync.RWMutex
	lastSuccess *topology.TopologyView
}

func NewTopologyViewService(
	declared DeclaredTopologyReader,
	projector topologyViewProjector,
) (*TopologyViewService, error) {
	if isNilInterface(declared) {
		return nil, ErrDeclaredTopologyReaderNil
	}
	if isNilInterface(projector) {
		return nil, ErrTopologyViewProjectorNil
	}
	return &TopologyViewService{declared: declared, projector: projector}, nil
}

// Current projects the latest declared state on demand so newly ingested
// observations are visible without waiting for the provider refresh interval.
func (service *TopologyViewService) Current(ctx context.Context) (*topology.TopologyView, error) {
	declared, err := service.declared.Current(ctx)
	if err != nil {
		return service.lastSuccessfulOr(err)
	}
	view, err := service.projector.Project(ctx, declared)
	if err != nil {
		return service.lastSuccessfulOr(err)
	}

	service.mu.Lock()
	if service.lastSuccess == nil || !view.GeneratedAt().Before(service.lastSuccess.GeneratedAt()) {
		service.lastSuccess = view
	}
	service.mu.Unlock()
	return view, nil
}

func (service *TopologyViewService) lastSuccessfulOr(err error) (*topology.TopologyView, error) {
	service.mu.RLock()
	lastSuccess := service.lastSuccess
	service.mu.RUnlock()
	if lastSuccess != nil {
		return lastSuccess, nil
	}
	return nil, err
}
