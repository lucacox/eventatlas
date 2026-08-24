package main

import (
	"fmt"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
)

func configureTopologyViewService(
	declared application.DeclaredTopologyReader,
	observations application.ObservationStore,
	retention time.Duration,
) (*application.TopologyViewService, error) {
	projector, err := application.NewTopologyViewProjector(
		observations,
		application.TopologyViewProjectorConfig{Retention: retention},
	)
	if err != nil {
		return nil, fmt.Errorf("configure topology view projector: %w", err)
	}
	service, err := application.NewTopologyViewService(declared, projector)
	if err != nil {
		return nil, fmt.Errorf("configure topology view service: %w", err)
	}
	return service, nil
}
