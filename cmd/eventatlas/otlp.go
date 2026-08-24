package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
	observationotel "github.com/lucacox/eventatlas/internal/observation/otel"
	"github.com/lucacox/eventatlas/internal/topology"
)

func configureOTLPServer(
	config config,
	scope topology.DiscoveryScope,
	store application.ObservationStore,
) (*http.Server, error) {
	if config.otlpHTTPAddress == "" {
		return nil, nil
	}

	sourceID, err := topology.NewSourceID(config.otlpSourceID)
	if err != nil {
		return nil, fmt.Errorf("configure OTLP source ID: %w", err)
	}
	normalizer, err := observationotel.NewNormalizer(observationotel.Config{
		SourceID:                  sourceID,
		Scope:                     scope,
		Environment:               config.environment,
		SupportedMessagingSystems: []string{"nats"},
		MaxFutureSkew:             config.otlpMaxFutureSkew,
		MaxAttributeValueLength:   defaultOTLPMaxAttributeValueLength,
	})
	if err != nil {
		return nil, fmt.Errorf("configure OTLP normalizer: %w", err)
	}
	observationService, err := application.NewObservationService(store)
	if err != nil {
		return nil, fmt.Errorf("create observation service: %w", err)
	}
	handler, err := observationotel.NewHTTPHandler(
		normalizer,
		observationService,
		observationotel.HTTPReceiverConfig{MaxRequestBytes: config.otlpMaxRequestBytes},
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP HTTP receiver: %w", err)
	}
	return &http.Server{
		Addr:              config.otlpHTTPAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}, nil
}
