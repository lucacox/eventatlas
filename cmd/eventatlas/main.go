package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/lucacox/eventatlas/internal/api"
	"github.com/lucacox/eventatlas/internal/application"
	natsdiscovery "github.com/lucacox/eventatlas/internal/discovery/nats"
	"github.com/lucacox/eventatlas/internal/storage/memory"
	"github.com/lucacox/eventatlas/internal/topology"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	config, err := loadConfig(os.LookupEnv)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connection, err := natsgo.Connect(
		config.natsURL,
		natsgo.Name("eventatlas"),
		natsgo.Timeout(config.discoveryTimeout),
	)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer connection.Close()

	manager, err := jetstream.New(connection)
	if err != nil {
		return fmt.Errorf("create JetStream manager: %w", err)
	}
	client, err := natsdiscovery.NewJetStreamClient(manager)
	if err != nil {
		return fmt.Errorf("create NATS discovery client: %w", err)
	}
	sourceID, err := topology.NewSourceID(config.sourceID)
	if err != nil {
		return fmt.Errorf("configure NATS source ID: %w", err)
	}
	provider, err := natsdiscovery.NewProvider(client, natsdiscovery.Config{
		SourceID:    sourceID,
		BrokerName:  config.brokerName,
		Environment: config.environment,
	})
	if err != nil {
		return fmt.Errorf("configure NATS provider: %w", err)
	}
	scope, err := topology.NewDiscoveryScope(config.discoveryScope)
	if err != nil {
		return fmt.Errorf("configure discovery scope: %w", err)
	}

	store := memory.NewTopologyStore()
	service, err := application.NewTopologyService(provider, store)
	if err != nil {
		return fmt.Errorf("create topology service: %w", err)
	}
	discoveryContext, cancelDiscovery := context.WithTimeout(ctx, config.discoveryTimeout)
	snapshot, err := service.Refresh(discoveryContext, scope)
	cancelDiscovery()
	if err != nil {
		return fmt.Errorf("initial topology discovery: %w", err)
	}
	log.Printf("discovered topology snapshot %s with %d nodes and %d edges", snapshot.ID(), len(snapshot.Nodes()), len(snapshot.Edges()))
	periodicRefresher, err := application.NewPeriodicRefresher(service, scope, application.PeriodicRefreshConfig{
		Interval: config.refreshInterval,
		Timeout:  config.discoveryTimeout,
	})
	if err != nil {
		return fmt.Errorf("create periodic topology refresher: %w", err)
	}

	handler, err := api.NewHandler(service)
	if err != nil {
		return fmt.Errorf("create HTTP API: %w", err)
	}
	server := &http.Server{
		Addr:              config.httpAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		periodicRefresher.Run(ctx, func(outcome application.RefreshOutcome) {
			if outcome.Err != nil {
				log.Printf("periodic topology refresh failed: %v", outcome.Err)
				return
			}
			if outcome.Snapshot == nil {
				log.Printf("periodic topology refresh returned no snapshot")
				return
			}
			log.Printf(
				"refreshed topology snapshot %s with %d nodes and %d edges",
				outcome.Snapshot.ID(),
				len(outcome.Snapshot.Nodes()),
				len(outcome.Snapshot.Edges()),
			)
		})
	}()
	go func() {
		log.Printf("EventAtlas API listening on %s (API docs: /docs, refresh interval: %s)", config.httpAddress, config.refreshInterval)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		stop()
		<-refreshDone
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP API: %w", err)
	case <-ctx.Done():
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), config.shutdownTimeout)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdownContext)
	serverErr := <-serverErrors
	<-refreshDone
	if shutdownErr != nil {
		return fmt.Errorf("shut down HTTP API: %w", shutdownErr)
	}
	if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP API: %w", serverErr)
	}
	return nil
}
