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
	observationotel "github.com/lucacox/eventatlas/internal/observation/otel"
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
	sourceID, err := topology.NewSourceID(config.sourceID)
	if err != nil {
		return fmt.Errorf("configure NATS source ID: %w", err)
	}
	scope, err := topology.NewDiscoveryScope(config.discoveryScope)
	if err != nil {
		return fmt.Errorf("configure discovery scope: %w", err)
	}
	topologyStore, observationStore, closeStores, err := configureStores(ctx, config, sourceID, scope)
	if err != nil {
		return err
	}
	defer closeStores()
	otlpServer, err := configureOTLPServer(config, scope, observationStore)
	if err != nil {
		return err
	}

	connection, err := natsgo.Connect(
		config.natsURL,
		natsgo.Name("eventatlas"),
		natsgo.Timeout(config.discoveryTimeout),
		natsgo.RetryOnFailedConnect(true),
		natsgo.MaxReconnects(-1),
		natsgo.ReconnectWait(2*time.Second),
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
			if err != nil {
				log.Printf("NATS disconnected: %v", err)
			}
		}),
		natsgo.ReconnectHandler(func(connection *natsgo.Conn) {
			log.Printf("NATS reconnected to %s", connection.ConnectedUrl())
		}),
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
	provider, err := natsdiscovery.NewProvider(client, natsdiscovery.Config{
		SourceID:    sourceID,
		BrokerName:  config.brokerName,
		Environment: config.environment,
	})
	if err != nil {
		return fmt.Errorf("configure NATS provider: %w", err)
	}
	service, err := application.NewTopologyService(provider, topologyStore)
	if err != nil {
		return fmt.Errorf("create topology service: %w", err)
	}
	viewService, err := configureTopologyViewService(service, observationStore, config.observationRetention)
	if err != nil {
		return err
	}
	snapshot, err := loadInitialTopology(ctx, service, scope, config)
	if err != nil {
		return err
	}
	log.Printf("current topology snapshot %s with %d nodes and %d edges", snapshot.ID(), len(snapshot.Nodes()), len(snapshot.Edges()))
	periodicRefresher, err := application.NewPeriodicRefresher(service, scope, application.PeriodicRefreshConfig{
		Interval: config.refreshInterval,
		Timeout:  config.discoveryTimeout,
	})
	if err != nil {
		return fmt.Errorf("create periodic topology refresher: %w", err)
	}

	handler, err := api.NewHandler(viewService)
	if err != nil {
		return fmt.Errorf("create HTTP API: %w", err)
	}
	apiServer := &http.Server{
		Addr:              config.httpAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

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
	servers := []namedHTTPServer{{name: "EventAtlas API", server: apiServer}}
	log.Printf("EventAtlas API configured on %s (API docs: /docs, refresh interval: %s)", config.httpAddress, config.refreshInterval)
	if otlpServer != nil {
		servers = append(servers, namedHTTPServer{name: "EventAtlas OTLP/HTTP receiver", server: otlpServer})
		log.Printf("EventAtlas OTLP/HTTP receiver configured on %s%s", config.otlpHTTPAddress, observationotel.TraceExportPath)
	} else {
		log.Printf("EventAtlas OTLP/HTTP receiver disabled")
	}

	serverErr := runHTTPServers(ctx, stop, servers, config.shutdownTimeout)
	<-refreshDone
	return serverErr
}

func loadInitialTopology(
	ctx context.Context,
	service *application.TopologyService,
	scope topology.DiscoveryScope,
	config config,
) (*topology.TopologySnapshot, error) {
	currentContext, cancelCurrent := context.WithTimeout(ctx, config.databaseTimeout)
	persisted, currentErr := service.Current(currentContext)
	cancelCurrent()
	if currentErr != nil && !errors.Is(currentErr, application.ErrTopologyNotFound) {
		return nil, fmt.Errorf("load persisted topology: %w", currentErr)
	}
	if persisted != nil {
		log.Printf("loaded persisted topology snapshot %s", persisted.ID())
	}

	discoveryContext, cancelDiscovery := context.WithTimeout(ctx, config.discoveryTimeout)
	discovered, discoveryErr := service.Refresh(discoveryContext, scope)
	cancelDiscovery()
	if discoveryErr == nil {
		return discovered, nil
	}
	if persisted != nil {
		log.Printf("initial topology discovery failed; serving persisted snapshot: %v", discoveryErr)
		return persisted, nil
	}
	return nil, fmt.Errorf("initial topology discovery: %w", discoveryErr)
}
