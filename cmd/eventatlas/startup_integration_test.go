//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/lucacox/eventatlas/internal/api"
	"github.com/lucacox/eventatlas/internal/application"
	natsdiscovery "github.com/lucacox/eventatlas/internal/discovery/nats"
	observationotel "github.com/lucacox/eventatlas/internal/observation/otel"
	postgresstore "github.com/lucacox/eventatlas/internal/storage/postgres"
	"github.com/lucacox/eventatlas/internal/topology"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPToTopologyPipelinePersistsAcrossRuntimeReconstruction(t *testing.T) {
	natsURL := os.Getenv("EVENTATLAS_NATS_URL")
	databaseURL := os.Getenv("EVENTATLAS_DATABASE_URL")
	if natsURL == "" || databaseURL == "" {
		t.Skip("EVENTATLAS_NATS_URL and EVENTATLAS_DATABASE_URL are required for end-to-end integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	connection := connectStartupIntegrationNATS(t, natsURL)
	t.Cleanup(connection.Close)
	manager, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	token := startupIntegrationToken(t)
	scope, err := topology.NewDiscoveryScope("account:e2e:" + token)
	if err != nil {
		t.Fatalf("NewDiscoveryScope() error = %v", err)
	}
	sourceID, err := topology.NewSourceID("provider:nats:e2e:" + token)
	if err != nil {
		t.Fatalf("NewSourceID() error = %v", err)
	}
	streamName := "EA_E2E_" + token
	subject := "eventatlas.e2e." + token + ".orders.*"
	physicalSubject := "eventatlas.e2e." + token + ".orders.created"
	setupContext, cancelSetup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelSetup()
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := manager.DeleteStream(cleanupContext, streamName); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("DeleteStream() cleanup error = %v", err)
		}
	})

	cleanupPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New(cleanup) error = %v", err)
	}
	t.Cleanup(cleanupPool.Close)
	if err := cleanupPool.Ping(ctx); err != nil {
		t.Fatalf("PostgreSQL ping error = %v", err)
	}
	if err := postgresstore.Migrate(ctx, cleanupPool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cleanupPool.Exec(cleanupContext, "DELETE FROM topology_observations WHERE discovery_scope = $1", scope.String())
		_, _ = cleanupPool.Exec(cleanupContext, "DELETE FROM topology_snapshots WHERE source_id = $1 AND discovery_scope = $2", sourceID.String(), scope.String())
	})

	runtime := newStartupIntegrationRuntime(t, ctx, natsURL, databaseURL, sourceID, scope)
	initialView := getStartupIntegrationTopology(t, runtime.apiHandler)
	if apiTopologyHasNode(initialView, streamName) || apiTopologyHasNode(initialView, subject) {
		t.Fatalf("initial topology already contains the unresolved stream or subject")
	}

	postOTLPTrace(t, runtime.otlpHandler, physicalSubject, time.Now().UTC().Add(-time.Second))
	observedView := getStartupIntegrationTopology(t, runtime.apiHandler)
	if observedView.View.Diagnostics.UnresolvedObservations != 1 {
		t.Fatalf("topology after OTLP unresolved observations = %d, want 1", observedView.View.Diagnostics.UnresolvedObservations)
	}

	if _, err := manager.CreateStream(setupContext, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  jetstream.MemoryStorage,
		Replicas: 1,
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}
	if _, err := runtime.topologyService.Refresh(ctx, scope); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	resolvedView := getStartupIntegrationTopology(t, runtime.apiHandler)
	if !apiTopologyHasNode(resolvedView, streamName) || !apiTopologyHasNode(resolvedView, subject) {
		t.Fatalf("refreshed topology does not contain declared stream and subject")
	}
	if !apiTopologyHasEdge(resolvedView, "checkout", subject, "publishes") {
		t.Fatalf("refreshed topology does not correlate checkout publishing with declared subject")
	}
	publishing := apiTopologyEdge(resolvedView, "checkout", subject, "publishes")
	if publishing == nil || len(publishing.Evidence) != 1 || publishing.Evidence[0].Mode != "observed" || publishing.Evidence[0].SourceSystem != "opentelemetry" {
		t.Fatalf("publishing evidence = %+v, want one observed OpenTelemetry record", publishing)
	}

	runtime.close()
	reopened := newStartupIntegrationRuntime(t, ctx, natsURL, databaseURL, sourceID, scope)
	defer reopened.close()
	persistedView := getStartupIntegrationTopology(t, reopened.apiHandler)
	if !apiTopologyHasEdge(persistedView, "checkout", subject, "publishes") {
		t.Fatalf("reconstructed runtime lost persisted checkout publishing edge")
	}
}

type startupIntegrationRuntime struct {
	topologyService *application.TopologyService
	apiHandler      http.Handler
	otlpHandler     http.Handler
	close           func()
}

func newStartupIntegrationRuntime(t *testing.T, ctx context.Context, natsURL, databaseURL string, sourceID topology.SourceID, scope topology.DiscoveryScope) startupIntegrationRuntime {
	t.Helper()
	config := config{
		natsURL:              natsURL,
		databaseURL:          databaseURL,
		environment:          "development",
		brokerName:           "Integration NATS",
		discoveryTimeout:     10 * time.Second,
		databaseTimeout:      10 * time.Second,
		observationRetention: defaultObservationRetention,
		otlpHTTPAddress:      ":4318",
		otlpSourceID:         "observation:otel:e2e",
		otlpMaxRequestBytes:  defaultOTLPMaxRequestBytes,
		otlpMaxFutureSkew:    defaultOTLPMaxFutureSkew,
	}
	topologyStore, observationStore, closeStores, err := configureStores(ctx, config, sourceID, scope)
	if err != nil {
		t.Fatalf("configureStores() error = %v", err)
	}
	connection := connectStartupIntegrationNATS(t, natsURL)
	manager, err := jetstream.New(connection)
	if err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("jetstream.New() error = %v", err)
	}
	client, err := natsdiscovery.NewJetStreamClient(manager)
	if err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}
	provider, err := natsdiscovery.NewProvider(client, natsdiscovery.Config{SourceID: sourceID, BrokerName: config.brokerName, Environment: config.environment})
	if err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("NewProvider() error = %v", err)
	}
	topologyService, err := application.NewTopologyService(provider, topologyStore)
	if err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	if _, err := loadInitialTopology(ctx, topologyService, scope, config); err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("loadInitialTopology() error = %v", err)
	}
	viewService, err := configureTopologyViewService(topologyService, observationStore, config.observationRetention)
	if err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("configureTopologyViewService() error = %v", err)
	}
	apiHandler, err := api.NewHandler(viewService)
	if err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("NewHandler() error = %v", err)
	}
	otlpServer, err := configureOTLPServer(config, scope, observationStore)
	if err != nil {
		closeStores()
		connection.Close()
		t.Fatalf("configureOTLPServer() error = %v", err)
	}
	return startupIntegrationRuntime{
		topologyService: topologyService,
		apiHandler:      apiHandler,
		otlpHandler:     otlpServer.Handler,
		close: func() {
			closeStores()
			connection.Close()
		},
	}
}

func postOTLPTrace(t *testing.T, handler http.Handler, physicalSubject string, observedAt time.Time) {
	t.Helper()
	trace := mainTestTraceRequest(observedAt)
	for _, attribute := range trace.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes {
		if attribute.Key == "messaging.destination.name" {
			attribute.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: physicalSubject}}
		}
		if attribute.Key == "messaging.destination.template" {
			attribute.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: physicalSubject[:len(physicalSubject)-len("created")] + "*"}}
		}
	}
	payload, err := proto.Marshal(trace)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, observationotel.TraceExportPath, bytes.NewReader(payload))
	request.Header.Set("Content-Type", observationotel.ProtobufContentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("POST %s status = %d, want 200; body = %x", observationotel.TraceExportPath, response.Code, response.Body.Bytes())
	}
}

func getStartupIntegrationTopology(t *testing.T, handler http.Handler) api.TopologyResponse {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/topology status = %d, want 200; body = %s", response.Code, response.Body.String())
	}
	var topologyResponse api.TopologyResponse
	if err := json.Unmarshal(response.Body.Bytes(), &topologyResponse); err != nil {
		t.Fatalf("decode topology response: %v", err)
	}
	return topologyResponse
}

func apiTopologyHasNode(response api.TopologyResponse, name string) bool {
	for _, node := range response.Nodes {
		if node.Name == name {
			return true
		}
	}
	return false
}

func apiTopologyHasEdge(response api.TopologyResponse, sourceName, targetName, kind string) bool {
	return apiTopologyEdge(response, sourceName, targetName, kind) != nil
}

func apiTopologyEdge(response api.TopologyResponse, sourceName, targetName, kind string) *api.EdgeResponse {
	namesByID := make(map[string]string, len(response.Nodes))
	for _, node := range response.Nodes {
		namesByID[node.ID] = node.Name
	}
	for index := range response.Edges {
		edge := &response.Edges[index]
		if namesByID[edge.SourceID] == sourceName && namesByID[edge.TargetID] == targetName && edge.Kind == kind {
			return edge
		}
	}
	return nil
}

func connectStartupIntegrationNATS(t *testing.T, url string) *natsgo.Conn {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := natsgo.Connect(url, natsgo.Name("eventatlas-e2e-integration-test"), natsgo.Timeout(500*time.Millisecond), natsgo.NoReconnect())
		if err == nil {
			return connection
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("connect to NATS at %s: %v", url, lastErr)
	return nil
}

func TestInitialTopologyFallsBackToPostgreSQLWhenDiscoveryFails(t *testing.T) {
	databaseURL := os.Getenv("EVENTATLAS_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("EVENTATLAS_DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("PostgreSQL ping error = %v", err)
	}
	if err := postgresstore.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	sourceID, _ := topology.NewSourceID("provider:nats:test")
	scope, _ := topology.NewDiscoveryScope("account:startup:" + startupIntegrationToken(t))
	store, err := postgresstore.NewTopologyStore(pool, sourceID, scope)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	persisted := startupTestSnapshot(t, "snapshot:persisted", scope)
	if err := store.Replace(ctx, persisted); err != nil {
		t.Fatalf("Replace(persisted) error = %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupContext,
			"DELETE FROM topology_snapshots WHERE source_id = $1 AND discovery_scope = $2",
			sourceID.String(), scope.String(),
		)
	})

	wantErr := errors.New("NATS unavailable")
	service, err := application.NewTopologyService(&startupProvider{err: wantErr}, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	got, err := loadInitialTopology(ctx, service, scope, startupTestConfig())
	if err != nil {
		t.Fatalf("loadInitialTopology() error = %v", err)
	}
	if got.ID() != persisted.ID() {
		t.Errorf("fallback snapshot = %q, want persisted %q", got.ID(), persisted.ID())
	}
}

func startupIntegrationToken(t *testing.T) string {
	t.Helper()
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate integration token: %v", err)
	}
	return hex.EncodeToString(value[:])
}
