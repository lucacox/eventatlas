//go:build integration

package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/lucacox/eventatlas/internal/application"
	natsdiscovery "github.com/lucacox/eventatlas/internal/discovery/nats"
	"github.com/lucacox/eventatlas/internal/storage/memory"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestTopologyAPIFromRealJetStream(t *testing.T) {
	natsURL := os.Getenv("EVENTATLAS_NATS_URL")
	if natsURL == "" {
		t.Skip("EVENTATLAS_NATS_URL is required for NATS integration tests")
	}

	connection := connectAPIIntegrationNATS(t, natsURL)
	t.Cleanup(connection.Close)
	manager, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	token := apiIntegrationToken(t)
	streamName := "EA_API_" + token
	consumerName := "worker_" + token
	subject := "eventatlas.api." + token + ".events.*"
	filter := "eventatlas.api." + token + ".events.created"
	setupContext, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSetup()
	if _, err := manager.CreateStream(setupContext, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  jetstream.MemoryStorage,
		Replicas: 1,
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.DeleteStream(ctx, streamName); err != nil {
			t.Errorf("DeleteStream(%q) cleanup error = %v", streamName, err)
		}
	})
	if _, err := manager.CreateOrUpdateConsumer(setupContext, streamName, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	client, err := natsdiscovery.NewJetStreamClient(manager)
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}
	sourceID, _ := topology.NewSourceID("provider:nats:api-integration")
	provider, err := natsdiscovery.NewProvider(client, natsdiscovery.Config{
		SourceID:    sourceID,
		BrokerName:  "Integration NATS",
		Environment: "test",
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	store := memory.NewTopologyStore()
	service, err := application.NewTopologyService(provider, store)
	if err != nil {
		t.Fatalf("NewTopologyService() error = %v", err)
	}
	scope, _ := topology.NewDiscoveryScope("integration:api")
	discoveryContext, cancelDiscovery := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDiscovery()
	if _, err := service.Refresh(discoveryContext, scope); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}

	handler, err := NewHandler(service)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/topology status = %d, want 200; body = %s", response.Code, response.Body.String())
	}
	var body TopologyResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode topology response: %v", err)
	}
	if body.Snapshot.SourceID != sourceID.String() || body.Snapshot.Scope != scope.String() {
		t.Errorf("API snapshot source/scope = (%q, %q), want (%q, %q)", body.Snapshot.SourceID, body.Snapshot.Scope, sourceID, scope)
	}
	nodeNames := make(map[string]NodeResponse)
	for _, node := range body.Nodes {
		nodeNames[node.Name] = node
	}
	for _, name := range []string{"Integration NATS", streamName, subject, consumerName, filter} {
		if _, exists := nodeNames[name]; !exists {
			t.Errorf("API topology does not contain node %q", name)
		}
	}
	if consumer := nodeNames[consumerName]; consumer.Durability != "durable" {
		t.Errorf("consumer durability = %q, want durable", consumer.Durability)
	}
	edgeCounts := make(map[string]int)
	for _, edge := range body.Edges {
		edgeCounts[edge.Kind]++
	}
	if edgeCounts["captured_by"] != 1 || edgeCounts["has_consumer"] != 1 || edgeCounts["filters"] != 1 {
		t.Errorf("API edge counts = captured:%d has:%d filters:%d, want 1/1/1", edgeCounts["captured_by"], edgeCounts["has_consumer"], edgeCounts["filters"])
	}
}

func connectAPIIntegrationNATS(t *testing.T, url string) *natsgo.Conn {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := natsgo.Connect(
			url,
			natsgo.Name("eventatlas-api-integration-test"),
			natsgo.Timeout(500*time.Millisecond),
			natsgo.NoReconnect(),
		)
		if err == nil {
			return connection
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("connect to NATS at %s: %v", url, lastErr)
	return nil
}

func apiIntegrationToken(t *testing.T) string {
	t.Helper()
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate integration test token: %v", err)
	}
	return hex.EncodeToString(value[:])
}
