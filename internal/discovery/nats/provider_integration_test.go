//go:build integration

package nats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestProviderDiscoversRealJetStream(t *testing.T) {
	natsURL := os.Getenv("EVENTATLAS_NATS_URL")
	if natsURL == "" {
		t.Skip("EVENTATLAS_NATS_URL is required for NATS integration tests")
	}

	connection := connectIntegrationNATS(t, natsURL)
	t.Cleanup(connection.Close)
	manager, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	token := integrationToken(t)
	streamName := "EA_ORDERS_" + token
	durableName := "billing_" + token
	ephemeralName := "audit_" + token
	subjectPrefix := "eventatlas." + token
	streamSubjects := []string{subjectPrefix + ".orders.*", subjectPrefix + ".shared"}
	billingFilters := []string{subjectPrefix + ".orders.created", subjectPrefix + ".orders.updated"}

	setupContext, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSetup()
	if _, err := manager.CreateStream(setupContext, jetstream.StreamConfig{
		Name:        streamName,
		Description: "EventAtlas integration test stream",
		Subjects:    streamSubjects,
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.MemoryStorage,
		Replicas:    1,
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.DeleteStream(cleanupContext, streamName); err != nil {
			t.Errorf("DeleteStream(%q) cleanup error = %v", streamName, err)
		}
	})

	if _, err := manager.CreateOrUpdateConsumer(setupContext, streamName, jetstream.ConsumerConfig{
		Name:           durableName,
		Durable:        durableName,
		Description:    "Billing integration consumer",
		FilterSubjects: billingFilters,
		AckPolicy:      jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("CreateOrUpdateConsumer(%q) error = %v", durableName, err)
	}
	if _, err := manager.CreateOrUpdateConsumer(setupContext, streamName, jetstream.ConsumerConfig{
		Name:              ephemeralName,
		FilterSubject:     subjectPrefix + ".shared",
		InactiveThreshold: time.Minute,
		AckPolicy:         jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("CreateOrUpdateConsumer(%q) error = %v", ephemeralName, err)
	}

	client, err := NewJetStreamClient(manager)
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}
	sourceID, err := topology.NewSourceID("provider:nats:integration")
	if err != nil {
		t.Fatalf("NewSourceID() error = %v", err)
	}
	provider, err := NewProvider(client, Config{
		SourceID:    sourceID,
		BrokerName:  "Integration NATS",
		Environment: "test",
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	scope, err := topology.NewDiscoveryScope("integration:default-account")
	if err != nil {
		t.Fatalf("NewDiscoveryScope() error = %v", err)
	}

	discoveryContext, cancelDiscovery := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDiscovery()
	snapshot, err := provider.Discover(discoveryContext, scope)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	nodesByName := make(map[string]topology.TopologyNode)
	consumerDurability := make(map[string]topology.Durability)
	for _, node := range snapshot.Nodes() {
		nodesByName[node.Name()] = node
		if consumer, ok := node.(*topology.Consumer); ok {
			consumerDurability[consumer.Name()] = consumer.Durability()
		}
	}
	for _, name := range append(append([]string{streamName, durableName, ephemeralName}, streamSubjects...), billingFilters...) {
		if nodesByName[name] == nil {
			t.Errorf("discovered topology does not contain node %q", name)
		}
	}
	if consumerDurability[durableName] != topology.DurabilityDurable {
		t.Errorf("consumer %q durability = %q, want durable", durableName, consumerDurability[durableName])
	}
	if consumerDurability[ephemeralName] != topology.DurabilityEphemeral {
		t.Errorf("consumer %q durability = %q, want ephemeral", ephemeralName, consumerDurability[ephemeralName])
	}
	if got := snapshot.Metadata()["nats.jetstream.stream_count"]; got != "1" {
		t.Errorf("stream count metadata = %q, want 1", got)
	}
	if got := snapshot.Metadata()["nats.jetstream.consumer_count"]; got != "2" {
		t.Errorf("consumer count metadata = %q, want 2", got)
	}

	edgeCounts := make(map[topology.EdgeKind]int)
	for _, edge := range snapshot.Edges() {
		edgeCounts[edge.Kind()]++
	}
	if edgeCounts[topology.EdgeKindCapturedBy] != 2 || edgeCounts[topology.EdgeKindHasConsumer] != 2 || edgeCounts[topology.EdgeKindFilters] != 3 {
		t.Errorf("edge counts = captured:%d has:%d filters:%d, want 2/2/3", edgeCounts[topology.EdgeKindCapturedBy], edgeCounts[topology.EdgeKindHasConsumer], edgeCounts[topology.EdgeKindFilters])
	}

	secondSnapshot, err := provider.Discover(discoveryContext, scope)
	if err != nil {
		t.Fatalf("second Discover() error = %v", err)
	}
	if !slices.Equal(integrationNodeIDs(snapshot), integrationNodeIDs(secondSnapshot)) {
		t.Errorf("node identities changed across equivalent discoveries:\nfirst  %v\nsecond %v", integrationNodeIDs(snapshot), integrationNodeIDs(secondSnapshot))
	}
	if snapshot.ID() == secondSnapshot.ID() {
		t.Errorf("separate discoveries returned the same snapshot ID %q", snapshot.ID())
	}
}

func connectIntegrationNATS(t *testing.T, url string) *natsgo.Conn {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := natsgo.Connect(
			url,
			natsgo.Name("eventatlas-integration-test"),
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

func integrationToken(t *testing.T) string {
	t.Helper()
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate integration test token: %v", err)
	}
	return hex.EncodeToString(value[:])
}

func integrationNodeIDs(snapshot *topology.TopologySnapshot) []string {
	result := make([]string, 0, len(snapshot.Nodes()))
	for _, node := range snapshot.Nodes() {
		result = append(result, fmt.Sprintf("%s=%s", node.Name(), node.ID()))
	}
	return result
}
