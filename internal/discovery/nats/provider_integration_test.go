//go:build integration

package nats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
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
	for _, name := range append([]string{streamName, durableName, ephemeralName}, streamSubjects...) {
		if nodesByName[name] == nil {
			t.Errorf("discovered topology does not contain node %q", name)
		}
	}
	for _, filter := range billingFilters {
		if nodesByName[filter] != nil {
			t.Errorf("consumer selector %q was promoted to a topology node", filter)
		}
	}
	if consumerDurability[durableName] != topology.DurabilityDurable {
		t.Errorf("consumer %q durability = %q, want durable", durableName, consumerDurability[durableName])
	}
	if consumerDurability[ephemeralName] != topology.DurabilityEphemeral {
		t.Errorf("consumer %q durability = %q, want ephemeral", ephemeralName, consumerDurability[ephemeralName])
	}
	if got, err := strconv.Atoi(snapshot.Metadata()["nats.jetstream.stream_count"]); err != nil || got < 1 {
		t.Errorf("stream count metadata = %q, want at least 1", snapshot.Metadata()["nats.jetstream.stream_count"])
	}
	if got, err := strconv.Atoi(snapshot.Metadata()["nats.jetstream.consumer_count"]); err != nil || got < 2 {
		t.Errorf("consumer count metadata = %q, want at least 2", snapshot.Metadata()["nats.jetstream.consumer_count"])
	}

	capturedCount := 0
	hasConsumerCount := 0
	bindingMetadata := make(map[string]map[string]string)
	for _, edge := range snapshot.Edges() {
		sourceName := nodesByIDName(snapshot, edge.SourceID())
		targetName := nodesByIDName(snapshot, edge.TargetID())
		if edge.Kind() == topology.EdgeKindCapturedBy && targetName == streamName {
			capturedCount++
		}
		if edge.Kind() == topology.EdgeKindHasConsumer && sourceName == streamName {
			hasConsumerCount++
			bindingMetadata[targetName] = edge.Evidence()[0].Metadata()
		}
	}
	if capturedCount != 2 || hasConsumerCount != 2 {
		t.Errorf("test stream edge counts = captured:%d has:%d, want 2/2", capturedCount, hasConsumerCount)
	}
	if got := bindingMetadata[durableName][consumerFilterSubjectsMetadataKey]; got != fmt.Sprintf(`["%s","%s"]`, billingFilters[0], billingFilters[1]) {
		t.Errorf("durable consumer filter subjects = %q", got)
	}
	if got := bindingMetadata[ephemeralName][consumerFilterSubjectsMetadataKey]; got != fmt.Sprintf(`["%s"]`, subjectPrefix+".shared") {
		t.Errorf("ephemeral consumer filter subjects = %q", got)
	}

	secondSnapshot, err := provider.Discover(discoveryContext, scope)
	if err != nil {
		t.Fatalf("second Discover() error = %v", err)
	}
	secondNodesByName := make(map[string]topology.TopologyNode, len(secondSnapshot.Nodes()))
	for _, node := range secondSnapshot.Nodes() {
		secondNodesByName[node.Name()] = node
	}
	stableNodeNames := []string{"Integration NATS", streamName, durableName, ephemeralName}
	stableNodeNames = append(stableNodeNames, streamSubjects...)
	for _, name := range stableNodeNames {
		first, firstExists := nodesByName[name]
		second, secondExists := secondNodesByName[name]
		if !firstExists || !secondExists {
			t.Errorf("node %q presence across discoveries = (%t, %t), want both true", name, firstExists, secondExists)
			continue
		}
		if first.ID() != second.ID() {
			t.Errorf("node %q identity changed across discoveries: %q -> %q", name, first.ID(), second.ID())
		}
	}
	if snapshot.ID() == secondSnapshot.ID() {
		t.Errorf("separate discoveries returned the same snapshot ID %q", snapshot.ID())
	}
}

func nodesByIDName(snapshot *topology.TopologySnapshot, id topology.NodeID) string {
	for _, node := range snapshot.Nodes() {
		if node.ID() == id {
			return node.Name()
		}
	}
	return ""
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
