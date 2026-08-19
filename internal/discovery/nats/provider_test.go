package nats

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestProviderDiscoversStreamTopology(t *testing.T) {
	t.Parallel()

	streams := []Stream{
		{Name: "PAYMENTS", Subjects: []string{"payments.received", "events.shared"}, Retention: "limits", Storage: "file", Replicas: 1},
		{Name: "ORDERS", Description: "Order events", Subjects: []string{"orders.*", "events.shared", "orders.*"}, Retention: "workqueue", Storage: "memory", Replicas: 3},
	}
	provider := newTestProvider(t, &fakeClient{streams: streams})
	capturedAt := time.Date(2026, time.August, 19, 15, 30, 0, 0, time.FixedZone("CEST", 2*60*60))
	provider.now = func() time.Time { return capturedAt }
	scope := mustDiscoveryScope(t, "account:production")

	snapshot, err := provider.Discover(context.Background(), scope)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if snapshot.SourceID() != provider.sourceID || snapshot.Scope() != scope {
		t.Errorf("snapshot source/scope = (%q, %q), want (%q, %q)", snapshot.SourceID(), snapshot.Scope(), provider.sourceID, scope)
	}
	if snapshot.Completeness() != topology.SnapshotCompletenessFull {
		t.Errorf("snapshot completeness = %q, want full", snapshot.Completeness())
	}
	if !strings.HasPrefix(snapshot.ID().String(), "snapshot:nats:") {
		t.Errorf("snapshot ID = %q, want NATS snapshot prefix", snapshot.ID())
	}
	if !snapshot.CapturedAt().Equal(capturedAt) || snapshot.CapturedAt().Location() != time.UTC {
		t.Errorf("snapshot captured at = %v, want UTC instant %v", snapshot.CapturedAt(), capturedAt)
	}
	if got := snapshot.Metadata()["nats.jetstream.stream_count"]; got != "2" {
		t.Errorf("snapshot stream count metadata = %q, want 2", got)
	}
	if got := snapshot.Metadata()["nats.jetstream.consumer_count"]; got != "0" {
		t.Errorf("snapshot consumer count metadata = %q, want 0", got)
	}

	brokers, destinations, resources := collectSnapshotNodes(t, snapshot)
	if len(brokers) != 1 || len(destinations) != 3 || len(resources) != 2 {
		t.Fatalf("node counts = brokers:%d destinations:%d resources:%d, want 1/3/2", len(brokers), len(destinations), len(resources))
	}
	if brokers[0].Name() != "NATS Main" || brokers[0].Provider() != "nats" || brokers[0].Environment() != "production" {
		t.Errorf("broker = (%q, %q, %q), want (NATS Main, nats, production)", brokers[0].Name(), brokers[0].Provider(), brokers[0].Environment())
	}
	orders := resources["ORDERS"]
	if orders == nil {
		t.Fatal("ORDERS resource not found")
	}
	if value, _ := orders.Attribute("nats.jetstream.description"); value != "Order events" {
		t.Errorf("ORDERS description = %q, want Order events", value)
	}
	for attribute, want := range map[string]string{
		"nats.jetstream.retention": "workqueue",
		"nats.jetstream.storage":   "memory",
		"nats.jetstream.replicas":  "3",
	} {
		if got, _ := orders.Attribute(attribute); got != want {
			t.Errorf("ORDERS attribute %s = %q, want %q", attribute, got, want)
		}
	}
	pattern := destinations["orders.*"]
	if pattern == nil {
		t.Fatal("orders.* destination not found")
	}
	if value, present := pattern.Attribute("nats.subject.is_pattern"); !present || value != "true" {
		t.Errorf("orders.* pattern attribute = (%q, %t), want (true, true)", value, present)
	}
	if _, present := destinations["events.shared"].Attribute("nats.subject.is_pattern"); present {
		t.Error("literal events.shared destination marked as pattern")
	}

	edges := snapshot.Edges()
	if len(edges) != 4 {
		t.Fatalf("edge count = %d, want 4 unique stream/subject relationships", len(edges))
	}
	for _, edge := range edges {
		if edge.Kind() != topology.EdgeKindCapturedBy {
			t.Errorf("edge kind = %q, want captured_by", edge.Kind())
		}
		evidence := edge.Evidence()
		if len(evidence) != 1 || evidence[0].Mode() != topology.EvidenceModeDeclared || evidence[0].SourceID() != provider.sourceID {
			t.Errorf("edge evidence = %+v, want one declared record from %q", evidence, provider.sourceID)
		}
		if evidence[0].SourceSystem().String() != "nats" || !evidence[0].FirstSeen().Equal(capturedAt) || !evidence[0].LastSeen().Equal(capturedAt) {
			t.Errorf("edge evidence source/time = (%q, %v, %v), want nats/%v", evidence[0].SourceSystem(), evidence[0].FirstSeen(), evidence[0].LastSeen(), capturedAt)
		}
	}
}

func TestProviderDiscoversConsumersAndBindingSelectors(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, &fakeClient{streams: []Stream{
		{
			Name:      "ORDERS",
			Subjects:  []string{"orders.>"},
			Retention: "limits",
			Storage:   "file",
			Replicas:  1,
			Consumers: []Consumer{
				{
					Name:           "billing",
					Description:    "Billing worker",
					Durable:        true,
					FilterSubjects: []string{"orders.updated", "orders.created", "orders.created", "orders.>"},
				},
				{
					Name:           "audit",
					FilterSubjects: []string{"orders.*"},
					DeliverSubject: "deliver.audit",
				},
				{Name: "catchall"},
			},
		},
	}})
	provider.now = func() time.Time { return time.Date(2026, time.August, 19, 14, 0, 0, 0, time.UTC) }

	snapshot, err := provider.Discover(context.Background(), mustDiscoveryScope(t, "account:production"))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if got := snapshot.Metadata()["nats.jetstream.consumer_count"]; got != "3" {
		t.Errorf("snapshot consumer count metadata = %q, want 3", got)
	}

	_, destinations, resources := collectSnapshotNodes(t, snapshot)
	consumers := collectSnapshotConsumers(t, snapshot)
	if len(destinations) != 1 || len(resources) != 1 || len(consumers) != 3 {
		t.Fatalf("node counts = destinations:%d resources:%d consumers:%d, want 1/1/3", len(destinations), len(resources), len(consumers))
	}
	billing := consumers["billing"]
	if billing == nil || billing.Durability() != topology.DurabilityDurable {
		t.Fatalf("billing consumer = %v, want durable", billing)
	}
	if billing.ConsumerKind() != topology.ConsumerKind("nats.jetstream.consumer") {
		t.Errorf("billing consumer kind = %q, want nats.jetstream.consumer", billing.ConsumerKind())
	}
	if description, _ := billing.Attribute("nats.jetstream.description"); description != "Billing worker" {
		t.Errorf("billing description = %q, want Billing worker", description)
	}
	if mode, _ := billing.Attribute("nats.jetstream.delivery_mode"); mode != "pull" {
		t.Errorf("billing delivery mode = %q, want pull", mode)
	}
	audit := consumers["audit"]
	if audit == nil || audit.Durability() != topology.DurabilityEphemeral {
		t.Fatalf("audit consumer = %v, want ephemeral", audit)
	}
	if mode, _ := audit.Attribute("nats.jetstream.delivery_mode"); mode != "push" {
		t.Errorf("audit delivery mode = %q, want push", mode)
	}
	if deliverSubject, _ := audit.Attribute("nats.jetstream.deliver_subject"); deliverSubject != "deliver.audit" {
		t.Errorf("audit deliver subject = %q, want deliver.audit", deliverSubject)
	}

	nodeNames := make(map[topology.NodeID]string)
	for _, node := range snapshot.Nodes() {
		nodeNames[node.ID()] = node.Name()
	}
	edgeCounts := make(map[topology.EdgeKind]int)
	bindingMetadata := make(map[string]map[string]string)
	for _, edge := range snapshot.Edges() {
		edgeCounts[edge.Kind()]++
		if edge.Kind() == topology.EdgeKindHasConsumer {
			consumerName := nodeNames[edge.TargetID()]
			bindingMetadata[consumerName] = edge.Evidence()[0].Metadata()
		}
	}
	if edgeCounts[topology.EdgeKindCapturedBy] != 1 || edgeCounts[topology.EdgeKindHasConsumer] != 3 {
		t.Errorf("edge counts = captured:%d has:%d, want 1/3", edgeCounts[topology.EdgeKindCapturedBy], edgeCounts[topology.EdgeKindHasConsumer])
	}
	if got := bindingMetadata["billing"][consumerFilterModeMetadataKey]; got != consumerFilterModeSubjects {
		t.Errorf("billing filter mode = %q, want subjects", got)
	}
	if got := bindingMetadata["billing"][consumerFilterSubjectsMetadataKey]; got != `["orders.>","orders.created","orders.updated"]` {
		t.Errorf("billing filter subjects = %q", got)
	}
	if got := bindingMetadata["audit"][consumerFilterSubjectsMetadataKey]; got != `["orders.*"]` {
		t.Errorf("audit filter subjects = %q", got)
	}
	if got := bindingMetadata["catchall"][consumerFilterModeMetadataKey]; got != consumerFilterModeAll {
		t.Errorf("catchall filter mode = %q, want all", got)
	}
	if got := bindingMetadata["catchall"][consumerFilterSubjectsMetadataKey]; got != `[]` {
		t.Errorf("catchall filter subjects = %q, want []", got)
	}
	for _, filter := range []string{"orders.created", "orders.updated", "orders.*"} {
		if destinations[filter] != nil {
			t.Errorf("consumer selector %q was promoted to a destination node", filter)
		}
	}
}

func TestProviderProducesDeterministicTopologyOrderAndIdentity(t *testing.T) {
	t.Parallel()

	firstInput := []Stream{
		{Name: "ZETA", Subjects: []string{"z.2", "z.1"}, Consumers: []Consumer{{Name: "worker-b", FilterSubjects: []string{"z.2", "z.1"}}, {Name: "worker-a"}}},
		{Name: "ALPHA", Subjects: []string{"a.2", "a.1"}},
	}
	secondInput := []Stream{
		{Name: "ALPHA", Subjects: []string{"a.1", "a.2"}},
		{Name: "ZETA", Subjects: []string{"z.1", "z.2"}, Consumers: []Consumer{{Name: "worker-a"}, {Name: "worker-b", FilterSubjects: []string{"z.1", "z.2"}}}},
	}
	capturedAt := time.Date(2026, time.August, 19, 14, 0, 0, 0, time.UTC)
	scope := mustDiscoveryScope(t, "account:production")

	first := newTestProvider(t, &fakeClient{streams: firstInput})
	first.now = func() time.Time { return capturedAt }
	firstSnapshot, err := first.Discover(context.Background(), scope)
	if err != nil {
		t.Fatalf("first Discover() error = %v", err)
	}
	second := newTestProvider(t, &fakeClient{streams: secondInput})
	second.now = func() time.Time { return capturedAt }
	secondSnapshot, err := second.Discover(context.Background(), scope)
	if err != nil {
		t.Fatalf("second Discover() error = %v", err)
	}

	if !slices.Equal(snapshotNodeIDs(firstSnapshot), snapshotNodeIDs(secondSnapshot)) {
		t.Errorf("node order/identity differs:\nfirst  %v\nsecond %v", snapshotNodeIDs(firstSnapshot), snapshotNodeIDs(secondSnapshot))
	}
	if !slices.Equal(snapshotEdgeKeys(firstSnapshot), snapshotEdgeKeys(secondSnapshot)) {
		t.Errorf("edge order/identity differs:\nfirst  %v\nsecond %v", snapshotEdgeKeys(firstSnapshot), snapshotEdgeKeys(secondSnapshot))
	}
}

func TestProviderSupportsEmptyJetStreamAccount(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, &fakeClient{})
	provider.now = func() time.Time { return time.Date(2026, time.August, 19, 14, 0, 0, 0, time.UTC) }
	snapshot, err := provider.Discover(context.Background(), mustDiscoveryScope(t, "account:empty"))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(snapshot.Nodes()) != 1 || snapshot.Nodes()[0].Kind() != topology.NodeKindBroker {
		t.Errorf("empty account nodes = %v, want broker only", snapshot.Nodes())
	}
	if len(snapshot.Edges()) != 0 {
		t.Errorf("empty account edges = %v, want none", snapshot.Edges())
	}
}

func TestProviderRejectsInvalidNativeTopology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		streams []Stream
		wantErr error
	}{
		{name: "blank stream name", streams: []Stream{{Name: " ", Subjects: []string{"orders.*"}}}, wantErr: ErrStreamNameEmpty},
		{name: "duplicate stream", streams: []Stream{{Name: "ORDERS"}, {Name: "ORDERS"}}, wantErr: ErrStreamDuplicate},
		{name: "blank subject", streams: []Stream{{Name: "ORDERS", Subjects: []string{"orders.*", " "}}}, wantErr: ErrStreamSubjectEmpty},
		{name: "blank consumer name", streams: []Stream{{Name: "ORDERS", Consumers: []Consumer{{Name: " "}}}}, wantErr: ErrConsumerNameEmpty},
		{name: "duplicate consumer", streams: []Stream{{Name: "ORDERS", Consumers: []Consumer{{Name: "billing"}, {Name: "billing"}}}}, wantErr: ErrConsumerDuplicate},
		{name: "blank consumer filter", streams: []Stream{{Name: "ORDERS", Consumers: []Consumer{{Name: "billing", FilterSubjects: []string{"orders.*", " "}}}}}, wantErr: ErrConsumerFilterEmpty},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			provider := newTestProvider(t, &fakeClient{streams: test.streams})
			provider.now = func() time.Time { return time.Date(2026, time.August, 19, 14, 0, 0, 0, time.UTC) }
			if _, err := provider.Discover(context.Background(), mustDiscoveryScope(t, "account:production")); !errors.Is(err, test.wantErr) {
				t.Errorf("Discover() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestProviderPropagatesClientFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("connection closed")
	provider := newTestProvider(t, &fakeClient{err: wantErr})
	snapshot, err := provider.Discover(context.Background(), mustDiscoveryScope(t, "account:production"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Discover() error = %v, want wrapped %v", err, wantErr)
	}
	if snapshot != nil {
		t.Errorf("Discover() snapshot = %v, want nil", snapshot)
	}
}

func TestProviderPropagatesSnapshotIDFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("random source unavailable")
	provider := newTestProvider(t, &fakeClient{})
	provider.now = func() time.Time { return time.Date(2026, time.August, 19, 14, 0, 0, 0, time.UTC) }
	provider.newSnapshotID = func() (topology.SnapshotID, error) {
		return topology.SnapshotID{}, wantErr
	}
	snapshot, err := provider.Discover(context.Background(), mustDiscoveryScope(t, "account:production"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Discover() error = %v, want wrapped %v", err, wantErr)
	}
	if snapshot != nil {
		t.Errorf("Discover() snapshot = %v, want nil", snapshot)
	}
}

func TestNewProviderValidatesConfiguration(t *testing.T) {
	t.Parallel()

	validSource := mustSourceID(t, "provider:nats:production-main")
	var typedNil *fakeClient
	tests := []struct {
		name    string
		client  Client
		config  Config
		wantErr error
	}{
		{name: "nil client", config: Config{SourceID: validSource, BrokerName: "NATS", Environment: "production"}, wantErr: ErrClientNil},
		{name: "typed nil client", client: typedNil, config: Config{SourceID: validSource, BrokerName: "NATS", Environment: "production"}, wantErr: ErrClientNil},
		{name: "zero source ID", client: &fakeClient{}, config: Config{BrokerName: "NATS", Environment: "production"}, wantErr: ErrSourceIDInvalid},
		{name: "blank broker name", client: &fakeClient{}, config: Config{SourceID: validSource, BrokerName: " ", Environment: "production"}, wantErr: ErrBrokerNameEmpty},
		{name: "blank environment", client: &fakeClient{}, config: Config{SourceID: validSource, BrokerName: "NATS", Environment: "\t"}, wantErr: ErrEnvironmentEmpty},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewProvider(test.client, test.config); !errors.Is(err, test.wantErr) {
				t.Errorf("NewProvider() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestProviderRejectsEmptyDiscoveryScope(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, &fakeClient{})
	if _, err := provider.Discover(context.Background(), topology.DiscoveryScope{}); !errors.Is(err, ErrDiscoveryScopeEmpty) {
		t.Errorf("Discover() error = %v, want %v", err, ErrDiscoveryScopeEmpty)
	}
}

type fakeClient struct {
	streams []Stream
	err     error
}

func (client *fakeClient) ListStreams(context.Context) ([]Stream, error) {
	if client.err != nil {
		return nil, client.err
	}
	return append([]Stream(nil), client.streams...), nil
}

func newTestProvider(t *testing.T, client Client) *Provider {
	t.Helper()
	provider, err := NewProvider(client, Config{
		SourceID:    mustSourceID(t, "provider:nats:production-main"),
		BrokerName:  " NATS Main ",
		Environment: " production ",
	})
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	return provider
}

func collectSnapshotNodes(t *testing.T, snapshot *topology.TopologySnapshot) ([]*topology.Broker, map[string]*topology.Destination, map[string]*topology.MessagingResource) {
	t.Helper()
	brokers := make([]*topology.Broker, 0)
	destinations := make(map[string]*topology.Destination)
	resources := make(map[string]*topology.MessagingResource)
	for _, node := range snapshot.Nodes() {
		switch value := node.(type) {
		case *topology.Broker:
			brokers = append(brokers, value)
		case *topology.Destination:
			destinations[value.Name()] = value
		case *topology.MessagingResource:
			resources[value.Name()] = value
		case *topology.Consumer:
			// Consumers are collected separately by tests that need them.
		default:
			t.Errorf("unexpected snapshot node type %T", node)
		}
	}
	return brokers, destinations, resources
}

func collectSnapshotConsumers(t *testing.T, snapshot *topology.TopologySnapshot) map[string]*topology.Consumer {
	t.Helper()
	consumers := make(map[string]*topology.Consumer)
	for _, node := range snapshot.Nodes() {
		if consumer, ok := node.(*topology.Consumer); ok {
			consumers[consumer.Name()] = consumer
		}
	}
	return consumers
}

func snapshotNodeIDs(snapshot *topology.TopologySnapshot) []string {
	result := make([]string, 0, len(snapshot.Nodes()))
	for _, node := range snapshot.Nodes() {
		result = append(result, node.ID().String())
	}
	return result
}

func snapshotEdgeKeys(snapshot *topology.TopologySnapshot) []string {
	result := make([]string, 0, len(snapshot.Edges()))
	for _, edge := range snapshot.Edges() {
		result = append(result, edge.SourceID().String()+"|"+string(edge.Kind())+"|"+edge.TargetID().String())
	}
	return result
}
