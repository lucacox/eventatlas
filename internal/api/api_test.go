package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/topology"
)

func TestGetTopologyReturnsProviderNeutralSnapshot(t *testing.T) {
	t.Parallel()

	snapshot := apiTestSnapshot(t)
	handler, err := NewHandler(&apiFakeReader{snapshot: snapshot})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/topology status = %d, want 200; body = %s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	var body TopologyResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Snapshot.ID != snapshot.ID().String() || body.Snapshot.SourceID != snapshot.SourceID().String() || body.Snapshot.Scope != snapshot.Scope().String() {
		t.Errorf("snapshot identity = (%q, %q, %q), want (%q, %q, %q)", body.Snapshot.ID, body.Snapshot.SourceID, body.Snapshot.Scope, snapshot.ID(), snapshot.SourceID(), snapshot.Scope())
	}
	if len(body.Nodes) != 4 || len(body.Edges) != 2 {
		t.Fatalf("response counts = nodes:%d edges:%d, want 4/2", len(body.Nodes), len(body.Edges))
	}
	nodes := make(map[string]NodeResponse)
	for _, node := range body.Nodes {
		nodes[node.Name] = node
	}
	if broker := nodes["NATS Test"]; broker.Provider != "nats" || broker.Environment != "test" {
		t.Errorf("broker API details = %+v, want nats/test", broker)
	}
	if stream := nodes["ORDERS"]; stream.ResourceKind != "nats.jetstream.stream" || stream.BrokerID == "" {
		t.Errorf("stream API details = %+v", stream)
	}
	if consumer := nodes["billing"]; consumer.ConsumerKind != "nats.jetstream.consumer" || consumer.Durability != "durable" {
		t.Errorf("consumer API details = %+v", consumer)
	}
	if body.Edges[0].Evidence[0].Mode != "declared" || body.Edges[0].Evidence[0].SourceSystem != "nats" {
		t.Errorf("edge evidence = %+v, want declared/nats", body.Edges[0].Evidence)
	}
	if got := body.Edges[1].Evidence[0].Metadata["nats.jetstream.filter_subjects"]; got != `["orders.*"]` {
		t.Errorf("consumer binding filter subjects = %q", got)
	}
}

func TestGetTopologyReturnsNotFoundBeforeFirstRefresh(t *testing.T) {
	t.Parallel()

	handler, err := NewHandler(&apiFakeReader{err: application.ErrTopologyNotFound})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body = %s", response.Code, response.Body.String())
	}
}

func TestGetTopologyReturnsInternalServerErrorForUnexpectedFailure(t *testing.T) {
	t.Parallel()

	handler, err := NewHandler(&apiFakeReader{err: errors.New("storage failed")})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "storage failed") {
		t.Errorf("internal error leaked in response: %s", response.Body.String())
	}
}

func TestGetTopologyReturnsInternalServerErrorForNilSnapshot(t *testing.T) {
	t.Parallel()

	handler, err := NewHandler(&apiFakeReader{})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", response.Code, response.Body.String())
	}
}

func TestHandlerServesStoplightElementsAndOpenAPI(t *testing.T) {
	t.Parallel()

	handler, err := NewHandler(&apiFakeReader{snapshot: apiTestSnapshot(t)})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	docs := httptest.NewRecorder()
	handler.ServeHTTP(docs, httptest.NewRequest(http.MethodGet, "/docs", nil))
	if docs.Code != http.StatusOK || !strings.Contains(docs.Body.String(), "<elements-api") {
		t.Errorf("GET /docs = %d, Stoplight Elements present = %t; body = %s", docs.Code, strings.Contains(docs.Body.String(), "<elements-api"), docs.Body.String())
	}

	spec := httptest.NewRecorder()
	handler.ServeHTTP(spec, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if spec.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json status = %d, want 200", spec.Code)
	}
	data, err := io.ReadAll(spec.Body)
	if err != nil {
		t.Fatalf("read OpenAPI response: %v", err)
	}
	if !strings.Contains(string(data), `"/api/v1/topology"`) || !strings.Contains(string(data), `"operationId":"get-topology"`) {
		t.Errorf("OpenAPI document does not describe topology operation: %s", data)
	}
}

func TestNewHandlerRejectsNilReader(t *testing.T) {
	t.Parallel()

	var typedNil *apiFakeReader
	if _, err := NewHandler(nil); !errors.Is(err, ErrTopologyReaderNil) {
		t.Errorf("NewHandler(nil) error = %v, want %v", err, ErrTopologyReaderNil)
	}
	if _, err := NewHandler(typedNil); !errors.Is(err, ErrTopologyReaderNil) {
		t.Errorf("NewHandler(typed nil) error = %v, want %v", err, ErrTopologyReaderNil)
	}
}

type apiFakeReader struct {
	snapshot *topology.TopologySnapshot
	err      error
}

func (reader *apiFakeReader) Current(context.Context) (*topology.TopologySnapshot, error) {
	return reader.snapshot, reader.err
}

func apiTestSnapshot(t *testing.T) *topology.TopologySnapshot {
	t.Helper()

	sourceID, _ := topology.NewSourceID("provider:nats:test")
	broker, err := topology.NewBroker("broker:test", "NATS Test", "nats", "test", nil)
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	destination, err := topology.NewDestination("destination:orders", "orders.*", topology.DestinationKindSubject, broker.ID(), "orders.*", map[string]string{"nats.subject.is_pattern": "true"})
	if err != nil {
		t.Fatalf("NewDestination() error = %v", err)
	}
	resource, err := topology.NewMessagingResource("resource:orders", "ORDERS", topology.ResourceKind("nats.jetstream.stream"), broker.ID(), nil)
	if err != nil {
		t.Fatalf("NewMessagingResource() error = %v", err)
	}
	consumer, err := topology.NewConsumer("consumer:billing", "billing", topology.ConsumerKind("nats.jetstream.consumer"), topology.DurabilityDurable, broker.ID(), nil)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	capturedAt := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	sourceSystem, _ := topology.NewSourceSystem("nats")
	evidence, err := topology.NewEvidence(sourceID, topology.EvidenceModeDeclared, sourceSystem, capturedAt, capturedAt, map[string]string{"account": "test"})
	if err != nil {
		t.Fatalf("NewEvidence() error = %v", err)
	}
	bindingEvidence, err := topology.NewEvidence(sourceID, topology.EvidenceModeDeclared, sourceSystem, capturedAt, capturedAt, map[string]string{
		"account":                        "test",
		"nats.jetstream.filter_mode":     "subjects",
		"nats.jetstream.filter_subjects": `["orders.*"]`,
	})
	if err != nil {
		t.Fatalf("NewEvidence(binding) error = %v", err)
	}
	edges := make([]topology.Edge, 0, 2)
	for _, relation := range []struct {
		source   topology.TopologyNode
		target   topology.TopologyNode
		kind     topology.EdgeKind
		evidence topology.Evidence
	}{
		{source: destination, target: resource, kind: topology.EdgeKindCapturedBy, evidence: evidence},
		{source: resource, target: consumer, kind: topology.EdgeKindHasConsumer, evidence: bindingEvidence},
	} {
		edge, err := topology.NewEdge(relation.source, relation.target, relation.kind, []topology.Evidence{relation.evidence})
		if err != nil {
			t.Fatalf("NewEdge(%s) error = %v", relation.kind, err)
		}
		edges = append(edges, edge)
	}
	id, _ := topology.NewSnapshotID("snapshot:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	snapshot, err := topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:           id,
		SourceID:     sourceID,
		Scope:        scope,
		CapturedAt:   capturedAt,
		Nodes:        []topology.TopologyNode{broker, destination, resource, consumer},
		Edges:        edges,
		Completeness: topology.SnapshotCompletenessFull,
		Metadata:     map[string]string{"nats.jetstream.stream_count": "1"},
	})
	if err != nil {
		t.Fatalf("NewTopologySnapshot() error = %v", err)
	}
	return snapshot
}
