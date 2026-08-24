package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	observationotel "github.com/lucacox/eventatlas/internal/observation/otel"
	"github.com/lucacox/eventatlas/internal/storage/memory"
	"github.com/lucacox/eventatlas/internal/topology"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestConfigureStoresUsesBothMemoryAdaptersWithoutDatabase(t *testing.T) {
	t.Parallel()

	sourceID, _ := topology.NewSourceID("provider:nats:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	topologyStore, observationStore, closeStores, err := configureStores(
		t.Context(),
		config{},
		sourceID,
		scope,
	)
	if err != nil {
		t.Fatalf("configureStores() error = %v", err)
	}
	defer closeStores()
	if _, ok := topologyStore.(*memory.TopologyStore); !ok {
		t.Errorf("Topology store = %T, want memory adapter", topologyStore)
	}
	if _, ok := observationStore.(*memory.ObservationStoreAdapter); !ok {
		t.Errorf("Observation store = %T, want memory adapter", observationStore)
	}
}

func TestConfigureOTLPServerIsOptional(t *testing.T) {
	t.Parallel()

	scope, _ := topology.NewDiscoveryScope("account:test")
	server, err := configureOTLPServer(config{}, scope, nil)
	if err != nil {
		t.Fatalf("configureOTLPServer(disabled) error = %v", err)
	}
	if server != nil {
		t.Errorf("configureOTLPServer(disabled) = %#v, want nil", server)
	}
}

func TestConfigureOTLPServerConnectsHTTPToObservationStore(t *testing.T) {
	t.Parallel()

	scope, _ := topology.NewDiscoveryScope("account:test")
	store := memory.NewObservationStore()
	server, err := configureOTLPServer(config{
		otlpHTTPAddress:     ":4318",
		otlpSourceID:        "observation:otel:test",
		environment:         "development",
		otlpMaxRequestBytes: defaultOTLPMaxRequestBytes,
		otlpMaxFutureSkew:   defaultOTLPMaxFutureSkew,
	}, scope, store)
	if err != nil {
		t.Fatalf("configureOTLPServer() error = %v", err)
	}
	if server == nil || server.Addr != ":4318" || server.Handler == nil {
		t.Fatalf("configureOTLPServer() = %#v, want configured :4318 server", server)
	}

	observedAt := time.Now().UTC().Add(-time.Second)
	payload, err := proto.Marshal(mainTestTraceRequest(observedAt))
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		observationotel.TraceExportPath,
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", observationotel.ProtobufContentType)
	response := httptest.NewRecorder()

	server.Handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("OTLP response status = %d, want 200; body = %x", response.Code, response.Body.Bytes())
	}
	aggregates, err := store.ListActive(t.Context(), scope, observedAt.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListActive() error = %v", err)
	}
	if len(aggregates) != 1 || aggregates[0].Key().Service().Name() != "checkout" {
		t.Errorf("Stored aggregates = %#v, want checkout observation", aggregates)
	}
}

func TestConfigureOTLPServerValidatesEnabledConfiguration(t *testing.T) {
	t.Parallel()

	scope, _ := topology.NewDiscoveryScope("account:test")
	valid := config{
		otlpHTTPAddress:     ":4318",
		otlpSourceID:        "observation:otel:test",
		environment:         "development",
		otlpMaxRequestBytes: defaultOTLPMaxRequestBytes,
		otlpMaxFutureSkew:   defaultOTLPMaxFutureSkew,
	}
	invalidSource := valid
	invalidSource.otlpSourceID = " "
	if _, err := configureOTLPServer(invalidSource, scope, memory.NewObservationStore()); err == nil ||
		!strings.Contains(err.Error(), "OTLP source ID") {
		t.Errorf("configureOTLPServer(invalid source) error = %v, want source ID error", err)
	}

	var typedNilStore *memory.ObservationStoreAdapter
	if _, err := configureOTLPServer(valid, scope, typedNilStore); err == nil ||
		!strings.Contains(err.Error(), "observation service") {
		t.Errorf("configureOTLPServer(nil store) error = %v, want observation service error", err)
	}
}

func TestRunHTTPServersGracefullyStopsAllListeners(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	servers := []namedHTTPServer{
		{name: "API", server: &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()}},
		{name: "OTLP", server: &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()}},
	}
	if err := runHTTPServers(ctx, cancel, servers, time.Second); err != nil {
		t.Fatalf("runHTTPServers(canceled) error = %v", err)
	}
}

func TestRunHTTPServersReportsListenerConflicts(t *testing.T) {
	t.Parallel()

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer occupied.Close()
	ctx, cancel := context.WithCancel(context.Background())
	server := namedHTTPServer{
		name:   "OTLP",
		server: &http.Server{Addr: occupied.Addr().String(), Handler: http.NotFoundHandler()},
	}

	err = runHTTPServers(ctx, cancel, []namedHTTPServer{server}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "listen for OTLP") {
		t.Errorf("runHTTPServers(conflict) error = %v, want named listener error", err)
	}
	if ctx.Err() == nil {
		t.Error("runHTTPServers(conflict) did not cancel the shared runtime context")
	}
}

func mainTestTraceRequest(observedAt time.Time) *collectortracepb.ExportTraceServiceRequest {
	return &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
					mainTestStringAttribute("service.name", "checkout"),
					mainTestStringAttribute("deployment.environment.name", "development"),
				}},
				ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
					{
						StartTimeUnixNano: uint64(observedAt.Add(-time.Millisecond).UnixNano()),
						EndTimeUnixNano:   uint64(observedAt.UnixNano()),
						Attributes: []*commonpb.KeyValue{
							mainTestStringAttribute("messaging.operation.type", "send"),
							mainTestStringAttribute("messaging.system", "nats"),
							mainTestStringAttribute("messaging.destination.name", "orders.created"),
							mainTestStringAttribute("messaging.destination.template", "orders.*"),
						},
					},
				}}},
			},
		},
	}
}

func mainTestStringAttribute(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: value},
		},
	}
}
