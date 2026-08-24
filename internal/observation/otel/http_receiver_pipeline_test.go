package otel

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/storage/memory"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestHTTPReceiverPipelinePersistsNormalizedFacts(t *testing.T) {
	t.Parallel()

	store := memory.NewObservationStore()
	service, err := application.NewObservationService(store)
	if err != nil {
		t.Fatalf("NewObservationService() error = %v", err)
	}
	normalizer := newTestNormalizer(t)
	handler, err := NewHTTPHandler(normalizer, service, HTTPReceiverConfig{MaxRequestBytes: receiverTestMaxRequestBytes})
	if err != nil {
		t.Fatalf("NewHTTPHandler() error = %v", err)
	}

	exportRequest := validTraceRequest(normalizerTestObservedAt)
	first := spanOf(exportRequest)
	second := cloneSpan(first)
	second.StartTimeUnixNano = uint64(normalizerTestObservedAt.Add(time.Minute).UnixNano())
	second.EndTimeUnixNano = uint64(normalizerTestObservedAt.Add(time.Minute + time.Second).UnixNano())
	exportRequest.ResourceSpans[0].ScopeSpans[0].Spans = []*tracepb.Span{first, second}
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, newProtobufHTTPRequest(t, exportRequest))

	decodeTraceResponse(t, response, http.StatusOK)
	config := normalizerTestConfig()
	aggregates, err := store.ListActive(t.Context(), config.Scope, normalizerTestObservedAt.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListActive() error = %v", err)
	}
	if len(aggregates) != 1 {
		t.Fatalf("ListActive() aggregates = %d, want one merged relationship", len(aggregates))
	}
	aggregate := aggregates[0]
	if aggregate.ObservationCount() != 2 {
		t.Errorf("Aggregate observation count = %d, want 2", aggregate.ObservationCount())
	}
	if !aggregate.FirstSeen().Equal(normalizerTestObservedAt.Add(time.Second)) ||
		!aggregate.LastSeen().Equal(normalizerTestObservedAt.Add(time.Minute+time.Second)) {
		t.Errorf(
			"Aggregate range = %v..%v, want %v..%v",
			aggregate.FirstSeen(),
			aggregate.LastSeen(),
			normalizerTestObservedAt.Add(time.Second),
			normalizerTestObservedAt.Add(time.Minute+time.Second),
		)
	}
	if aggregate.Key().Service().Name() != "checkout" || aggregate.Key().Destination().LogicalName() != "orders.*" {
		t.Errorf("Aggregate key = %#v, want checkout -> orders.*", aggregate.Key())
	}
}
