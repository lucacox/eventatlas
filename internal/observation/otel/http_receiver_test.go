package otel

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lucacox/eventatlas/internal/observation"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

const receiverTestMaxRequestBytes = 1024 * 1024

func TestHTTPHandlerAcceptsAndPersistsProtobufTraceRequest(t *testing.T) {
	t.Parallel()

	ingestor := &receiverIngestorStub{}
	handler := newTestHTTPHandler(t, ingestor, receiverTestMaxRequestBytes)
	request := newProtobufHTTPRequest(t, validTraceRequest(normalizerTestObservedAt))
	request.Header.Set("Content-Type", ProtobufContentType+"; charset=binary")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	exportResponse := decodeTraceResponse(t, response, http.StatusOK)
	if exportResponse.PartialSuccess != nil {
		t.Errorf("Partial success = %#v, want unset", exportResponse.PartialSuccess)
	}
	if ingestor.calls != 1 || len(ingestor.facts) != 1 {
		t.Fatalf("Ingest() calls/facts = %d/%d, want 1/1", ingestor.calls, len(ingestor.facts))
	}
	if ingestor.facts[0].Service().Name() != "checkout" || ingestor.facts[0].Destination().LogicalName() != "orders.*" {
		t.Errorf("Ingested fact = %#v, want checkout -> orders.*", ingestor.facts[0])
	}
}

func TestHTTPHandlerReturnsPartialSuccessForMixedBatch(t *testing.T) {
	t.Parallel()

	ingestor := &receiverIngestorStub{}
	handler := newTestHTTPHandler(t, ingestor, receiverTestMaxRequestBytes)
	exportRequest := validTraceRequest(normalizerTestObservedAt)
	valid := spanOf(exportRequest)
	ignored := cloneSpan(valid)
	setStringAttribute(ignored.Attributes, attributeMessagingOperation, "receive")
	rejectedDestination := cloneSpan(valid)
	rejectedDestination.Attributes = removeAttribute(rejectedDestination.Attributes, attributeDestinationName)
	rejectedSystem := cloneSpan(valid)
	setStringAttribute(rejectedSystem.Attributes, attributeMessagingSystem, "kafka")
	exportRequest.ResourceSpans[0].ScopeSpans[0].Spans = []*tracepb.Span{
		valid,
		ignored,
		rejectedDestination,
		rejectedSystem,
	}
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, newProtobufHTTPRequest(t, exportRequest))

	exportResponse := decodeTraceResponse(t, response, http.StatusOK)
	partial := exportResponse.GetPartialSuccess()
	if partial == nil || partial.GetRejectedSpans() != 2 {
		t.Fatalf("Partial success = %#v, want two rejected spans", partial)
	}
	wantMessage := "EventAtlas rejected 2 spans: destination_missing=1, messaging_system_unsupported=1"
	if partial.GetErrorMessage() != wantMessage {
		t.Errorf("Partial success message = %q, want %q", partial.GetErrorMessage(), wantMessage)
	}
	if ingestor.calls != 1 || len(ingestor.facts) != 1 {
		t.Errorf("Ingest() calls/facts = %d/%d, want only accepted fact", ingestor.calls, len(ingestor.facts))
	}
}

func TestHTTPHandlerAcknowledgesEmptyIgnoredAndRejectedOnlyBatchesWithoutStoreCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		request     func(*testing.T) *http.Request
		wantPartial int64
	}{
		{
			name: "empty protobuf message",
			request: func(t *testing.T) *http.Request {
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, nil)
				request.Header.Set("Content-Type", ProtobufContentType)
				return request
			},
		},
		{
			name: "ignored span",
			request: func(t *testing.T) *http.Request {
				exportRequest := validTraceRequest(normalizerTestObservedAt)
				spanOf(exportRequest).Attributes = removeAttribute(spanOf(exportRequest).Attributes, attributeMessagingOperation)
				return newProtobufHTTPRequest(t, exportRequest)
			},
		},
		{
			name: "rejected span",
			request: func(t *testing.T) *http.Request {
				exportRequest := validTraceRequest(normalizerTestObservedAt)
				spanOf(exportRequest).Attributes = removeAttribute(spanOf(exportRequest).Attributes, attributeDestinationName)
				return newProtobufHTTPRequest(t, exportRequest)
			},
			wantPartial: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ingestor := &receiverIngestorStub{err: errors.New("store must not be called")}
			handler := newTestHTTPHandler(t, ingestor, receiverTestMaxRequestBytes)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, test.request(t))

			exportResponse := decodeTraceResponse(t, response, http.StatusOK)
			if got := exportResponse.GetPartialSuccess().GetRejectedSpans(); got != test.wantPartial {
				t.Errorf("Rejected spans = %d, want %d", got, test.wantPartial)
			}
			if ingestor.calls != 0 {
				t.Errorf("Ingest() calls = %d, want 0", ingestor.calls)
			}
		})
	}
}

func TestHTTPHandlerAcceptsGzipCompressedRequest(t *testing.T) {
	t.Parallel()

	ingestor := &receiverIngestorStub{}
	handler := newTestHTTPHandler(t, ingestor, receiverTestMaxRequestBytes)
	payload := marshalTraceRequest(t, validTraceRequest(normalizerTestObservedAt))
	request := httptest.NewRequest(http.MethodPost, TraceExportPath, gzipPayload(t, payload))
	request.Header.Set("Content-Type", ProtobufContentType)
	request.Header.Set("Content-Encoding", contentEncodingGzip)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	decodeTraceResponse(t, response, http.StatusOK)
	if ingestor.calls != 1 {
		t.Errorf("Ingest() calls = %d, want 1", ingestor.calls)
	}
}

func TestHTTPHandlerRejectsInvalidTransportRequestsWithProtobufStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		request     func(*testing.T) *http.Request
		wantStatus  int
		wantMessage string
	}{
		{
			name: "missing content type",
			request: func(*testing.T) *http.Request {
				return httptest.NewRequest(http.MethodPost, TraceExportPath, nil)
			},
			wantStatus:  http.StatusUnsupportedMediaType,
			wantMessage: "content type",
		},
		{
			name: "JSON is outside first slice",
			request: func(*testing.T) *http.Request {
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, strings.NewReader("{}"))
				request.Header.Set("Content-Type", "application/json")
				return request
			},
			wantStatus:  http.StatusUnsupportedMediaType,
			wantMessage: "content type",
		},
		{
			name: "unsupported content encoding",
			request: func(*testing.T) *http.Request {
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, nil)
				request.Header.Set("Content-Type", ProtobufContentType)
				request.Header.Set("Content-Encoding", "br")
				return request
			},
			wantStatus:  http.StatusUnsupportedMediaType,
			wantMessage: "content encoding",
		},
		{
			name: "malformed gzip",
			request: func(*testing.T) *http.Request {
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, strings.NewReader("not-gzip"))
				request.Header.Set("Content-Type", ProtobufContentType)
				request.Header.Set("Content-Encoding", contentEncodingGzip)
				return request
			},
			wantStatus:  http.StatusBadRequest,
			wantMessage: "could not be decoded",
		},
		{
			name: "malformed protobuf",
			request: func(*testing.T) *http.Request {
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, bytes.NewReader([]byte{0xff}))
				request.Header.Set("Content-Type", ProtobufContentType)
				return request
			},
			wantStatus:  http.StatusBadRequest,
			wantMessage: "not valid protobuf",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ingestor := &receiverIngestorStub{}
			handler := newTestHTTPHandler(t, ingestor, receiverTestMaxRequestBytes)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, test.request(t))

			status := decodeStatusResponse(t, response, test.wantStatus)
			if !strings.Contains(status.GetMessage(), test.wantMessage) {
				t.Errorf("Status message = %q, want it to contain %q", status.GetMessage(), test.wantMessage)
			}
			if ingestor.calls != 0 {
				t.Errorf("Ingest() calls = %d, want 0", ingestor.calls)
			}
		})
	}
}

func TestHTTPHandlerLimitsCompressedAndDecompressedRequestSize(t *testing.T) {
	t.Parallel()

	const maxBytes = int64(64)
	tests := []struct {
		name    string
		request func(*testing.T) *http.Request
	}{
		{
			name: "wire payload",
			request: func(*testing.T) *http.Request {
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, bytes.NewReader(make([]byte, maxBytes+1)))
				request.Header.Set("Content-Type", ProtobufContentType)
				return request
			},
		},
		{
			name: "streamed wire payload without content length",
			request: func(*testing.T) *http.Request {
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, bytes.NewReader(make([]byte, maxBytes+1)))
				request.ContentLength = -1
				request.Header.Set("Content-Type", ProtobufContentType)
				return request
			},
		},
		{
			name: "expanded gzip payload",
			request: func(t *testing.T) *http.Request {
				compressed := gzipPayload(t, make([]byte, 4096))
				if int64(compressed.Len()) > maxBytes {
					t.Fatalf("test gzip payload = %d bytes, want at most %d", compressed.Len(), maxBytes)
				}
				request := httptest.NewRequest(http.MethodPost, TraceExportPath, compressed)
				request.Header.Set("Content-Type", ProtobufContentType)
				request.Header.Set("Content-Encoding", contentEncodingGzip)
				return request
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ingestor := &receiverIngestorStub{}
			handler := newTestHTTPHandler(t, ingestor, maxBytes)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, test.request(t))

			decodeStatusResponse(t, response, http.StatusRequestEntityTooLarge)
			if ingestor.calls != 0 {
				t.Errorf("Ingest() calls = %d, want 0", ingestor.calls)
			}
		})
	}
}

func TestHTTPHandlerMapsPersistenceFailuresToRetryableStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "store unavailable", err: errors.New("database unavailable"), wantStatus: http.StatusServiceUnavailable},
		{name: "deadline", err: context.DeadlineExceeded, wantStatus: http.StatusGatewayTimeout},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ingestor := &receiverIngestorStub{err: test.err}
			handler := newTestHTTPHandler(t, ingestor, receiverTestMaxRequestBytes)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, newProtobufHTTPRequest(t, validTraceRequest(normalizerTestObservedAt)))

			status := decodeStatusResponse(t, response, test.wantStatus)
			if status.GetMessage() != "temporarily unable to persist observations" {
				t.Errorf("Status message = %q, want bounded persistence message", status.GetMessage())
			}
			if ingestor.calls != 1 {
				t.Errorf("Ingest() calls = %d, want 1", ingestor.calls)
			}
		})
	}
}

func TestHTTPHandlerMapsRequestCancellationToRetryableFailure(t *testing.T) {
	t.Parallel()

	ingestor := &receiverIngestorStub{}
	handler := newTestHTTPHandler(t, ingestor, receiverTestMaxRequestBytes)
	request := newProtobufHTTPRequest(t, validTraceRequest(normalizerTestObservedAt))
	ctx, cancel := context.WithCancel(request.Context())
	cancel()
	request = request.WithContext(ctx)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	status := decodeStatusResponse(t, response, http.StatusServiceUnavailable)
	if status.GetMessage() != "could not normalize trace request" {
		t.Errorf("Status message = %q, want bounded normalization message", status.GetMessage())
	}
	if ingestor.calls != 0 {
		t.Errorf("Ingest() calls = %d, want 0", ingestor.calls)
	}
}

func TestHTTPHandlerUsesMethodAwareTraceRoute(t *testing.T) {
	t.Parallel()

	handler := newTestHTTPHandler(t, &receiverIngestorStub{}, receiverTestMaxRequestBytes)

	wrongMethod := httptest.NewRecorder()
	handler.ServeHTTP(wrongMethod, httptest.NewRequest(http.MethodGet, TraceExportPath, nil))
	if wrongMethod.Code != http.StatusMethodNotAllowed || wrongMethod.Header().Get("Allow") != http.MethodPost {
		t.Errorf("GET %s = status %d Allow %q, want 405 Allow POST", TraceExportPath, wrongMethod.Code, wrongMethod.Header().Get("Allow"))
	}

	wrongPath := httptest.NewRecorder()
	handler.ServeHTTP(wrongPath, httptest.NewRequest(http.MethodPost, "/v1/metrics", nil))
	if wrongPath.Code != http.StatusNotFound {
		t.Errorf("POST /v1/metrics status = %d, want 404", wrongPath.Code)
	}
}

func TestNewHTTPHandlerValidatesDependenciesAndConfig(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	ingestor := &receiverIngestorStub{}
	var typedNil *receiverIngestorStub

	if _, err := NewHTTPHandler(nil, ingestor, HTTPReceiverConfig{MaxRequestBytes: 1}); !errors.Is(err, ErrNormalizerNil) {
		t.Errorf("NewHTTPHandler(nil normalizer) error = %v, want %v", err, ErrNormalizerNil)
	}
	if _, err := NewHTTPHandler(normalizer, nil, HTTPReceiverConfig{MaxRequestBytes: 1}); !errors.Is(err, ErrFactIngestorNil) {
		t.Errorf("NewHTTPHandler(nil ingestor) error = %v, want %v", err, ErrFactIngestorNil)
	}
	if _, err := NewHTTPHandler(normalizer, typedNil, HTTPReceiverConfig{MaxRequestBytes: 1}); !errors.Is(err, ErrFactIngestorNil) {
		t.Errorf("NewHTTPHandler(typed nil ingestor) error = %v, want %v", err, ErrFactIngestorNil)
	}
	if _, err := NewHTTPHandler(normalizer, ingestor, HTTPReceiverConfig{}); !errors.Is(err, ErrMaxRequestBytesInvalid) {
		t.Errorf("NewHTTPHandler(invalid max bytes) error = %v, want %v", err, ErrMaxRequestBytesInvalid)
	}
}

type receiverIngestorStub struct {
	calls int
	facts []observation.Fact
	err   error
}

func (ingestor *receiverIngestorStub) Ingest(_ context.Context, facts []observation.Fact) error {
	ingestor.calls++
	ingestor.facts = append([]observation.Fact(nil), facts...)
	return ingestor.err
}

func newTestHTTPHandler(t *testing.T, ingestor FactIngestor, maxRequestBytes int64) http.Handler {
	t.Helper()
	handler, err := NewHTTPHandler(newTestNormalizer(t), ingestor, HTTPReceiverConfig{MaxRequestBytes: maxRequestBytes})
	if err != nil {
		t.Fatalf("NewHTTPHandler() error = %v", err)
	}
	return handler
}

func newProtobufHTTPRequest(t *testing.T, request *collectortracepb.ExportTraceServiceRequest) *http.Request {
	t.Helper()
	httpRequest := httptest.NewRequest(http.MethodPost, TraceExportPath, bytes.NewReader(marshalTraceRequest(t, request)))
	httpRequest.Header.Set("Content-Type", ProtobufContentType)
	return httpRequest
}

func marshalTraceRequest(t *testing.T, request *collectortracepb.ExportTraceServiceRequest) []byte {
	t.Helper()
	payload, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("proto.Marshal(trace request) error = %v", err)
	}
	return payload
}

func decodeTraceResponse(t *testing.T, response *httptest.ResponseRecorder, wantStatus int) *collectortracepb.ExportTraceServiceResponse {
	t.Helper()
	assertProtobufResponse(t, response, wantStatus)
	message := &collectortracepb.ExportTraceServiceResponse{}
	if err := proto.Unmarshal(response.Body.Bytes(), message); err != nil {
		t.Fatalf("proto.Unmarshal(trace response) error = %v", err)
	}
	return message
}

func decodeStatusResponse(t *testing.T, response *httptest.ResponseRecorder, wantStatus int) *statuspb.Status {
	t.Helper()
	assertProtobufResponse(t, response, wantStatus)
	message := &statuspb.Status{}
	if err := proto.Unmarshal(response.Body.Bytes(), message); err != nil {
		t.Fatalf("proto.Unmarshal(status response) error = %v", err)
	}
	return message
}

func assertProtobufResponse(t *testing.T, response *httptest.ResponseRecorder, wantStatus int) {
	t.Helper()
	if response.Code != wantStatus {
		t.Fatalf("HTTP status = %d, want %d; body = %x", response.Code, wantStatus, response.Body.Bytes())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != ProtobufContentType {
		t.Errorf("Content-Type = %q, want %q", contentType, ProtobufContentType)
	}
}

func gzipPayload(t *testing.T, payload []byte) *bytes.Buffer {
	t.Helper()
	buffer := &bytes.Buffer{}
	writer := gzip.NewWriter(buffer)
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("gzip.Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip.Close() error = %v", err)
	}
	return buffer
}
