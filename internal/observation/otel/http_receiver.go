package otel

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"reflect"
	"sort"
	"strings"

	"github.com/lucacox/eventatlas/internal/observation"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	TraceExportPath         = "/v1/traces"
	ProtobufContentType     = "application/x-protobuf"
	contentEncodingGzip     = "gzip"
	contentEncodingIdentity = "identity"
)

var (
	ErrNormalizerNil          = errors.New("otel normalizer cannot be nil")
	ErrFactIngestorNil        = errors.New("otel fact ingestor cannot be nil")
	ErrMaxRequestBytesInvalid = errors.New("otel maximum request bytes must be positive")
)

// FactIngestor is the receiver's narrow application port. ObservationService
// satisfies it without exposing storage or transaction details to HTTP code.
type FactIngestor interface {
	Ingest(ctx context.Context, facts []observation.Fact) error
}

// HTTPReceiverConfig controls transport-level admission independently from
// normalization and observation retention policy.
type HTTPReceiverConfig struct {
	MaxRequestBytes int64
}

// NewHTTPHandler builds the binary-Protobuf OTLP/HTTP trace endpoint. Routing
// is deliberately standard net/http because OTLP is not part of the JSON Huma
// API contract.
func NewHTTPHandler(
	normalizer *Normalizer,
	ingestor FactIngestor,
	config HTTPReceiverConfig,
) (http.Handler, error) {
	if normalizer == nil {
		return nil, ErrNormalizerNil
	}
	if isNilFactIngestor(ingestor) {
		return nil, ErrFactIngestorNil
	}
	if config.MaxRequestBytes <= 0 {
		return nil, ErrMaxRequestBytesInvalid
	}

	receiver := &httpReceiver{
		normalizer:      normalizer,
		ingestor:        ingestor,
		maxRequestBytes: config.MaxRequestBytes,
	}
	mux := http.NewServeMux()
	mux.Handle("POST "+TraceExportPath, receiver)
	return mux, nil
}

type httpReceiver struct {
	normalizer      *Normalizer
	ingestor        FactIngestor
	maxRequestBytes int64
}

func (receiver *httpReceiver) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !isProtobufContentType(request.Header.Get("Content-Type")) {
		writeFailure(writer, http.StatusUnsupportedMediaType, "content type must be application/x-protobuf")
		return
	}

	payload, bodyFailure := receiver.readRequestBody(writer, request)
	if bodyFailure != nil {
		writeFailure(writer, bodyFailure.status, bodyFailure.message)
		return
	}

	exportRequest := &collectortracepb.ExportTraceServiceRequest{}
	if err := proto.Unmarshal(payload, exportRequest); err != nil {
		writeFailure(writer, http.StatusBadRequest, "request body is not valid protobuf")
		return
	}

	result, err := receiver.normalizer.Normalize(request.Context(), exportRequest)
	if err != nil {
		writeFailure(writer, normalizationFailureStatus(err), "could not normalize trace request")
		return
	}

	if result.AcceptedSpans() > 0 {
		if err := receiver.ingestor.Ingest(request.Context(), result.Facts()); err != nil {
			writeFailure(writer, ingestionFailureStatus(err), "temporarily unable to persist observations")
			return
		}
	}

	response := &collectortracepb.ExportTraceServiceResponse{}
	if result.RejectedSpans() > 0 {
		response.PartialSuccess = &collectortracepb.ExportTracePartialSuccess{
			RejectedSpans: result.RejectedSpans(),
			ErrorMessage:  partialSuccessMessage(result),
		}
	}
	writeProto(writer, http.StatusOK, response)
}

type requestFailure struct {
	status  int
	message string
}

func (receiver *httpReceiver) readRequestBody(
	writer http.ResponseWriter,
	request *http.Request,
) ([]byte, *requestFailure) {
	if request.ContentLength > receiver.maxRequestBytes {
		return nil, &requestFailure{http.StatusRequestEntityTooLarge, "request body exceeds configured limit"}
	}

	limitedBody := http.MaxBytesReader(writer, request.Body, receiver.maxRequestBytes)
	var reader io.Reader = limitedBody
	var gzipReader *gzip.Reader

	switch strings.ToLower(strings.TrimSpace(request.Header.Get("Content-Encoding"))) {
	case "", contentEncodingIdentity:
	case contentEncodingGzip:
		var err error
		gzipReader, err = gzip.NewReader(limitedBody)
		if err != nil {
			return nil, requestBodyReadFailure(err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	default:
		return nil, &requestFailure{http.StatusUnsupportedMediaType, "unsupported content encoding"}
	}

	readLimit := receiver.maxRequestBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	payload, err := io.ReadAll(io.LimitReader(reader, readLimit))
	if err != nil {
		return nil, requestBodyReadFailure(err)
	}
	if int64(len(payload)) > receiver.maxRequestBytes {
		return nil, &requestFailure{http.StatusRequestEntityTooLarge, "request body exceeds configured limit"}
	}
	return payload, nil
}

func requestBodyReadFailure(err error) *requestFailure {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		return &requestFailure{http.StatusRequestEntityTooLarge, "request body exceeds configured limit"}
	}
	return &requestFailure{http.StatusBadRequest, "request body could not be decoded"}
}

func isProtobufContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, ProtobufContentType)
}

func normalizationFailureStatus(err error) int {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	if errors.Is(err, context.Canceled) {
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

func ingestionFailureStatus(err error) int {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	return http.StatusServiceUnavailable
}

func partialSuccessMessage(result Result) string {
	reasons := result.RejectionReasons()
	keys := make([]string, 0, len(reasons))
	for reason := range reasons {
		keys = append(keys, reason.String())
	}
	sort.Strings(keys)

	details := make([]string, 0, len(keys))
	for _, key := range keys {
		details = append(details, fmt.Sprintf("%s=%d", key, reasons[RejectionReason(key)]))
	}
	spanLabel := "spans"
	if result.RejectedSpans() == 1 {
		spanLabel = "span"
	}
	return fmt.Sprintf("EventAtlas rejected %d %s: %s", result.RejectedSpans(), spanLabel, strings.Join(details, ", "))
}

func writeFailure(writer http.ResponseWriter, status int, message string) {
	writeProto(writer, status, &statuspb.Status{Message: message})
}

func writeProto(writer http.ResponseWriter, status int, message proto.Message) {
	payload, err := proto.Marshal(message)
	if err != nil {
		status = http.StatusInternalServerError
		payload, _ = proto.Marshal(&statuspb.Status{Message: "could not encode OTLP response"})
	}
	writer.Header().Set("Content-Type", ProtobufContentType)
	writer.WriteHeader(status)
	_, _ = writer.Write(payload)
}

func isNilFactIngestor(value FactIngestor) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
