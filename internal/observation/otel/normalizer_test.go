package otel

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

var normalizerTestObservedAt = time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)

func TestNormalizerMapsPublishingSpanToProviderNeutralFact(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	request := validTraceRequest(normalizerTestObservedAt)
	request.ResourceSpans[0].Resource.Attributes = append(
		request.ResourceSpans[0].Resource.Attributes,
		stringAttribute("service.instance.id", "checkout-7f9b9d"),
	)
	request.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes = append(
		request.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes,
		stringAttribute("messaging.message.id", "very-high-cardinality-value"),
	)

	result, err := normalizer.Normalize(context.Background(), request)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if result.TotalSpans() != 1 || result.AcceptedSpans() != 1 || result.IgnoredSpans() != 0 || result.RejectedSpans() != 0 {
		t.Fatalf(
			"Normalize() counters = total:%d accepted:%d ignored:%d rejected:%d, want 1/1/0/0",
			result.TotalSpans(),
			result.AcceptedSpans(),
			result.IgnoredSpans(),
			result.RejectedSpans(),
		)
	}

	facts := result.Facts()
	if len(facts) != 1 {
		t.Fatalf("Normalize() facts = %d, want 1", len(facts))
	}
	fact := facts[0]
	if fact.SourceID().String() != "observation:otel:test" || fact.Scope().String() != "account:test" {
		t.Errorf("Fact source/scope = %q/%q, want configured values", fact.SourceID(), fact.Scope())
	}
	if !fact.ObservedAt().Equal(normalizerTestObservedAt.Add(time.Second)) {
		t.Errorf("Fact observed at = %v, want span end %v", fact.ObservedAt(), normalizerTestObservedAt.Add(time.Second))
	}
	if fact.RelationshipKind() != topology.EdgeKindPublishes {
		t.Errorf("Fact relationship = %q, want %q", fact.RelationshipKind(), topology.EdgeKindPublishes)
	}
	if service := fact.Service(); service.Environment() != "development" || service.Namespace() != "commerce" || service.Name() != "checkout" {
		t.Errorf("Fact service = %#v, want development/commerce/checkout", service)
	}
	if destination := fact.Destination(); destination.MessagingSystem() != "nats" || destination.PhysicalName() != "orders.42" || destination.LogicalName() != "orders.*" {
		t.Errorf("Fact destination = %#v, want nats/orders.42/orders.*", destination)
	}

	metadata := fact.Metadata()
	wantMetadata := map[string]string{
		MetadataEnvironmentSource:      EnvironmentSourceResource,
		MetadataInstrumentationName:    "github.com/acme/natsotel",
		MetadataInstrumentationVersion: "1.2.3",
		MetadataResourceSchemaURL:      "https://opentelemetry.io/schemas/1.44.0",
		MetadataScopeSchemaURL:         "https://opentelemetry.io/schemas/1.44.0",
	}
	for key, want := range wantMetadata {
		if metadata[key] != want {
			t.Errorf("Fact metadata[%q] = %q, want %q", key, metadata[key], want)
		}
	}
	if _, retained := metadata["service.instance.id"]; retained {
		t.Error("Fact metadata retained service.instance.id")
	}
	if _, retained := metadata["messaging.message.id"]; retained {
		t.Error("Fact metadata retained messaging.message.id")
	}

	// Results are immutable at the package boundary, just like domain facts.
	facts[0] = observation.Fact{}
	metadata[MetadataEnvironmentSource] = "mutated"
	if result.Facts()[0].Service().Name() != "checkout" {
		t.Error("Facts() exposed the result slice to caller mutation")
	}
	if result.Facts()[0].Metadata()[MetadataEnvironmentSource] != EnvironmentSourceResource {
		t.Error("Fact.Metadata() exposed metadata to caller mutation")
	}
}

func TestNormalizerMapsProcessSpanToConsumesFact(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	request := validTraceRequest(normalizerTestObservedAt)
	setStringAttribute(spanOf(request).Attributes, attributeMessagingOperation, operationTypeProcess)

	result, err := normalizer.Normalize(context.Background(), request)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if result.AcceptedSpans() != 1 || result.IgnoredSpans() != 0 || result.RejectedSpans() != 0 {
		t.Fatalf("Normalize() counters = accepted:%d ignored:%d rejected:%d, want 1/0/0", result.AcceptedSpans(), result.IgnoredSpans(), result.RejectedSpans())
	}
	if got := result.Facts()[0].RelationshipKind(); got != topology.EdgeKindConsumes {
		t.Errorf("Fact relationship = %q, want %q", got, topology.EdgeKindConsumes)
	}
}

func TestNormalizerRejectsInvalidProcessSpan(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	request := validTraceRequest(normalizerTestObservedAt)
	span := spanOf(request)
	setStringAttribute(span.Attributes, attributeMessagingOperation, operationTypeProcess)
	span.Attributes = removeAttribute(span.Attributes, attributeDestinationName)

	result, err := normalizer.Normalize(context.Background(), request)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if result.AcceptedSpans() != 0 || result.RejectedSpans() != 1 || result.RejectionReasons()[RejectionDestinationMissing] != 1 {
		t.Errorf("Normalize() result = accepted:%d rejected:%d reasons:%v, want 0/1 destination_missing", result.AcceptedSpans(), result.RejectedSpans(), result.RejectionReasons())
	}
}

func TestNormalizerUsesConfiguredEnvironmentAndStartTimeFallbacks(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	request := validTraceRequest(normalizerTestObservedAt)
	resource := request.ResourceSpans[0].Resource
	resource.Attributes = removeAttribute(resource.Attributes, attributeEnvironment)
	span := request.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.EndTimeUnixNano = 0

	result, err := normalizer.Normalize(context.Background(), request)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	fact := result.Facts()[0]
	if fact.Service().Environment() != "development" {
		t.Errorf("Fact environment = %q, want configured fallback", fact.Service().Environment())
	}
	if fact.Metadata()[MetadataEnvironmentSource] != EnvironmentSourceConfiguredFallback {
		t.Errorf("Environment source = %q, want configured fallback", fact.Metadata()[MetadataEnvironmentSource])
	}
	if !fact.ObservedAt().Equal(normalizerTestObservedAt) {
		t.Errorf("Fact observed at = %v, want span start %v", fact.ObservedAt(), normalizerTestObservedAt)
	}
}

func TestNormalizerSeparatesAcceptedIgnoredAndRejectedSpans(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	request := validTraceRequest(normalizerTestObservedAt)
	valid := request.ResourceSpans[0].ScopeSpans[0].Spans[0]
	ignored := cloneSpan(valid)
	setStringAttribute(ignored.Attributes, attributeMessagingOperation, "receive")
	unrelated := cloneSpan(valid)
	unrelated.Attributes = removeAttribute(unrelated.Attributes, attributeMessagingOperation)
	rejected := cloneSpan(valid)
	rejected.Attributes = removeAttribute(rejected.Attributes, attributeDestinationName)
	request.ResourceSpans[0].ScopeSpans[0].Spans = []*tracepb.Span{valid, ignored, unrelated, rejected, nil}

	result, err := normalizer.Normalize(context.Background(), request)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if result.TotalSpans() != 5 || result.AcceptedSpans() != 1 || result.IgnoredSpans() != 2 || result.RejectedSpans() != 2 {
		t.Fatalf(
			"Normalize() counters = total:%d accepted:%d ignored:%d rejected:%d, want 5/1/2/2",
			result.TotalSpans(),
			result.AcceptedSpans(),
			result.IgnoredSpans(),
			result.RejectedSpans(),
		)
	}
	reasons := result.RejectionReasons()
	if reasons[RejectionDestinationMissing] != 1 || reasons[RejectionInvalidSpan] != 1 {
		t.Errorf("Rejection reasons = %#v, want one destination_missing and one invalid_span", reasons)
	}

	reasons[RejectionInvalidSpan] = 99
	if result.RejectionReasons()[RejectionInvalidSpan] != 1 {
		t.Error("RejectionReasons() exposed its internal map to caller mutation")
	}
}

func TestNormalizerRejectsMalformedPublishingSpans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*collectortracepb.ExportTraceServiceRequest)
		wantReason RejectionReason
	}{
		{
			name: "operation has wrong type",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				setAttribute(spanOf(request).Attributes, attributeMessagingOperation, boolAttribute(attributeMessagingOperation, true).Value)
			},
			wantReason: RejectionInvalidAttributeType,
		},
		{
			name: "service name missing",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				resourceOf(request).Attributes = removeAttribute(resourceOf(request).Attributes, attributeServiceName)
			},
			wantReason: RejectionServiceNameMissing,
		},
		{
			name: "SDK fallback service name",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				setStringAttribute(resourceOf(request).Attributes, attributeServiceName, "unknown_service:checkout")
			},
			wantReason: RejectionServiceNameUnknown,
		},
		{
			name: "environment blank",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				setStringAttribute(resourceOf(request).Attributes, attributeEnvironment, " ")
			},
			wantReason: RejectionEnvironmentInvalid,
		},
		{
			name: "environment mismatch",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				setStringAttribute(resourceOf(request).Attributes, attributeEnvironment, "production")
			},
			wantReason: RejectionEnvironmentMismatch,
		},
		{
			name: "messaging system missing",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				spanOf(request).Attributes = removeAttribute(spanOf(request).Attributes, attributeMessagingSystem)
			},
			wantReason: RejectionMessagingSystemMissing,
		},
		{
			name: "messaging system unsupported",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				setStringAttribute(spanOf(request).Attributes, attributeMessagingSystem, "kafka")
			},
			wantReason: RejectionMessagingSystemUnsupported,
		},
		{
			name: "destination missing",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				spanOf(request).Attributes = removeAttribute(spanOf(request).Attributes, attributeDestinationName)
			},
			wantReason: RejectionDestinationMissing,
		},
		{
			name: "NATS inbox is temporary",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				setStringAttribute(spanOf(request).Attributes, attributeDestinationName, "_INBOX.reply-42")
			},
			wantReason: RejectionDestinationTemporary,
		},
		{
			name: "duplicate identity attribute",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				spanOf(request).Attributes = append(spanOf(request).Attributes, stringAttribute(attributeDestinationName, "other"))
			},
			wantReason: RejectionDuplicateAttribute,
		},
		{
			name: "identity attribute too long",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				setStringAttribute(spanOf(request).Attributes, attributeDestinationName, strings.Repeat("a", 65))
			},
			wantReason: RejectionAttributeValueTooLong,
		},
		{
			name: "timestamp missing",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				spanOf(request).StartTimeUnixNano = 0
				spanOf(request).EndTimeUnixNano = 0
			},
			wantReason: RejectionTimestampMissing,
		},
		{
			name: "end before start",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				spanOf(request).EndTimeUnixNano = spanOf(request).StartTimeUnixNano - 1
			},
			wantReason: RejectionTimestampInvalid,
		},
		{
			name: "timestamp overflows Unix nanoseconds",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				spanOf(request).StartTimeUnixNano = 0
				spanOf(request).EndTimeUnixNano = math.MaxUint64
			},
			wantReason: RejectionTimestampInvalid,
		},
		{
			name: "timestamp too far in future",
			mutate: func(request *collectortracepb.ExportTraceServiceRequest) {
				spanOf(request).EndTimeUnixNano = uint64(normalizerTestObservedAt.Add(8 * time.Minute).UnixNano())
			},
			wantReason: RejectionTimestampFuture,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			normalizer := newTestNormalizer(t)
			request := validTraceRequest(normalizerTestObservedAt)
			test.mutate(request)

			result, err := normalizer.Normalize(context.Background(), request)
			if err != nil {
				t.Fatalf("Normalize() error = %v", err)
			}
			if result.TotalSpans() != 1 || result.AcceptedSpans() != 0 || result.IgnoredSpans() != 0 || result.RejectedSpans() != 1 {
				t.Fatalf(
					"Normalize() counters = total:%d accepted:%d ignored:%d rejected:%d, want 1/0/0/1",
					result.TotalSpans(),
					result.AcceptedSpans(),
					result.IgnoredSpans(),
					result.RejectedSpans(),
				)
			}
			if got := result.RejectionReasons(); len(got) != 1 || got[test.wantReason] != 1 {
				t.Errorf("Normalize() rejection reasons = %#v, want one %q", got, test.wantReason)
			}
		})
	}
}

func TestNormalizerOmitsOversizedDiagnosticMetadata(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	request := validTraceRequest(normalizerTestObservedAt)
	scopeSpans := request.ResourceSpans[0].ScopeSpans[0]
	scopeSpans.Scope.Name = strings.Repeat("a", 65)
	scopeSpans.SchemaUrl = strings.Repeat("b", 65)

	result, err := normalizer.Normalize(context.Background(), request)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if result.AcceptedSpans() != 1 {
		t.Fatalf("Normalize() accepted spans = %d, want 1", result.AcceptedSpans())
	}
	metadata := result.Facts()[0].Metadata()
	if _, exists := metadata[MetadataInstrumentationName]; exists {
		t.Error("Normalize() retained oversized instrumentation scope name")
	}
	if _, exists := metadata[MetadataScopeSchemaURL]; exists {
		t.Error("Normalize() retained oversized scope schema URL")
	}
}

func TestNormalizerHonorsContextAndRejectsNilInputs(t *testing.T) {
	t.Parallel()

	normalizer := newTestNormalizer(t)
	request := validTraceRequest(normalizerTestObservedAt)
	var nilContext context.Context

	if _, err := normalizer.Normalize(nilContext, request); !errors.Is(err, ErrContextNil) {
		t.Errorf("Normalize(nil context) error = %v, want %v", err, ErrContextNil)
	}
	if _, err := normalizer.Normalize(context.Background(), nil); !errors.Is(err, ErrRequestNil) {
		t.Errorf("Normalize(nil request) error = %v, want %v", err, ErrRequestNil)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := normalizer.Normalize(ctx, request); !errors.Is(err, context.Canceled) || result.TotalSpans() != 0 {
		t.Errorf("Normalize(canceled) = result %#v, error %v; want empty result and context canceled", result, err)
	}

	second := cloneSpan(spanOf(request))
	request.ResourceSpans[0].ScopeSpans[0].Spans = append(request.ResourceSpans[0].ScopeSpans[0].Spans, second)
	progressiveContext := newCancelAfterErrCallsContext(3)
	if result, err := normalizer.Normalize(progressiveContext, request); !errors.Is(err, context.Canceled) || result.TotalSpans() != 0 {
		t.Errorf("Normalize(mid-batch cancellation) = result %#v, error %v; want discarded result and context canceled", result, err)
	}
}

func TestNewNormalizerValidatesConfiguration(t *testing.T) {
	t.Parallel()

	valid := normalizerTestConfig()
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{name: "source ID", mutate: func(config *Config) { config.SourceID = topology.SourceID{} }, wantErr: ErrSourceIDInvalid},
		{name: "scope", mutate: func(config *Config) { config.Scope = topology.DiscoveryScope{} }, wantErr: ErrScopeInvalid},
		{name: "environment", mutate: func(config *Config) { config.Environment = " " }, wantErr: ErrEnvironmentEmpty},
		{name: "no messaging systems", mutate: func(config *Config) { config.SupportedMessagingSystems = nil }, wantErr: ErrMessagingSystemsEmpty},
		{name: "blank messaging system", mutate: func(config *Config) { config.SupportedMessagingSystems = []string{" "} }, wantErr: ErrMessagingSystemEmpty},
		{name: "negative future skew", mutate: func(config *Config) { config.MaxFutureSkew = -time.Second }, wantErr: ErrFutureSkewNegative},
		{name: "attribute length", mutate: func(config *Config) { config.MaxAttributeValueLength = 0 }, wantErr: ErrAttributeValueLengthInvalid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid
			test.mutate(&config)
			if _, err := NewNormalizer(config); !errors.Is(err, test.wantErr) {
				t.Errorf("NewNormalizer() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func newTestNormalizer(t *testing.T) *Normalizer {
	t.Helper()
	normalizer, err := NewNormalizer(normalizerTestConfig())
	if err != nil {
		t.Fatalf("NewNormalizer() error = %v", err)
	}
	normalizer.now = func() time.Time { return normalizerTestObservedAt.Add(2 * time.Minute) }
	return normalizer
}

func normalizerTestConfig() Config {
	sourceID, _ := topology.NewSourceID("observation:otel:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	return Config{
		SourceID:                  sourceID,
		Scope:                     scope,
		Environment:               " development ",
		SupportedMessagingSystems: []string{" NATS "},
		MaxFutureSkew:             5 * time.Minute,
		MaxAttributeValueLength:   64,
	}
}

func validTraceRequest(start time.Time) *collectortracepb.ExportTraceServiceRequest {
	return &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						stringAttribute(attributeServiceName, "checkout"),
						stringAttribute(attributeServiceNamespace, "commerce"),
						stringAttribute(attributeEnvironment, "development"),
					},
				},
				SchemaUrl: "https://opentelemetry.io/schemas/1.44.0",
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "github.com/acme/natsotel",
							Version: "1.2.3",
						},
						SchemaUrl: "https://opentelemetry.io/schemas/1.44.0",
						Spans: []*tracepb.Span{
							{
								Name:              "publish orders.*",
								StartTimeUnixNano: uint64(start.UnixNano()),
								EndTimeUnixNano:   uint64(start.Add(time.Second).UnixNano()),
								Attributes: []*commonpb.KeyValue{
									stringAttribute(attributeMessagingOperation, operationTypeSend),
									stringAttribute(attributeMessagingSystem, "NATS"),
									stringAttribute(attributeDestinationName, "orders.42"),
									stringAttribute(attributeDestinationTemplate, "orders.*"),
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceOf(request *collectortracepb.ExportTraceServiceRequest) *resourcepb.Resource {
	return request.ResourceSpans[0].Resource
}

func spanOf(request *collectortracepb.ExportTraceServiceRequest) *tracepb.Span {
	return request.ResourceSpans[0].ScopeSpans[0].Spans[0]
}

func stringAttribute(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: value},
		},
	}
}

func boolAttribute(key string, value bool) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{BoolValue: value},
		},
	}
}

func setStringAttribute(attributes []*commonpb.KeyValue, key, value string) {
	setAttribute(attributes, key, stringAttribute(key, value).Value)
}

func setAttribute(attributes []*commonpb.KeyValue, key string, value *commonpb.AnyValue) {
	for _, attribute := range attributes {
		if attribute.GetKey() == key {
			attribute.Value = value
			return
		}
	}
}

func removeAttribute(attributes []*commonpb.KeyValue, key string) []*commonpb.KeyValue {
	filtered := make([]*commonpb.KeyValue, 0, len(attributes))
	for _, attribute := range attributes {
		if attribute.GetKey() != key {
			filtered = append(filtered, attribute)
		}
	}
	return filtered
}

func cloneSpan(span *tracepb.Span) *tracepb.Span {
	return proto.Clone(span).(*tracepb.Span)
}

type cancelAfterErrCallsContext struct {
	calls    int
	cancelAt int
	done     chan struct{}
	canceled bool
}

func newCancelAfterErrCallsContext(cancelAt int) *cancelAfterErrCallsContext {
	return &cancelAfterErrCallsContext{cancelAt: cancelAt, done: make(chan struct{})}
}

func (ctx *cancelAfterErrCallsContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAfterErrCallsContext) Done() <-chan struct{}       { return ctx.done }
func (ctx *cancelAfterErrCallsContext) Value(any) any               { return nil }

func (ctx *cancelAfterErrCallsContext) Err() error {
	if ctx.canceled {
		return context.Canceled
	}
	ctx.calls++
	if ctx.calls >= ctx.cancelAt {
		ctx.canceled = true
		close(ctx.done)
		return context.Canceled
	}
	return nil
}
